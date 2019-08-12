package io.grpc.internal;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.grpc.*;
import io.perfmark.PerfMark;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static io.grpc.internal.GrpcUtil.MESSAGE_ENCODING_KEY;

public class MyServerImpl extends Server {

    private final Object lock = new Object();
    private static final ServerStreamListener NOOP_LISTENER = new MyNoopListener();

    /** Service encapsulating something similar to an accept() socket. */
    private final List<? extends InternalServer> transportServers;

    /** {@code transportServer} and services encapsulating something similar to a TCP connection. */
    @GuardedBy("lock") private final Set<ServerTransport> transports = new HashSet<>();

    private final DecompressorRegistry decompressorRegistry;
    private final CompressorRegistry compressorRegistry;
    private Executor executor;
    private final ObjectPool<? extends Executor> executorPool;
    private final HandlerRegistry registry;
    private final HandlerRegistry fallbackRegistry;// ?? 干嘛的？


    @GuardedBy("lock") private boolean started;
    @GuardedBy("lock") private boolean shutdown;
    /** non-{@code null} if immediate shutdown has been requested. */
    @GuardedBy("lock") private Status shutdownNowStatus;
    /** {@code true} if ServerListenerImpl.serverShutdown() was called. */
    @GuardedBy("lock") private boolean serverShutdownCallbackInvoked;
    @GuardedBy("lock") private boolean terminated;


    @GuardedBy("lock") private boolean transportServersTerminated;
    private final long handshakeTimeoutMillis = 3000L;


    @GuardedBy("lock") private int activeTransportServers;

    MyServerImpl(List<? extends InternalServer> transportServers,
                 AbstractServerImplBuilder<?> builder){

        this.executorPool = Preconditions.checkNotNull(builder.executorPool, "executorPool");
        this.registry = Preconditions.checkNotNull(builder.registryBuilder.build(), "registryBuilder");
        this.fallbackRegistry =
                Preconditions.checkNotNull(builder.fallbackRegistry, "fallbackRegistry");
        this.transportServers = transportServers;
        this.decompressorRegistry = builder.decompressorRegistry;
        this.compressorRegistry = builder.compressorRegistry;
    }


    @Override
    public Server start() throws IOException {
        ServerListener serverListener = new MyServerListenerImpl();
        for(InternalServer internalServer : transportServers){
            internalServer.start(serverListener);
        }
        executor = Preconditions.checkNotNull(executorPool.getObject(),"executor");
        started = true;
        return null;
    }

    @Override
    public Server shutdown() {
        return null;
    }

    @Override
    public Server shutdownNow() {
        return null;
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public void awaitTermination() throws InterruptedException {

    }


    private final class MyServerListenerImpl implements ServerListener {
        @Override
        public ServerTransportListener transportCreated(ServerTransport transport) {
            synchronized (lock) {
                transports.add(transport);
            }
            ServerTransportListener listener = new MyServerTransportlistenerImpl(transport);
            return listener;
        }

        @Override
        public void serverShutdown() {
            ArrayList<ServerTransport> copiedTransports;
            Status shutdownNowStatusCopy;

            // 如果只是连接关闭 减去计数 否则 关闭所有连接
            synchronized (lock){
                activeTransportServers -- ;
                if(activeTransportServers != 0){
                    return;
                }

                // transports collection can be modified during shutdown(), even if we hold the lock, due
                // to reentrancy.
                copiedTransports = new ArrayList<>(transports);
                shutdownNowStatusCopy = shutdownNowStatus;
                serverShutdownCallbackInvoked = true;
            }

            for (ServerTransport transport : copiedTransports) {
                if (shutdownNowStatusCopy == null) {
                    transport.shutdown();
                } else {
                    transport.shutdownNow(shutdownNowStatusCopy);
                }
            }
            synchronized (lock) {
                transportServersTerminated = true;
                checkForTermination();
            }
        }


    }

    /** Notify of complete shutdown if necessary. */
    private void checkForTermination() {
        synchronized (lock) {
            if (shutdown && transports.isEmpty() && transportServersTerminated) {
                if (terminated) {
                    throw new AssertionError("Server already terminated");
                }
                terminated = true;
                if (executor != null) {
                    executor = executorPool.returnObject(executor);
                }
                lock.notifyAll();
            }
        }
    }

    private final class MyServerTransportlistenerImpl implements ServerTransportListener {

        private final ServerTransport transport;

        MyServerTransportlistenerImpl(final ServerTransport transport){
            this.transport = transport;
            transport.getScheduledExecutorService().schedule(new Runnable() {
                @Override
                public void run() {
                    transport.shutdownNow(Status.CANCELLED.withDescription("Handshake timeout exceeded"));
                }
            }, handshakeTimeoutMillis, TimeUnit.MILLISECONDS);
        }


        @Override
        public void streamCreated(final ServerStream stream, final String methodName, final Metadata headers) {
            // 检查header中的编码信息，如果指定了编码 且找不到的对应的解码信息 返回未实现状态
            if(headers.containsKey(MESSAGE_ENCODING_KEY)){
                String encoding = headers.get(MESSAGE_ENCODING_KEY);
                Decompressor decompressor = decompressorRegistry.lookupDecompressor(encoding);
                if (decompressor == null) {
                    stream.close(
                            Status.UNIMPLEMENTED.withDescription(
                                    String.format("Can't find decompressor for %s", encoding)),
                            new Metadata());
                    return;
                }
                stream.setDecompressor(decompressor);
            }

            final StatsTraceContext statsTraceCtx = Preconditions.checkNotNull(
                    stream.statsTraceContext(), "statsTraceCtx not present from stream");
            // 上线文信息   超时处理？todo
            final Context.CancellableContext context = null;

            // 执行器
            final Executor wrappedExecutor = new SerializingExecutor(executor);

            final MyJumpToApplicationThreadServerStreamListener jumpListener
                    = new MyJumpToApplicationThreadServerStreamListener(
                    wrappedExecutor, executor, stream, context);

            stream.setListener(jumpListener);

            // 放到有序queue中 有一个一个执行
            wrappedExecutor.execute(new ContextRunnable(context) {
                @Override
                public void runInContext() {
                    ServerStreamListener listener = NOOP_LISTENER;
                    try {
                        ServerMethodDefinition<?, ?> method = registry.lookupMethod(methodName);
                        if (method == null) {
                            method = fallbackRegistry.lookupMethod(methodName, stream.getAuthority());
                        }
                        if (method == null) {
                            Status status = Status.UNIMPLEMENTED.withDescription(
                                    "Method not found: " + methodName);
                            stream.close(status, new Metadata());
                            context.cancel(null);
                            return;
                        }

                        statsTraceCtx.serverCallStarted(new ServerCallInfoImpl<>(
                                method.getMethodDescriptor(), // notify with original method descriptor
                                stream.getAttributes(),
                                stream.getAuthority()));

                        ServerCallHandler handler = method.getServerCallHandler();
                        // todo interceptedDef

//                        ServerCallImpl call = new ServerCallImpl<>(
//                                stream,
//                                method.getMethodDescriptor(),
//                                headers,
//                                context,
//                                decompressorRegistry,
//                                compressorRegistry,
//                                serverCallTracer,
//                                tag);
//
//                        ServerCall.Listener<WReqT> serverCallListener = handler.startCall(call, headers);
//                        listener = call.newServerStreamListener(serverCallListener);
                    }catch (Exception e){
                        stream.close(Status.fromThrowable(e), new Metadata());
                        context.cancel(null);
                        throw e;
                    }catch (Error e){
                        stream.close(Status.fromThrowable(e), new Metadata());
                        context.cancel(null);
                        throw e;
                    }finally {
                        jumpListener.setListener(listener);
                    }
                }

            });

        }

        @Override
        public Attributes transportReady(Attributes attributes) {
            return null;
        }

        @Override
        public void transportTerminated() {

        }
    }


    @VisibleForTesting
    static final class MyJumpToApplicationThreadServerStreamListener implements ServerStreamListener {
        private final Executor callExecutor;
        private final Executor cancelExecutor;
        private final Context.CancellableContext context;
        private final ServerStream stream;
        private ServerStreamListener listener;

        public MyJumpToApplicationThreadServerStreamListener(Executor executor,
                                                             Executor cancelExecutor, ServerStream stream, Context.CancellableContext context) {
            this.callExecutor = executor;
            this.cancelExecutor = cancelExecutor;
            this.stream = stream;
            this.context = context;
        }

        /**
         * This call MUST be serialized on callExecutor to avoid races.
         */
        private ServerStreamListener getListener() {
            if (listener == null) {
                throw new IllegalStateException("listener unset");
            }
            return listener;
        }

        @VisibleForTesting
        void setListener(ServerStreamListener listener) {
            Preconditions.checkNotNull(listener, "listener must not be null");
            Preconditions.checkState(this.listener == null, "Listener already set");
            this.listener = listener;
        }

        @Override
        public void halfClosed() {

        }

        @Override
        public void closed(Status status) {

        }

        @Override
        public void messagesAvailable(final MessageProducer producer) {
            callExecutor.execute(new ContextRunnable(context) {
                @Override
                public void runInContext() {
                    try {
                        getListener().messagesAvailable(producer);
                    } catch (RuntimeException e) {
                        internalClose();
                        throw e;
                    } catch (Error e) {
                        internalClose();
                        throw e;
                    } finally {
                        // cdf
                        // abc
                        // bcd
                        //PerfMark.stopTask("ServerCallListener(app).messagesAvailable", tag);
                    }
                }
            });
        }

        /**
         * Like {@link ServerCall#close(Status, Metadata)}, but thread-safe for internal use.
         */
        private void internalClose() {
            // TODO(ejona86): this is not thread-safe :)
            stream.close(Status.UNKNOWN, new Metadata());
        }

        @Override
        public void onReady() {

        }
    }


    private static final class MyNoopListener implements ServerStreamListener {
        @Override
        public void halfClosed() {

        }

        @Override
        public void closed(Status status) {

        }

        @Override
        public void messagesAvailable(MessageProducer producer) {

        }

        @Override
        public void onReady() {

        }
    }
}
