package org.arpha;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import org.arpha.common.ConsumerMessageType;
import org.arpha.common.MessageType;
import org.arpha.model.BrokerPollResponse;
import org.arpha.model.ConsumerMessage;
import org.arpha.model.Message;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class GenericConsumerClient<T> {

    private final ConsumerConfiguration<T> config;
    private final ObjectMapper mapper = new ObjectMapper();
    private volatile Channel channel;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final EventLoopGroup group = new NioEventLoopGroup();
    private final AtomicBoolean isConnectingOrReconnecting = new AtomicBoolean(false);
    private volatile String currentHost;
    private volatile int currentPort;
    private final AtomicBoolean tasksInitialized = new AtomicBoolean(false);


    public GenericConsumerClient(ConsumerConfiguration<T> config) {
        this.config = config;
        this.currentHost = config.getInitialHost();
        this.currentPort = config.getInitialPort();
    }

    public void start() {
        System.out.println("Consumer " + config.consumerId() + " starting for topic " + config.topic() + ", group " + config.group());
        connect(this.currentHost, this.currentPort);
    }

    private void connect(String host, int port) {
        if (!isConnectingOrReconnecting.compareAndSet(false, true)) {
            System.out.println("Consumer " + config.consumerId() + ": Connection/Reconnection attempt already in progress to " + host + ":" + port + ". Skipping this attempt.");
            return;
        }
        System.out.println("Consumer " + config.consumerId() + ": Attempting to connect to " + host + ":" + port);

        if (this.channel != null && this.channel.isOpen()) {
            System.out.println("Consumer " + config.consumerId() + ": Closing existing channel " + this.channel.id() + " before new connection attempt.");
            this.channel.close().awaitUninterruptibly(1, TimeUnit.SECONDS);
        }

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new LineBasedFrameDecoder(8192));
                        pipeline.addLast(new StringDecoder(CharsetUtil.UTF_8));
                        pipeline.addLast(new StringEncoder(CharsetUtil.UTF_8));
                        pipeline.addLast(new ConsumerHandler());
                    }
                });

        bootstrap.connect(host, port).addListener((ChannelFutureListener) future -> {
            isConnectingOrReconnecting.set(false); // Release the "lock"

            if (future.isSuccess()) {
                channel = future.channel();
                this.currentHost = host;
                this.currentPort = port;
                config.updateBrokerAddress(host, port);
                System.out.println("Consumer " + config.consumerId() + ": Connected to broker at " + host + ":" + port + " on channel " + channel.id());

                if (tasksInitialized.compareAndSet(false, true)) {
                    initializeScheduledTasks();
                }
            } else {
                System.err.println("Consumer " + config.consumerId() + ": Connection to " + host + ":" + port + " failed: " + (future.cause() != null ? future.cause().getMessage() : "Unknown cause"));
                if (!scheduler.isShutdown()) {
                    scheduleReconnect(config.getInitialHost(), config.getInitialPort(), 5); // Fallback
                }
            }
        });
    }

    private void handleRedirect(String redirectToAddress) {
        System.out.println("Consumer " + config.consumerId() + ": Received redirect instruction to: " + redirectToAddress);
        if (redirectToAddress == null || redirectToAddress.isEmpty()) {
            System.err.println("Consumer " + config.consumerId() + ": Invalid redirect address received (null or empty).");
            return;
        }

        String[] parts = redirectToAddress.split(":");
        if (parts.length != 2) {
            System.err.println("Consumer " + config.consumerId() + ": Invalid redirect address format: " + redirectToAddress);
            return;
        }
        String newHost = parts[0];
        int newPort;
        try {
            newPort = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            System.err.println("Consumer " + config.consumerId() + ": Invalid port in redirect address: " + redirectToAddress);
            return;
        }

        if (this.currentHost.equals(newHost) && this.currentPort == newPort && isChannelReady()) {
            System.out.println("Consumer " + config.consumerId() + ": Already connected to redirect address " + redirectToAddress + ". No action needed.");
            return;
        }

        System.out.println("Consumer " + config.consumerId() + ": Redirecting. Will connect to " + newHost + ":" + newPort);
        if (!scheduler.isShutdown()) {
            scheduler.schedule(() -> connect(newHost, newPort), 0, TimeUnit.MILLISECONDS);
        }
    }


    private void scheduleReconnect(String host, int port, int delaySeconds) {
        if (scheduler.isShutdown() || scheduler.isTerminated()) {
            // System.err.println("Consumer " + config.consumerId() + ": Scheduler is not running. Cannot schedule reconnect.");
            return;
        }
        System.out.println("Consumer " + config.consumerId() + ": Scheduling reconnect to " + host + ":" + port + " in " + delaySeconds + " seconds...");
        scheduler.schedule(() -> connect(host, port), delaySeconds, TimeUnit.SECONDS);
    }


    private void sendPoll() {
        if (!isChannelReady()) return;
        try {
            ConsumerMessage msg = config.createMessage(ConsumerMessageType.POLL);
            msg.setOffset(config.getPollOffset());
            msg.setPartition(config.getCurrentPartition());
            channel.writeAndFlush(new Message(MessageType.CONSUMER, config.topic(), mapper.writeValueAsString(msg)).serialize() + "\n");
        } catch (Exception e) {
            System.err.println("Consumer " + config.consumerId() + ": Failed to send poll: " + e.getMessage());
        }
    }

    private void sendCommit() {
        if (!isChannelReady()) return;
        try {
            ConsumerMessage msg = config.createMessage(ConsumerMessageType.COMMIT);
            msg.setOffset(config.getCommitOffset());
            msg.setPartition(config.getCurrentPartition());
//            System.out.println("Consumer " + config.consumerId() + ": Sending COMMIT for topic " + config.topic() +
//                    ", partition " + msg.getPartition() + ", offset " + msg.getOffset());
            channel.writeAndFlush(new Message(MessageType.CONSUMER, config.topic(), mapper.writeValueAsString(msg)).serialize() + "\n");
        } catch (Exception e) {
            System.err.println("Consumer " + config.consumerId() + ": Failed to send commit: " + e.getMessage());
        }
    }

    private void sendHeartbeat() {
        if (!isChannelReady()) return;
        try {
            ConsumerMessage msg = config.createMessage(ConsumerMessageType.HEARTBEAT);
            channel.writeAndFlush(new Message(MessageType.CONSUMER, config.topic(), mapper.writeValueAsString(msg)).serialize() + "\n");
        } catch (Exception e) {
            System.err.println("Consumer " + config.consumerId() + ": Failed to send heartbeat: " + e.getMessage());
        }
    }

    private boolean isChannelReady() {
        return channel != null && channel.isActive();
    }

    public void stop() {
        System.out.println("Consumer " + config.consumerId() + ": Stopping...");
        if (!scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException ie) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (channel != null && channel.isOpen()) {
            channel.close().awaitUninterruptibly(2, TimeUnit.SECONDS);
        }
        if (!group.isShuttingDown() && !group.isShutdown()) {
            group.shutdownGracefully().awaitUninterruptibly(5, TimeUnit.SECONDS);
        }
        System.out.println("Consumer " + config.consumerId() + ": Stopped.");
    }

    private void initializeScheduledTasks() {
        if (scheduler.isShutdown()) {
            System.err.println("Consumer " + config.consumerId() + ": Cannot initialize tasks, scheduler is shutdown.");
            return;
        }
        System.out.println("Consumer " + config.consumerId() + ": Initializing scheduled tasks (Heartbeat @5s, Poll @2s).");
        scheduler.scheduleAtFixedRate(this::sendHeartbeat, 0, 5, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::sendPoll, 1, 100000, TimeUnit.NANOSECONDS);
    }

    @ChannelHandler.Sharable
    private class ConsumerHandler extends SimpleChannelInboundHandler<String> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String rawJsonMessage) throws Exception {
            if (rawJsonMessage == null || rawJsonMessage.trim().isEmpty() || rawJsonMessage.trim().equals("{}")) {
                return;
            }

            BrokerPollResponse brokerResponse;
            try {
                brokerResponse = mapper.readValue(rawJsonMessage, BrokerPollResponse.class);
            } catch (Exception e) {
                System.err.println("Consumer " + config.consumerId() + ": Failed to parse BrokerPollResponse JSON: '" + rawJsonMessage + "'. Error: " + e.getMessage());
                throw e;
            }

            if (brokerResponse.getRedirectToAddress() != null && !brokerResponse.getRedirectToAddress().isEmpty()) {
                ctx.executor().execute(() -> handleRedirect(brokerResponse.getRedirectToAddress()));
                return;
            }

            if (brokerResponse.getPartition() == -1 && brokerResponse.getMessage() == null) {
                return;
            }

            if (brokerResponse.getPartition() != -1) {
                if (config.getCurrentPartition() != brokerResponse.getPartition()) {
                    System.out.println("Consumer " + config.consumerId() + ": Partition assignment changed from " +
                            config.getCurrentPartition() + " to " + brokerResponse.getPartition() +
                            ". Resetting poll offset for new partition to 0.");
                    config.setCurrentPartitionAndResetOffset(brokerResponse.getPartition(), 0L);
                } else {
                    config.setCurrentPartition(brokerResponse.getPartition());
                }
            }

            if (brokerResponse.hasPayload()) {
                String messagePayload = brokerResponse.getMessage();
//                System.out.println("Consumer " + config.consumerId() + ": Received message for partition " + brokerResponse.getPartition() +
//                        " at offset " + brokerResponse.getOffset() + ". Payload: " + messagePayload.substring(0, Math.min(messagePayload.length(), 50)) + "...");
                Object parsed;

                if (config.clazz().equals(String.class)) {
                    parsed = messagePayload;
                } else {
                    parsed = mapper.readValue(messagePayload, config.clazz());
                }

                config.handler().accept((T) parsed);
                config.setNextPollOffset(brokerResponse.getOffset());
                sendCommit();
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            System.out.println("Consumer " + config.consumerId() + ": Channel " + ctx.channel().id() + " became inactive with " + ctx.channel().remoteAddress());

            if (!isConnectingOrReconnecting.get()) {
                if (!scheduler.isShutdown()) {
                    System.out.println("Consumer " + config.consumerId() + ": Scheduling reconnect via channelInactive as no connection attempt is currently in progress.");
                    scheduler.schedule(() -> connect(currentHost, currentPort), 1, TimeUnit.SECONDS);
                } else {
                    System.out.println("Consumer " + config.consumerId() + ": Scheduler is shutdown. Cannot schedule reconnect via channelInactive.");
                }
            } else {
                System.out.println("Consumer " + config.consumerId() + ": Channel " + ctx.channel().id() + " inactive, but a connection/reconnection attempt is already in progress. Skipping scheduled reconnect from channelInactive.");
            }
            // *** MODIFICATION END ***
            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.err.println("Consumer " + config.consumerId() + ": Exception in consumer handler for channel " + ctx.channel().id() + " (" + ctx.channel().remoteAddress() + "): " + cause.getMessage());
            cause.printStackTrace();
            ctx.close();
        }
    }

    public static class ConsumerConfiguration<T> {
        private String initialHost;
        private int initialPort;
        private String currentBrokerHost;
        private int currentBrokerPort;
        private String topic;
        private String group;
        private Class<T> clazz;
        private Consumer<T> handler;
        private final String consumerId = UUID.randomUUID().toString();
        private long currentOffsetForPartition = 0;
        private int currentPartition = 0;

        public ConsumerConfiguration(String host, int port, String topic, String group, Class<T> clazz, Consumer<T> handler, long initialOffset) {
            this.initialHost = host;
            this.initialPort = port;
            this.currentBrokerHost = host;
            this.currentBrokerPort = port;
            this.topic = topic;
            this.group = group;
            this.clazz = clazz;
            this.handler = handler;
            this.currentOffsetForPartition = initialOffset;
        }
        public void updateBrokerAddress(String newHost, int newPort) {
            this.currentBrokerHost = newHost;
            this.currentBrokerPort = newPort;
        }
        public String getInitialHost() { return initialHost; }
        public int getInitialPort() { return initialPort; }
        public ConsumerMessage createMessage(ConsumerMessageType type) {
            ConsumerMessage msg = new ConsumerMessage();
            msg.setAction(type);
            msg.setTopic(topic);
            msg.setConsumerGroup(group);
            msg.setConsumerId(consumerId);
            return msg;
        }
        public String topic() { return topic; }
        public String group() { return group; }
        public Class<T> clazz() { return clazz; }
        public Consumer<T> handler() { return handler; }
        public String consumerId() { return consumerId; }
        public synchronized long getPollOffset() { return currentOffsetForPartition; }
        public synchronized void setNextPollOffset(long processedMessageOffset) { this.currentOffsetForPartition = processedMessageOffset + 1; }
        public synchronized int getCurrentPartition() { return currentPartition; }
        public synchronized void setCurrentPartition(int partition) {
            if (this.currentPartition != partition) {
                this.currentPartition = partition;
            }
        }
        public synchronized void setCurrentPartitionAndResetOffset(int partition, long offset) {
            this.currentPartition = partition;
            this.currentOffsetForPartition = offset;
        }
        public synchronized long getCommitOffset() { return currentOffsetForPartition; }
    }
}
