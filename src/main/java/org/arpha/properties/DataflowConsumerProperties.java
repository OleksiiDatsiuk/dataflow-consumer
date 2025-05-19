package org.arpha.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "dataflow")
public class DataflowConsumerProperties {

    private ClusterProperties broker;
    private ConsumerProperties consumer;

    public static class ConsumerProperties {

        private String defaultConsumerGroup;
        private int maxPollMessages = 10;

        public String getDefaultConsumerGroup() {
            return defaultConsumerGroup;
        }

        public void setDefaultConsumerGroup(String defaultConsumerGroup) {
            this.defaultConsumerGroup = defaultConsumerGroup;
        }

        public int getMaxPollMessages() {
            return maxPollMessages;
        }

        public void setMaxPollMessages(int maxPollMessages) {
            this.maxPollMessages = maxPollMessages;
        }
    }

    public static class ClusterProperties {
        private String host;
        private int port;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }
    }

    public ClusterProperties getBroker() {
        return broker;
    }

    public void setBroker(ClusterProperties broker) {
        this.broker = broker;
    }

    public ConsumerProperties getConsumer() {
        return consumer;
    }

    public void setConsumer(ConsumerProperties consumer) {
        this.consumer = consumer;
    }

    public String getClusterHost() {
        return broker.getHost();
    }

    public void setClusterHost(String clusterHost) {
        this.broker.setHost(clusterHost);
    }

    public int getClusterPort() {
        return broker.getPort();
    }

    public void setClusterPort(int clusterPort) {
        this.broker.setPort(clusterPort);
    }

    public String getDefaultConsumerGroup() {
        return consumer.defaultConsumerGroup;
    }

    public void setDefaultConsumerGroup(String defaultConsumerGroup) {
        consumer.setDefaultConsumerGroup(defaultConsumerGroup);
    }

    public int getMaxPollMessages() {
        return consumer.getMaxPollMessages();
    }

    public void setMaxPollMessages(int maxPollMessages) {
        consumer.setMaxPollMessages(maxPollMessages);
    }
}
