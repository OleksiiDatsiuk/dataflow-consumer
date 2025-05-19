package org.arpha.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "dataflow.consumer")
public class DataflowConsumerProperties {

    /**
     * The host of the dataflow cluster.
     */
    private String clusterHost;

    /**
     * The port of the dataflow cluster.
     */
    private int clusterPort;

    /**
     * The default consumer group to use if not specified in @DataflowConsumer.
     */
    private String defaultConsumerGroup;

    /**
     * Maximum number of messages to poll at once.
     * (This property was in your ConsumerMessage.java, including it here for consistency
     * although your GenericConsumerClient doesn't seem to use it directly for poll construction.
     * You might want to integrate it into GenericConsumerClient.ConsumerConfiguration if needed)
     */
    private int maxPollMessages = 10; // Example default

    public String getClusterHost() {
        return clusterHost;
    }

    public void setClusterHost(String clusterHost) {
        this.clusterHost = clusterHost;
    }

    public int getClusterPort() {
        return clusterPort;
    }

    public void setClusterPort(int clusterPort) {
        this.clusterPort = clusterPort;
    }

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
