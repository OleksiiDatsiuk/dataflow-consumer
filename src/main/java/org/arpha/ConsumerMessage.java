package org.arpha;

public class ConsumerMessage {

    private ConsumerMessageType action;
    private String topic;
    private int partition;
    private int maxMessages;
    private long offset;
    private String consumerGroup;
    private String consumerId;

    public ConsumerMessage(ConsumerMessageType action, String topic, int partition, int maxMessages, long offset, String consumerGroup, String consumerId) {
        this.action = action;
        this.topic = topic;
        this.partition = partition;
        this.maxMessages = maxMessages;
        this.offset = offset;
        this.consumerGroup = consumerGroup;
        this.consumerId = consumerId;
    }

    public ConsumerMessage() {
    }

    public ConsumerMessageType getAction() {
        return action;
    }

    public void setAction(ConsumerMessageType action) {
        this.action = action;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public int getMaxMessages() {
        return maxMessages;
    }

    public void setMaxMessages(int maxMessages) {
        this.maxMessages = maxMessages;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }
}
