package org.arpha.model;

public class BrokerPollResponse {
    private int partition;
    private long offset;
    private String message;

    private String redirectToAddress;
    private Integer redirectToBrokerId;

    public BrokerPollResponse() {}

    public BrokerPollResponse(int partition, long offset, String message) {
        this(partition, offset, message, null, null);
    }

    public BrokerPollResponse(int partition, long offset, String message, String redirectToAddress, Integer redirectToBrokerId) {
        this.partition = partition;
        this.offset = offset;
        this.message = message;
        this.redirectToAddress = redirectToAddress;
        this.redirectToBrokerId = redirectToBrokerId;
    }

    public String getRedirectToAddress() {
        return redirectToAddress;
    }

    public void setRedirectToAddress(String redirectToAddress) {
        this.redirectToAddress = redirectToAddress;
    }

    public Integer getRedirectToBrokerId() {
        return redirectToBrokerId;
    }

    public void setRedirectToBrokerId(Integer redirectToBrokerId) {
        this.redirectToBrokerId = redirectToBrokerId;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public String getMessage() {
        return message;
    }

    public boolean hasPayload() {
        return message != null;
    }
}