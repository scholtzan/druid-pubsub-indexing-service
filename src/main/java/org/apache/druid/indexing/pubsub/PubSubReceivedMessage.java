package org.apache.druid.indexing.pubsub;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class PubSubReceivedMessage {
    @JsonProperty("data") private final String data;
    @JsonProperty("attributes") private final Map<String, String> attributes;
    @JsonProperty("messageId") private final String messageId;
    @JsonProperty("publishTime") private final String publishTime;
    private String ackId;

    public PubSubReceivedMessage(String data, Map<String, String> attributes, String messageId, String publishTime) {
        this.data = data;
        this.attributes = attributes;
        this.messageId = messageId;
        this.publishTime = publishTime;
        this.ackId = null;
    }

    public String getData() {
        return data;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAckId(String ackId) {
        this.ackId = ackId;
    }

    public String getAckId() {
        return this.ackId;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getPublishTime() {
        return publishTime;
    }
}
