package org.apache.druid.indexing.pubsub;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.protobuf.ByteString;

import java.util.Map;

public class PubSubReceivedMessage {
    private final ByteString data;
    private final Map<String, String> attributes;
    private final AckReplyConsumer consumer;

    public PubSubReceivedMessage(ByteString data, Map<String, String> attributes, AckReplyConsumer consumer) {
        this.data = data;
        this.attributes = attributes;
        this.consumer = consumer;
    }

    public ByteString getData() {
        return data;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void ack() {
        consumer.ack();
    }
}
