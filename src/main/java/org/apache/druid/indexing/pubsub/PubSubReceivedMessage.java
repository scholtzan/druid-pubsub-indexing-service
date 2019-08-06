package org.apache.druid.indexing.pubsub;

import com.google.protobuf.ByteString;

import java.util.Map;

public class PubSubReceivedMessage {
    private final ByteString data;
    private final Map<String, String> attributes;

    public PubSubReceivedMessage(ByteString data, Map<String, String> attributes) {
        this.data = data;
        this.attributes = attributes;
    }

    public ByteString getData() {
        return data;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }
}
