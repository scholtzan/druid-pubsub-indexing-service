package org.apache.druid.indexing.pubsub;

import org.apache.druid.indexing.seekablestream.utils.RandomIdUtils;
import org.apache.druid.java.util.common.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class PubSubConsumerConfigs {
    public static Map<String, Object> getConsumerProperties()
    {
        final Map<String, Object> props = new HashMap<>();
        props.put("metadata.max.age.ms", "10000");
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        props.put("group.id", StringUtils.format("kafka-supervisor-%s", RandomIdUtils.getRandomId()));
        props.put("auto.offset.reset", "none");
        props.put("enable.auto.commit", "false");
        props.put("isolation.level", "read_committed");
        return props;
    }

}
