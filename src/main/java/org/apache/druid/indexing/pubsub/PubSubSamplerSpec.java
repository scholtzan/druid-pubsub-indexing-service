package org.apache.druid.indexing.pubsub;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.indexing.overlord.sampler.FirehoseSampler;
import org.apache.druid.indexing.overlord.sampler.SamplerConfig;
import org.apache.druid.indexing.pubsub.supervisor.PubSubSupervisorIOConfig;
import org.apache.druid.indexing.pubsub.supervisor.PubSubSupervisorSpec;
import org.apache.druid.indexing.seekablestream.SeekableStreamSamplerSpec;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;

import java.util.HashMap;
import java.util.Map;

public class PubSubSamplerSpec extends SeekableStreamSamplerSpec {
    private final ObjectMapper objectMapper;

    @JsonCreator
    public KafkaSamplerSpec(
            @JsonProperty("spec") final PubSubSupervisorSpec ingestionSpec,
            @JsonProperty("samplerConfig") final SamplerConfig samplerConfig,
            @JacksonInject FirehoseSampler firehoseSampler,
            @JacksonInject ObjectMapper objectMapper
    )
    {
        super(ingestionSpec, samplerConfig, firehoseSampler);

        this.objectMapper = objectMapper;
    }

    @Override
    protected Firehose getFirehose(InputRowParser parser)
    {
        return new KafkaSamplerFirehose(parser);
    }

    protected class KafkaSamplerFirehose extends SeekableStreamSamplerFirehose
    {
        private KafkaSamplerFirehose(InputRowParser parser)
        {
            super(parser);
        }

        @Override
        protected RecordSupplier getRecordSupplier()
        {
            ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

                final Map<String, Object> props = new HashMap<>(((PubSubSupervisorIOConfig) ioConfig).getConsumerProperties());

                props.put("enable.auto.commit", "false");
                props.put("auto.offset.reset", "none");
                props.put("key.deserializer", ByteArrayDeserializer.class.getName());
                props.put("value.deserializer", ByteArrayDeserializer.class.getName());
                props.put("request.timeout.ms", Integer.toString(samplerConfig.getTimeoutMs()));

                return new PubSubRecordSupplier(props, objectMapper);
            }
            finally {
                Thread.currentThread().setContextClassLoader(currCtxCl);
            }
        }
    }
}
