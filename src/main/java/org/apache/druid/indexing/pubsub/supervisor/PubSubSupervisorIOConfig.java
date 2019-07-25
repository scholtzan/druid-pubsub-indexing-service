package org.apache.druid.indexing.pubsub.supervisor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.Period;

import java.util.Map;

public class PubSubSupervisorIOConfig extends SeekableStreamSupervisorIOConfig {
    public static final String BOOTSTRAP_SERVERS_KEY = "bootstrap.servers";
    public static final String TRUST_STORE_PASSWORD_KEY = "ssl.truststore.password";
    public static final String KEY_STORE_PASSWORD_KEY = "ssl.keystore.password";
    public static final String KEY_PASSWORD_KEY = "ssl.key.password";
    public static final long DEFAULT_POLL_TIMEOUT_MILLIS = 100;

    private final Map<String, Object> consumerProperties;
    private final long pollTimeout;


    @JsonCreator
    public PubSubSupervisorIOConfig(
            @JsonProperty("topic") String topic,
            @JsonProperty("replicas") Integer replicas,
            @JsonProperty("taskCount") Integer taskCount,
            @JsonProperty("taskDuration") Period taskDuration,
            @JsonProperty("consumerProperties") Map<String, Object> consumerProperties,
            @JsonProperty("pollTimeout") Long pollTimeout,
            @JsonProperty("startDelay") Period startDelay,
            @JsonProperty("period") Period period,
            @JsonProperty("useEarliestOffset") Boolean useEarliestOffset,
            @JsonProperty("completionTimeout") Period completionTimeout,
            @JsonProperty("lateMessageRejectionPeriod") Period lateMessageRejectionPeriod,
            @JsonProperty("earlyMessageRejectionPeriod") Period earlyMessageRejectionPeriod
    )
    {
        super(
                Preconditions.checkNotNull(topic, "topic"),
                replicas,
                taskCount,
                taskDuration,
                startDelay,
                period,
                useEarliestOffset,
                completionTimeout,
                lateMessageRejectionPeriod,
                earlyMessageRejectionPeriod
        );

        this.consumerProperties = Preconditions.checkNotNull(consumerProperties, "consumerProperties");
        Preconditions.checkNotNull(
                consumerProperties.get(BOOTSTRAP_SERVERS_KEY),
                StringUtils.format("consumerProperties must contain entry for [%s]", BOOTSTRAP_SERVERS_KEY)
        );
        this.pollTimeout = pollTimeout != null ? pollTimeout : DEFAULT_POLL_TIMEOUT_MILLIS;
    }

    @JsonProperty
    public String getTopic()
    {
        return getStream();
    }

    @JsonProperty
    public Map<String, Object> getConsumerProperties()
    {
        return consumerProperties;
    }

    @JsonProperty
    public long getPollTimeout()
    {
        return pollTimeout;
    }

    @JsonProperty
    public boolean isUseEarliestOffset()
    {
        return isUseEarliestSequenceNumber();
    }

    @Override
    public String toString()
    {
        return "PubSubSupervisorIOConfig{" +
                "topic='" + getTopic() + '\'' +
                ", replicas=" + getReplicas() +
                ", taskCount=" + getTaskCount() +
                ", taskDuration=" + getTaskDuration() +
                ", consumerProperties=" + consumerProperties +
                ", pollTimeout=" + pollTimeout +
                ", startDelay=" + getStartDelay() +
                ", period=" + getPeriod() +
                ", useEarliestOffset=" + isUseEarliestOffset() +
                ", completionTimeout=" + getCompletionTimeout() +
                ", earlyMessageRejectionPeriod=" + getEarlyMessageRejectionPeriod() +
                ", lateMessageRejectionPeriod=" + getLateMessageRejectionPeriod() +
                '}';
    }
}
