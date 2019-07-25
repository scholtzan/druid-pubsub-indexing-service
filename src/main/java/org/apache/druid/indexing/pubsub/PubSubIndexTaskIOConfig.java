package org.apache.druid.indexing.pubsub;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.pubsub.supervisor.PubSubSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Map;

public class PubSubIndexTaskIOConfig extends SeekableStreamIndexTaskIOConfig<Integer, Long> {
    private final Map<String, Object> consumerProperties;
    private final long pollTimeout;

    @JsonCreator
    public PubSubIndexTaskIOConfig(
            @JsonProperty("taskGroupId") @Nullable Integer taskGroupId, // can be null for backward compabitility
            @JsonProperty("baseSequenceName") String baseSequenceName,
            // startPartitions and endPartitions exist to be able to read old ioConfigs in metadata store
            @JsonProperty("startPartitions") @Nullable
            @Deprecated SeekableStreamEndSequenceNumbers<Integer, Long> startPartitions,
            @JsonProperty("endPartitions") @Nullable
            @Deprecated SeekableStreamEndSequenceNumbers<Integer, Long> endPartitions,
            // startSequenceNumbers and endSequenceNumbers must be set for new versions
            @JsonProperty("startSequenceNumbers")
            @Nullable SeekableStreamStartSequenceNumbers<Integer, Long> startSequenceNumbers,
            @JsonProperty("endSequenceNumbers")
            @Nullable SeekableStreamEndSequenceNumbers<Integer, Long> endSequenceNumbers,
            @JsonProperty("consumerProperties") Map<String, Object> consumerProperties,
            @JsonProperty("pollTimeout") Long pollTimeout,
            @JsonProperty("useTransaction") Boolean useTransaction,
            @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
            @JsonProperty("maximumMessageTime") DateTime maximumMessageTime
    )
    {
        super(
                taskGroupId,
                baseSequenceName,
                startSequenceNumbers == null
                        ? Preconditions.checkNotNull(startPartitions, "startPartitions").asStartPartitions(true)
                        : startSequenceNumbers,
                endSequenceNumbers == null ? endPartitions : endSequenceNumbers,
                useTransaction,
                minimumMessageTime,
                maximumMessageTime
        );

        this.consumerProperties = Preconditions.checkNotNull(consumerProperties, "consumerProperties");
        this.pollTimeout = pollTimeout != null ? pollTimeout : PubSubSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS;

        final SeekableStreamEndSequenceNumbers<Integer, Long> myEndSequenceNumbers = getEndSequenceNumbers();
        for (int partition : myEndSequenceNumbers.getPartitionSequenceNumberMap().keySet()) {
            Preconditions.checkArgument(
                    myEndSequenceNumbers.getPartitionSequenceNumberMap()
                            .get(partition)
                            .compareTo(getStartSequenceNumbers().getPartitionSequenceNumberMap().get(partition)) >= 0,
                    "end offset must be >= start offset for partition[%s]",
                    partition
            );
        }
    }

    public PubSubIndexTaskIOConfig(
            int taskGroupId,
            String baseSequenceName,
            SeekableStreamStartSequenceNumbers<Integer, Long> startSequenceNumbers,
            SeekableStreamEndSequenceNumbers<Integer, Long> endSequenceNumbers,
            Map<String, Object> consumerProperties,
            Long pollTimeout,
            Boolean useTransaction,
            DateTime minimumMessageTime,
            DateTime maximumMessageTime
    )
    {
        this(
                taskGroupId,
                baseSequenceName,
                null,
                null,
                startSequenceNumbers,
                endSequenceNumbers,
                consumerProperties,
                pollTimeout,
                useTransaction,
                minimumMessageTime,
                maximumMessageTime
        );
    }

    /**
     * This method is for compatibilty so that newer version of KafkaIndexTaskIOConfig can be read by
     * old version of Druid. Note that this method returns end sequence numbers instead of start. This is because
     * {@link SeekableStreamStartSequenceNumbers} didn't exist before.
     */
    @JsonProperty
    @Deprecated
    public SeekableStreamEndSequenceNumbers<Integer, Long> getStartPartitions()
    {
        // Converting to start sequence numbers. This is allowed for Kafka because the start offset is always inclusive.
        final SeekableStreamStartSequenceNumbers<Integer, Long> startSequenceNumbers = getStartSequenceNumbers();
        return new SeekableStreamEndSequenceNumbers<>(
                startSequenceNumbers.getStream(),
                startSequenceNumbers.getPartitionSequenceNumberMap()
        );
    }

    /**
     * This method is for compatibility so that newer version of PubSubIndexTaskIOConfig can be read by
     * old version of Druid.
     */
    @JsonProperty
    @Deprecated
    public SeekableStreamEndSequenceNumbers<Integer, Long> getEndPartitions()
    {
        return getEndSequenceNumbers();
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

    @Override
    public String toString()
    {
        return "PubSubIndexTaskIOConfig{" +
                "taskGroupId=" + getTaskGroupId() +
                ", baseSequenceName='" + getBaseSequenceName() + '\'' +
                ", startSequenceNumbers=" + getStartSequenceNumbers() +
                ", endSequenceNumbers=" + getEndSequenceNumbers() +
                ", consumerProperties=" + consumerProperties +
                ", pollTimeout=" + pollTimeout +
                ", useTransaction=" + isUseTransaction() +
                ", minimumMessageTime=" + getMinimumMessageTime() +
                ", maximumMessageTime=" + getMaximumMessageTime() +
                '}';
    }
}
