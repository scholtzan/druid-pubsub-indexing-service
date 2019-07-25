package org.apache.druid.indexing.pubsub;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.seekablestream.SeekableStreamDataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.SeekableStreamSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SequenceMetadata;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.utils.CircularBuffer;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class IncrementalPublishingPubSubIndexTaskRunner extends SeekableStreamIndexTaskRunner<Integer, Long> {
    private static final EmittingLogger log = new EmittingLogger(IncrementalPublishingPubSubIndexTaskRunner.class);
    private final PubSubIndexTask task;

    public IncrementalPublishingPubSubIndexTaskRunner(
            PubSubIndexTask task,
            InputRowParser<ByteBuffer> parser,
            AuthorizerMapper authorizerMapper,
            Optional<ChatHandlerProvider> chatHandlerProvider,
            CircularBuffer<Throwable> savedParseExceptions,
            RowIngestionMetersFactory rowIngestionMetersFactory,
            LockGranularity lockGranularityToUse
    )
    {
        super(
                task,
                parser,
                authorizerMapper,
                chatHandlerProvider,
                savedParseExceptions,
                rowIngestionMetersFactory,
                lockGranularityToUse
        );
        this.task = task;
    }

    @Override
    protected Long getNextStartOffset(@NotNull Long sequenceNumber)
    {
        return sequenceNumber + 1;
    }

    @Nonnull
    @Override
    protected List<OrderedPartitionableRecord<Integer, Long>> getRecords(
            RecordSupplier<Integer, Long> recordSupplier,
            TaskToolbox toolbox
    ) throws Exception
    {
        // Handles OffsetOutOfRangeException, which is thrown if the seeked-to
        // offset is not present in the topic-partition. This can happen if we're asking a task to read from data
        // that has not been written yet (which is totally legitimate). So let's wait for it to show up.
        List<OrderedPartitionableRecord<Integer, Long>> records = new ArrayList<>();
        try {
            records = recordSupplier.poll(task.getIOConfig().getPollTimeout());
        }
        catch (OffsetOutOfRangeException e) {
            log.warn("OffsetOutOfRangeException with message [%s]", e.getMessage());
            possiblyResetOffsetsOrWait(e.offsetOutOfRangePartitions(), recordSupplier, toolbox);
        }

        return records;
    }

    @Override
    protected SeekableStreamEndSequenceNumbers<Integer, Long> deserializePartitionsFromMetadata(
            ObjectMapper mapper,
            Object object
    )
    {
        return mapper.convertValue(object, mapper.getTypeFactory().constructParametrizedType(
                SeekableStreamEndSequenceNumbers.class,
                SeekableStreamEndSequenceNumbers.class,
                Integer.class,
                Long.class
        ));
    }

    private void possiblyResetOffsetsOrWait(
            Map<TopicPartition, Long> outOfRangePartitions,
            RecordSupplier<Integer, Long> recordSupplier,
            TaskToolbox taskToolbox
    ) throws InterruptedException, IOException
    {
        final Map<TopicPartition, Long> resetPartitions = new HashMap<>();
        boolean doReset = false;
        if (task.getTuningConfig().isResetOffsetAutomatically()) {
            for (Map.Entry<TopicPartition, Long> outOfRangePartition : outOfRangePartitions.entrySet()) {
                final TopicPartition topicPartition = outOfRangePartition.getKey();
                final long nextOffset = outOfRangePartition.getValue();
                // seek to the beginning to get the least available offset
                StreamPartition<Integer> streamPartition = StreamPartition.of(
                        topicPartition.topic(),
                        topicPartition.partition()
                );
                final Long leastAvailableOffset = recordSupplier.getEarliestSequenceNumber(streamPartition);
                if (leastAvailableOffset == null) {
                    throw new ISE(
                            "got null sequence number for partition[%s] when fetching from pubsub!",
                            topicPartition.partition()
                    );
                }
                // reset the seek
                recordSupplier.seek(streamPartition, nextOffset);
                // Reset consumer offset if resetOffsetAutomatically is set to true
                // and the current message offset in the pubsub partition is more than the
                // next message offset that we are trying to fetch
                if (leastAvailableOffset > nextOffset) {
                    doReset = true;
                    resetPartitions.put(topicPartition, nextOffset);
                }
            }
        }

        if (doReset) {
            sendResetRequestAndWait(CollectionUtils.mapKeys(resetPartitions, streamPartition -> StreamPartition.of(
                    streamPartition.topic(),
                    streamPartition.partition()
            )), taskToolbox);
        } else {
            log.warn("Retrying in %dms", task.getPollRetryMs());
            pollRetryLock.lockInterruptibly();
            try {
                long nanos = TimeUnit.MILLISECONDS.toNanos(task.getPollRetryMs());
                while (nanos > 0L && !pauseRequested && !stopRequested.get()) {
                    nanos = isAwaitingRetry.awaitNanos(nanos);
                }
            }
            finally {
                pollRetryLock.unlock();
            }
        }
    }

    @Override
    protected SeekableStreamDataSourceMetadata<Integer, Long> createDataSourceMetadata(
            SeekableStreamSequenceNumbers<Integer, Long> partitions
    )
    {
        return new PubSubDataSourceMetadata(partitions);
    }

    @Override
    protected OrderedSequenceNumber<Long> createSequenceNumber(Long sequenceNumber)
    {
        return PubSubSequenceNumber.of(sequenceNumber);
    }

    @Override
    protected void possiblyResetDataSourceMetadata(
            TaskToolbox toolbox,
            RecordSupplier<Integer, Long> recordSupplier,
            Set<StreamPartition<Integer>> assignment
    )
    {
        // do nothing
    }

    @Override
    protected boolean isEndOffsetExclusive()
    {
        return true;
    }

    @Override
    protected boolean isEndOfShard(Long seqNum)
    {
        return false;
    }

    @Override
    public TypeReference<List<SequenceMetadata<Integer, Long>>> getSequenceMetadataTypeReference()
    {
        return new TypeReference<List<SequenceMetadata<Integer, Long>>>()
        {
        };
    }

    @Nullable
    @Override
    protected TreeMap<Integer, Map<Integer, Long>> getCheckPointsFromContext(
            TaskToolbox toolbox,
            String checkpointsString
    ) throws IOException
    {
        if (checkpointsString != null) {
            log.info("Checkpoints [%s]", checkpointsString);
            return toolbox.getObjectMapper().readValue(
                    checkpointsString,
                    new TypeReference<TreeMap<Integer, Map<Integer, Long>>>()
                    {
                    }
            );
        } else {
            return null;
        }
    }
}
