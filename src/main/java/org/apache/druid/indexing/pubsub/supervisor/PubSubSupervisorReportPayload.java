package org.apache.druid.indexing.pubsub.supervisor;

import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorReportPayload;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class PubSubSupervisorReportPayload extends SeekableStreamSupervisorReportPayload<Integer, Long> {
    public PubSubSupervisorReportPayload(
            String dataSource,
            String topic,
            int partitions,
            int replicas,
            long durationSeconds,
            @Nullable Map<Integer, Long> latestOffsets,
            @Nullable Map<Integer, Long> minimumLag,
            @Nullable Long aggregateLag,
            @Nullable DateTime offsetsLastUpdated,
            boolean suspended,
            boolean healthy,
            SupervisorStateManager.State state,
            SupervisorStateManager.State detailedState,
            List<SupervisorStateManager.ExceptionEvent> recentErrors
    )
    {
        super(
                dataSource,
                topic,
                partitions,
                replicas,
                durationSeconds,
                latestOffsets,
                minimumLag,
                aggregateLag,
                offsetsLastUpdated,
                suspended,
                healthy,
                state,
                detailedState,
                recentErrors
        );
    }

    @Override
    public String toString()
    {
        return "PubSubSupervisorReportPayload{" +
                "dataSource='" + getDataSource() + '\'' +
                ", topic='" + getStream() + '\'' +
                ", partitions=" + getPartitions() +
                ", replicas=" + getReplicas() +
                ", durationSeconds=" + getDurationSeconds() +
                ", active=" + getActiveTasks() +
                ", publishing=" + getPublishingTasks() +
                (getLatestOffsets() != null ? ", latestOffsets=" + getLatestOffsets() : "") +
                (getMinimumLag() != null ? ", minimumLag=" + getMinimumLag() : "") +
                (getAggregateLag() != null ? ", aggregateLag=" + getAggregateLag() : "") +
                (getOffsetsLastUpdated() != null ? ", sequenceLastUpdated=" + getOffsetsLastUpdated() : "") +
                ", suspended=" + isSuspended() +
                ", healthy=" + isHealthy() +
                ", state=" + getState() +
                ", detailedState=" + getDetailedState() +
                ", recentErrors=" + getRecentErrors() +
                '}';
    }
}
