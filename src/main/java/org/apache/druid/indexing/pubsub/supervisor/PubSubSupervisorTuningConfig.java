package org.apache.druid.indexing.pubsub.supervisor;

import org.apache.druid.indexing.pubsub.PubSubIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorTuningConfig;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.indexing.TuningConfigs;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;

public class PubSubSupervisorTuningConfig extends PubSubIndexTaskTuningConfig
{
    private static final String DEFAULT_OFFSET_FETCH_PERIOD = "PT30S";
    private static final String DEFAULT_HTTP_TIMEOUT = "PT10S";
    private static final String DEFAULT_SHUTDOWN_TIMEOUT = "PT80S";
    private static final int DEFAULT_CHAT_RETRIES = 8;

    private final Integer workerThreads;
    private final Integer chatThreads;
    private final Long chatRetries;
    private final Duration httpTimeout;
    private final Duration shutdownTimeout;
    private final Duration offsetFetchPeriod;

    public PubSubSupervisorTuningConfig(
            @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
            @JsonProperty("maxBytesInMemory") Long maxBytesInMemory,
            @JsonProperty("maxRowsPerSegment") Integer maxRowsPerSegment,
            @JsonProperty("maxTotalRows") Long maxTotalRows,
            @JsonProperty("intermediatePersistPeriod") Period intermediatePersistPeriod,
            @JsonProperty("basePersistDirectory") File basePersistDirectory,
            @JsonProperty("maxPendingPersists") Integer maxPendingPersists,
            @JsonProperty("indexSpec") IndexSpec indexSpec,
            @JsonProperty("indexSpecForIntermediatePersists") @Nullable IndexSpec indexSpecForIntermediatePersists,
            // This parameter is left for compatibility when reading existing configs, to be removed in Druid 0.12.
            @JsonProperty("buildV9Directly") Boolean buildV9Directly,
            @JsonProperty("reportParseExceptions") Boolean reportParseExceptions,
            @JsonProperty("handoffConditionTimeout") Long handoffConditionTimeout,
            @JsonProperty("resetOffsetAutomatically") Boolean resetOffsetAutomatically,
            @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
            @JsonProperty("workerThreads") Integer workerThreads,
            @JsonProperty("chatThreads") Integer chatThreads,
            @JsonProperty("chatRetries") Long chatRetries,
            @JsonProperty("httpTimeout") Period httpTimeout,
            @JsonProperty("shutdownTimeout") Period shutdownTimeout,
            @JsonProperty("offsetFetchPeriod") Period offsetFetchPeriod,
            @JsonProperty("intermediateHandoffPeriod") Period intermediateHandoffPeriod,
            @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
            @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
            @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions
    )
    {
        super(
                maxRowsInMemory,
                maxBytesInMemory,
                maxRowsPerSegment,
                maxTotalRows,
                intermediatePersistPeriod,
                basePersistDirectory,
                maxPendingPersists,
                indexSpec,
                indexSpecForIntermediatePersists,
                true,
                reportParseExceptions,
                handoffConditionTimeout,
                resetOffsetAutomatically,
                segmentWriteOutMediumFactory,
                intermediateHandoffPeriod,
                logParseExceptions,
                maxParseExceptions,
                maxSavedParseExceptions
        );
        this.workerThreads = workerThreads;
        this.chatThreads = chatThreads;
        this.chatRetries = (chatRetries != null ? chatRetries : DEFAULT_CHAT_RETRIES);
        this.httpTimeout = SeekableStreamSupervisorTuningConfig.defaultDuration(httpTimeout, DEFAULT_HTTP_TIMEOUT);
        this.shutdownTimeout = SeekableStreamSupervisorTuningConfig.defaultDuration(
                shutdownTimeout,
                DEFAULT_SHUTDOWN_TIMEOUT
        );
        this.offsetFetchPeriod = SeekableStreamSupervisorTuningConfig.defaultDuration(
                offsetFetchPeriod,
                DEFAULT_OFFSET_FETCH_PERIOD
        );
    }

    @JsonProperty
    public Integer getWorkerThreads()
    {
        return workerThreads;
    }

    @JsonProperty
    public Integer getChatThreads()
    {
        return chatThreads;
    }

    @JsonProperty
    public Long getChatRetries()
    {
        return chatRetries;
    }

    @JsonProperty
    public Duration getHttpTimeout()
    {
        return httpTimeout;
    }

    @JsonProperty
    public Duration getShutdownTimeout()
    {
        return shutdownTimeout;
    }

    @JsonProperty
    public Duration getOffsetFetchPeriod()
    {
        return offsetFetchPeriod;
    }

    @Override
    public String toString()
    {
        return "PubSubSupervisorTuningConfig{" +
                "maxRowsInMemory=" + getMaxRowsInMemory() +
                ", maxRowsPerSegment=" + getMaxRowsPerSegment() +
                ", maxTotalRows=" + getMaxTotalRows() +
                ", maxBytesInMemory=" + TuningConfigs.getMaxBytesInMemoryOrDefault(getMaxBytesInMemory()) +
                ", intermediatePersistPeriod=" + getIntermediatePersistPeriod() +
                ", basePersistDirectory=" + getBasePersistDirectory() +
                ", maxPendingPersists=" + getMaxPendingPersists() +
                ", indexSpec=" + getIndexSpec() +
                ", reportParseExceptions=" + isReportParseExceptions() +
                ", handoffConditionTimeout=" + getHandoffConditionTimeout() +
                ", resetOffsetAutomatically=" + isResetOffsetAutomatically() +
                ", segmentWriteOutMediumFactory=" + getSegmentWriteOutMediumFactory() +
                ", workerThreads=" + workerThreads +
                ", chatThreads=" + chatThreads +
                ", chatRetries=" + chatRetries +
                ", httpTimeout=" + httpTimeout +
                ", shutdownTimeout=" + shutdownTimeout +
                ", offsetFetchPeriod=" + offsetFetchPeriod +
                ", intermediateHandoffPeriod=" + getIntermediateHandoffPeriod() +
                ", logParseExceptions=" + isLogParseExceptions() +
                ", maxParseExceptions=" + getMaxParseExceptions() +
                ", maxSavedParseExceptions=" + getMaxSavedParseExceptions() +
                '}';
    }
}