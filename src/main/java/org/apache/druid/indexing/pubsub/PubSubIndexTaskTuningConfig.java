package org.apache.druid.indexing.pubsub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;


@JsonTypeName("PubSubTuningConfig")
public class PubSubIndexTaskTuningConfig extends SeekableStreamIndexTaskTuningConfig {
    @JsonCreator
    public PubSubIndexTaskTuningConfig(
            @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
            @JsonProperty("maxBytesInMemory") @Nullable Long maxBytesInMemory,
            @JsonProperty("maxRowsPerSegment") @Nullable Integer maxRowsPerSegment,
            @JsonProperty("maxTotalRows") @Nullable Long maxTotalRows,
            @JsonProperty("intermediatePersistPeriod") @Nullable Period intermediatePersistPeriod,
            @JsonProperty("basePersistDirectory") @Nullable File basePersistDirectory,
            @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
            @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
            @JsonProperty("indexSpecForIntermediatePersists") @Nullable IndexSpec indexSpecForIntermediatePersists,
            // This parameter is left for compatibility when reading existing configs, to be removed in Druid 0.12.
            @JsonProperty("buildV9Directly") @Nullable Boolean buildV9Directly,
            @Deprecated @JsonProperty("reportParseExceptions") @Nullable Boolean reportParseExceptions,
            @JsonProperty("handoffConditionTimeout") @Nullable Long handoffConditionTimeout,
            @JsonProperty("resetOffsetAutomatically") @Nullable Boolean resetOffsetAutomatically,
            @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
            @JsonProperty("intermediateHandoffPeriod") @Nullable Period intermediateHandoffPeriod,
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
                false,
                segmentWriteOutMediumFactory,
                intermediateHandoffPeriod,
                logParseExceptions,
                maxParseExceptions,
                maxSavedParseExceptions
        );
    }

    @Override
    public PubSubIndexTaskTuningConfig withBasePersistDirectory(File dir)
    {
        return new PubSubIndexTaskTuningConfig(
                getMaxRowsInMemory(),
                getMaxBytesInMemory(),
                getMaxRowsPerSegment(),
                getMaxTotalRows(),
                getIntermediatePersistPeriod(),
                dir,
                getMaxPendingPersists(),
                getIndexSpec(),
                getIndexSpecForIntermediatePersists(),
                true,
                isReportParseExceptions(),
                getHandoffConditionTimeout(),
                isResetOffsetAutomatically(),
                getSegmentWriteOutMediumFactory(),
                getIntermediateHandoffPeriod(),
                isLogParseExceptions(),
                getMaxParseExceptions(),
                getMaxSavedParseExceptions()
        );
    }


    @Override
    public String toString()
    {
        return "PubSubIndexTaskTuningConfig{" +
                "maxRowsInMemory=" + getMaxRowsInMemory() +
                ", maxRowsPerSegment=" + getMaxRowsPerSegment() +
                ", maxTotalRows=" + getMaxTotalRows() +
                ", maxBytesInMemory=" + getMaxBytesInMemory() +
                ", intermediatePersistPeriod=" + getIntermediatePersistPeriod() +
                ", basePersistDirectory=" + getBasePersistDirectory() +
                ", maxPendingPersists=" + getMaxPendingPersists() +
                ", indexSpec=" + getIndexSpec() +
                ", indexSpecForIntermediatePersists=" + getIndexSpecForIntermediatePersists() +
                ", reportParseExceptions=" + isReportParseExceptions() +
                ", handoffConditionTimeout=" + getHandoffConditionTimeout() +
                ", resetOffsetAutomatically=" + isResetOffsetAutomatically() +
                ", segmentWriteOutMediumFactory=" + getSegmentWriteOutMediumFactory() +
                ", intermediateHandoffPeriod=" + getIntermediateHandoffPeriod() +
                ", logParseExceptions=" + isLogParseExceptions() +
                ", maxParseExceptions=" + getMaxParseExceptions() +
                ", maxSavedParseExceptions=" + getMaxSavedParseExceptions() +
                '}';
    }
}
