package org.apache.druid.indexing.pubsub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorConfig;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Objects;


@JsonTypeName("PubSubTuningConfig")
public class PubSubIndexTaskTuningConfig implements TuningConfig, AppenderatorConfig {
    private static final EmittingLogger log = new EmittingLogger(PubSubIndexTaskTuningConfig.class);

    private static final int DEFAULT_MAX_ROWS_PER_SEGMENT = 5_000_000;
    private static final boolean DEFAULT_RESET_OFFSET_AUTOMATICALLY = false;
    private static final boolean DEFAULT_SKIP_SEQUENCE_NUMBER_AVAILABILITY_CHECK = false;
    private static final int DEFAULT_MAX_TOTAL_ROWS = 25000;

    private final int maxRowsInMemory;
    private final long maxBytesInMemory;
    private final int maxRowsPerSegment;
    private final Long maxTotalRows;
    private final Period intermediatePersistPeriod;
    private final File basePersistDirectory;
    @Deprecated
    private final int maxPendingPersists;
    private final IndexSpec indexSpec;
    private final boolean reportParseExceptions;
    private final long handoffConditionTimeout;
    private final boolean resetOffsetAutomatically;
    @Nullable
    private final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;
    private final Period intermediateHandoffPeriod;
    private final boolean skipSequenceNumberAvailabilityCheck;

    private final boolean logParseExceptions;
    private final int maxParseExceptions;
    private final int maxSavedParseExceptions;
    private final IndexSpec indexSpecForIntermediatePersists;

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
            @JsonProperty("handoffConditionTimeout") @Nullable Long handoffConditionTimeout,
            @JsonProperty("resetOffsetAutomatically") @Nullable Boolean resetOffsetAutomatically,
            @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
            @JsonProperty("intermediateHandoffPeriod") @Nullable Period intermediateHandoffPeriod,
            @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
            @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
            @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions
    ) {
        log.info("Init PubSubIndexTaskTuningConfig");
        // Cannot be a static because default basePersistDirectory is unique per-instance
        final RealtimeTuningConfig defaults = RealtimeTuningConfig.makeDefaultTuningConfig(basePersistDirectory);

        this.maxRowsInMemory = maxRowsInMemory == null ? defaults.getMaxRowsInMemory() : maxRowsInMemory;
        this.maxRowsPerSegment = maxRowsPerSegment == null ? DEFAULT_MAX_ROWS_PER_SEGMENT : maxRowsPerSegment;
        this.maxTotalRows = maxTotalRows == null ? DEFAULT_MAX_TOTAL_ROWS : maxTotalRows;
        // initializing this to 0, it will be lazily initialized to a value
        // @see server.src.main.java.org.apache.druid.segment.indexing.TuningConfigs#getMaxBytesInMemoryOrDefault(long)
        this.maxBytesInMemory = maxBytesInMemory == null ? 0 : maxBytesInMemory;
        this.intermediatePersistPeriod = intermediatePersistPeriod == null
                ? defaults.getIntermediatePersistPeriod()
                : intermediatePersistPeriod;
        this.basePersistDirectory = defaults.getBasePersistDirectory();
        this.maxPendingPersists = maxPendingPersists == null ? 0 : maxPendingPersists;
        this.indexSpec = indexSpec == null ? defaults.getIndexSpec() : indexSpec;
        this.reportParseExceptions = defaults.isReportParseExceptions();
        this.handoffConditionTimeout = handoffConditionTimeout == null
                ? defaults.getHandoffConditionTimeout()
                : handoffConditionTimeout;
        this.resetOffsetAutomatically = resetOffsetAutomatically == null
                ? DEFAULT_RESET_OFFSET_AUTOMATICALLY
                : resetOffsetAutomatically;
        this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;
        this.intermediateHandoffPeriod = intermediateHandoffPeriod == null
                ? new Period().withDays(Integer.MAX_VALUE)
                : intermediateHandoffPeriod;
        this.skipSequenceNumberAvailabilityCheck = DEFAULT_SKIP_SEQUENCE_NUMBER_AVAILABILITY_CHECK;
        this.indexSpecForIntermediatePersists = indexSpecForIntermediatePersists == null ?
                this.indexSpec : indexSpecForIntermediatePersists;

        if (this.reportParseExceptions) {
            this.maxParseExceptions = 0;
            this.maxSavedParseExceptions = maxSavedParseExceptions == null ? 0 : Math.min(1, maxSavedParseExceptions);
        } else {
            this.maxParseExceptions = maxParseExceptions == null
                    ? TuningConfig.DEFAULT_MAX_PARSE_EXCEPTIONS
                    : maxParseExceptions;
            this.maxSavedParseExceptions = maxSavedParseExceptions == null
                    ? TuningConfig.DEFAULT_MAX_SAVED_PARSE_EXCEPTIONS
                    : maxSavedParseExceptions;
        }
        this.logParseExceptions = logParseExceptions == null
                ? TuningConfig.DEFAULT_LOG_PARSE_EXCEPTIONS
                : logParseExceptions;
    }

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
    @JsonProperty
    public int getMaxRowsInMemory() {
        return maxRowsInMemory;
    }

    @Override
    @JsonProperty
    public long getMaxBytesInMemory() {
        return maxBytesInMemory;
    }

    @Override
    @JsonProperty
    public Integer getMaxRowsPerSegment() {
        return maxRowsPerSegment;
    }

    @JsonProperty
    @Override
    public Long getMaxTotalRows() {
        return maxTotalRows;
    }

    @Override
    @JsonProperty
    public Period getIntermediatePersistPeriod() {
        return intermediatePersistPeriod;
    }

    @Override
    @JsonProperty
    public File getBasePersistDirectory() {
        return basePersistDirectory;
    }


    @Override
    @JsonProperty
    @Deprecated
    public int getMaxPendingPersists() {
        return maxPendingPersists;
    }

    @Override
    @JsonProperty
    public IndexSpec getIndexSpec() {
        return indexSpec;
    }

    /**
     * Always returns true, doesn't affect the version being built.
     */
    @Deprecated
    @JsonProperty
    public boolean getBuildV9Directly() {
        return true;
    }

    @Override
    @JsonProperty
    public boolean isReportParseExceptions() {
        return reportParseExceptions;
    }

    @JsonProperty
    public long getHandoffConditionTimeout() {
        return handoffConditionTimeout;
    }

    @JsonProperty
    public boolean isResetOffsetAutomatically() {
        return resetOffsetAutomatically;
    }

    @Override
    @JsonProperty
    @Nullable
    public SegmentWriteOutMediumFactory getSegmentWriteOutMediumFactory() {
        return segmentWriteOutMediumFactory;
    }

    @JsonProperty
    public Period getIntermediateHandoffPeriod() {
        return intermediateHandoffPeriod;
    }

    @JsonProperty
    @Override
    public IndexSpec getIndexSpecForIntermediatePersists()
    {
        return indexSpecForIntermediatePersists;
    }


    @JsonProperty
    public boolean isLogParseExceptions() {
        return logParseExceptions;
    }

    @JsonProperty
    public int getMaxParseExceptions() {
        return maxParseExceptions;
    }

    @JsonProperty
    public int getMaxSavedParseExceptions() {
        return maxSavedParseExceptions;
    }

    @JsonProperty
    public boolean isSkipSequenceNumberAvailabilityCheck() {
        return skipSequenceNumberAvailabilityCheck;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PubSubIndexTaskTuningConfig that = (PubSubIndexTaskTuningConfig) o;
        return maxRowsInMemory == that.maxRowsInMemory &&
                maxBytesInMemory == that.maxBytesInMemory &&
                maxRowsPerSegment == that.maxRowsPerSegment &&
                maxPendingPersists == that.maxPendingPersists &&
                reportParseExceptions == that.reportParseExceptions &&
                handoffConditionTimeout == that.handoffConditionTimeout &&
                resetOffsetAutomatically == that.resetOffsetAutomatically &&
                skipSequenceNumberAvailabilityCheck == that.skipSequenceNumberAvailabilityCheck &&
                logParseExceptions == that.logParseExceptions &&
                maxParseExceptions == that.maxParseExceptions &&
                maxSavedParseExceptions == that.maxSavedParseExceptions &&
                Objects.equals(maxTotalRows, that.maxTotalRows) &&
                Objects.equals(intermediatePersistPeriod, that.intermediatePersistPeriod) &&
                Objects.equals(basePersistDirectory, that.basePersistDirectory) &&
                Objects.equals(indexSpec, that.indexSpec) &&
                Objects.equals(segmentWriteOutMediumFactory, that.segmentWriteOutMediumFactory) &&
                Objects.equals(intermediateHandoffPeriod, that.intermediateHandoffPeriod);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                maxRowsInMemory,
                maxBytesInMemory,
                maxRowsPerSegment,
                maxTotalRows,
                intermediatePersistPeriod,
                basePersistDirectory,
                maxPendingPersists,
                indexSpec,
                reportParseExceptions,
                handoffConditionTimeout,
                resetOffsetAutomatically,
                segmentWriteOutMediumFactory,
                intermediateHandoffPeriod,
                skipSequenceNumberAvailabilityCheck,
                logParseExceptions,
                maxParseExceptions,
                maxSavedParseExceptions
        );
    }

    @Override
    public String toString() {
        return "PubSubIndexTaskTuningConfig{" +
                "maxRowsInMemory=" + getMaxRowsInMemory() +
                ", maxRowsPerSegment=" + getMaxRowsPerSegment() +
                ", maxTotalRows=" + getMaxTotalRows() +
                ", maxBytesInMemory=" + getMaxBytesInMemory() +
                ", intermediatePersistPeriod=" + getIntermediatePersistPeriod() +
                ", basePersistDirectory=" + getBasePersistDirectory() +
                ", maxPendingPersists=" + getMaxPendingPersists() +
                ", indexSpec=" + getIndexSpec() +
                ", indexSpecForIntermediatePersists=" + "false" +
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
