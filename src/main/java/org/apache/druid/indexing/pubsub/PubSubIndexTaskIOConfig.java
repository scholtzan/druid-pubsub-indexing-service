package org.apache.druid.indexing.pubsub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import org.apache.druid.segment.indexing.IOConfig;
import org.joda.time.DateTime;
import org.threeten.bp.Duration;

// todo: io config provided by submitted tasks (is that even the case for pubsub?)
public class PubSubIndexTaskIOConfig implements IOConfig {
    private final Optional<DateTime> minimumMessageTime;
    private final Optional<DateTime> maximumMessageTime;
    private final int maxMessagesPerPoll;
    private final int maxMessageSizePerPoll;
    private final Duration keepAliveTime;
    private final Duration keepAliveTimeout;
    private final int maxRowsPerSegment;
    private final long maxTotalRows;
    private final int pushTimeout;
    private final org.joda.time.Duration taskCheckDuration;

    @JsonCreator
    public PubSubIndexTaskIOConfig(
            @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
            @JsonProperty("maximumMessageTime") DateTime maximumMessageTime,
            @JsonProperty("maxMessagesPerPoll") int maxMessagesPerPoll,
            @JsonProperty("maxMessageSizePerPoll") int maxMessageSizePerPoll,
            @JsonProperty("keepAliveTime") Duration keepAliveTime,
            @JsonProperty("keepAliveTimeout") Duration keepAliveTimeout,
            @JsonProperty("maxRowsPerSegment") int maxRowsPerSegment,
            @JsonProperty("maxTotalRows") long maxTotalRows,
            @JsonProperty("pushTimeout") int pushTimeout,
            @JsonProperty("taskCheckDuration") org.joda.time.Duration taskCheckDuration
    ) {
        this.minimumMessageTime = Optional.fromNullable(minimumMessageTime);
        this.maximumMessageTime = Optional.fromNullable(maximumMessageTime);
        this.keepAliveTime = keepAliveTime;
        this.keepAliveTimeout = keepAliveTimeout;
        this.maxMessageSizePerPoll = maxMessageSizePerPoll;
        this.maxMessagesPerPoll = maxMessagesPerPoll;
        this.pushTimeout = pushTimeout;
        this.maxTotalRows = maxTotalRows;
        this.maxRowsPerSegment = maxRowsPerSegment;
        this.taskCheckDuration = taskCheckDuration;
    }

    @JsonProperty
    public Optional<DateTime> getMaximumMessageTime() {
        return maximumMessageTime;
    }

    @JsonProperty
    public Optional<DateTime> getMinimumMessageTime() {
        return minimumMessageTime;
    }

    @JsonProperty
    public int getMaxMessagesPerPoll() {
        return maxMessagesPerPoll;
    }

    @JsonProperty
    public int getMaxMessageSizePerPoll() {
        return maxMessageSizePerPoll;
    }

    public Duration getKeepAliveTime() {
        return keepAliveTime;
    }

    public Duration getKeepAliveTimeout() {
        return keepAliveTimeout;
    }

    public org.joda.time.Duration getTaskCheckDuration() {
        return this.taskCheckDuration;
    }

    @Override
    public String toString() {
        return "PubSubIndexTaskIOConfig{" +
                "minimumMessageTime=" + getMinimumMessageTime() +
                ", maximumMessageTime=" + getMaximumMessageTime() +
                '}';
    }

    public int getMaxRowsPerSegment() {
        return maxRowsPerSegment;
    }

    public long getMaxTotalRows() {
        return maxTotalRows;
    }

    public int getPushTimeout() {
        return pushTimeout;
    }
}
