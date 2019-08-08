package org.apache.druid.indexing.pubsub.supervisor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.threeten.bp.Duration;

public class PubSubSupervisorIOConfig {
    public static final int DEFAULT_POLL_TIMEOUT_MILLIS = 100;

    private final String topic;
    private final String projectId;
    private final String subscriptionId;
    private final int pollTimeout;
    private final Integer replicas;
    private final Integer taskCount;
    private final org.joda.time.Duration taskDuration;
    private final org.joda.time.Duration startDelay;
    private final org.joda.time.Duration period;
    private final org.joda.time.Duration completionTimeout;
    private final Optional<org.joda.time.Duration> lateMessageRejectionPeriod;
    private final Optional<org.joda.time.Duration> earlyMessageRejectionPeriod;
    private int maxMessagesPerPoll;
    private int maxMessageSizePerPoll;
    private Duration keepAliveTime;
    private Duration keepAliveTimeout;
    private boolean decompressData;
    private final Optional<DateTime> minimumMessageTime;
    private final Optional<DateTime> maximumMessageTime;

    @JsonCreator
    public PubSubSupervisorIOConfig(
            @JsonProperty("topic") String topic,
            @JsonProperty("subscriptionId") String subscriptionId,
            @JsonProperty("projectId") String projectId,
            @JsonProperty("replicas") Integer replicas,
            @JsonProperty("taskCount") Integer taskCount,
            @JsonProperty("taskDuration") Period taskDuration,
            @JsonProperty("pollTimeout") Integer pollTimeout,
            @JsonProperty("startDelay") Period startDelay,
            @JsonProperty("period") Period period,
            @JsonProperty("useEarliestOffset") Boolean useEarliestOffset,
            @JsonProperty("completionTimeout") Period completionTimeout,
            @JsonProperty("lateMessageRejectionPeriod") Period lateMessageRejectionPeriod,
            @JsonProperty("earlyMessageRejectionPeriod") Period earlyMessageRejectionPeriod,
            @JsonProperty("maxMessagesPerPoll") int maxMessagesPerPoll,
            @JsonProperty("maxMessageSizePerPoll") int maxMessageSizePerPoll,
            @JsonProperty("keepAliveTime") Duration keepAliveTime,
            @JsonProperty("keepAliveTimeout") Duration keepAliveTimeout,
            @JsonProperty("decompressData") boolean decompressData,
            @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
            @JsonProperty("maximumMessageTime") DateTime maximumMessageTime
    )
    {
        this.pollTimeout = pollTimeout != null ? pollTimeout : DEFAULT_POLL_TIMEOUT_MILLIS;
        this.subscriptionId = subscriptionId;
        this.projectId = projectId;
        this.topic = Preconditions.checkNotNull(topic, "stream cannot be null");
        this.replicas = replicas != null ? replicas : 1;
        this.taskCount = taskCount != null ? taskCount : 1;
        this.taskDuration = defaultDuration(taskDuration, "PT1H");
        this.startDelay = defaultDuration(startDelay, "PT5S");
        this.period = defaultDuration(period, "PT30S");
        this.completionTimeout = defaultDuration(completionTimeout, "PT30M");
        this.lateMessageRejectionPeriod = lateMessageRejectionPeriod == null
                ? Optional.absent()
                : Optional.of(lateMessageRejectionPeriod.toStandardDuration());
        this.earlyMessageRejectionPeriod = earlyMessageRejectionPeriod == null
                ? Optional.absent()
                : Optional.of(earlyMessageRejectionPeriod.toStandardDuration());
        this.maxMessagesPerPoll = maxMessagesPerPoll;
        this.maxMessageSizePerPoll = maxMessageSizePerPoll;
        this.keepAliveTime = keepAliveTime;
        this.keepAliveTimeout = keepAliveTimeout;
        this.decompressData = decompressData;
        this.minimumMessageTime = Optional.fromNullable(minimumMessageTime);
        this.maximumMessageTime = Optional.fromNullable(maximumMessageTime);
    }

    private static org.joda.time.Duration defaultDuration(final Period period, final String theDefault)
    {
        return (period == null ? new Period(theDefault) : period).toStandardDuration();
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
    public String getTopic()
    {
        return this.topic;
    }

    @JsonProperty
    public String getSubscriptionId() {
        return this.subscriptionId;
    }

    @JsonProperty
    public String getProjectId() {
        return this.projectId;
    }

    @JsonProperty
    public Integer getReplicas()
    {
        return replicas;
    }

    @JsonProperty
    public Integer getTaskCount()
    {
        return taskCount;
    }

    @JsonProperty
    public org.joda.time.Duration getTaskDuration()
    {
        return taskDuration;
    }

    @JsonProperty
    public org.joda.time.Duration getStartDelay()
    {
        return startDelay;
    }

    @JsonProperty
    public org.joda.time.Duration getPeriod()
    {
        return period;
    }

    @JsonProperty
    public org.joda.time.Duration getCompletionTimeout()
    {
        return completionTimeout;
    }

    @JsonProperty
    public Optional<org.joda.time.Duration> getEarlyMessageRejectionPeriod()
    {
        return earlyMessageRejectionPeriod;
    }

    @JsonProperty
    public Optional<org.joda.time.Duration> getLateMessageRejectionPeriod()
    {
        return lateMessageRejectionPeriod;
    }

    @JsonProperty
    public int getPollTimeout()
    {
        return pollTimeout;
    }

    @JsonProperty
    public boolean getDecompressData() {
        return decompressData;
    }

    @Override
    public String toString()
    {
        return "PubSubSupervisorIOConfig{" +
                "topic='" + getTopic() + '\'' +
                ", subscriptionId=" + getSubscriptionId() +
                ", projectId=" + getProjectId() +
                ", replicas=" + getReplicas() +
                ", taskCount=" + getTaskCount() +
                ", taskDuration=" + getTaskDuration() +
                ", pollTimeout=" + pollTimeout +
                ", startDelay=" + getStartDelay() +
                ", period=" + getPeriod() +
                ", completionTimeout=" + getCompletionTimeout() +
                ", earlyMessageRejectionPeriod=" + getEarlyMessageRejectionPeriod() +
                ", lateMessageRejectionPeriod=" + getLateMessageRejectionPeriod() +
                '}';
    }

    public int getMaxMessagesPerPoll() {
        return maxMessagesPerPoll;
    }

    public int getMaxMessageSizePerPoll() {
        return maxMessageSizePerPoll;
    }

    public Duration getKeepAliveTime() {
        return keepAliveTime;
    }

    public Duration getKeepAliveTimeout() {
        return keepAliveTimeout;
    }
}
