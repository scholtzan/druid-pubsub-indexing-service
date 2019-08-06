package org.apache.druid.indexing.pubsub;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.repackaged.com.google.common.annotations.VisibleForTesting;
import com.google.common.base.*;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import org.apache.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.SurrogateAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.seekablestream.utils.RandomIdUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.*;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.utils.CircularBuffer;

import java.util.Map;

public class PubSubIndexTask extends AbstractTask implements ChatHandler {
    private static final String TYPE = "index_pubsub";
    private static final EmittingLogger log = new EmittingLogger(PubSubIndexTask.class);

    private final PubSubIndexTaskRunner runner;
    protected final DataSchema dataSchema;
    protected final PubSubIndexTaskTuningConfig tuningConfig;
    protected final PubSubIndexTaskIOConfig ioConfig;
    protected final Optional<ChatHandlerProvider> chatHandlerProvider;
    protected final Map<String, Object> context;
    protected final AuthorizerMapper authorizerMapper;
    protected final RowIngestionMetersFactory rowIngestionMetersFactory;
    protected final CircularBuffer<Throwable> savedParseExceptions;
    private final String projectId;
    private final String subscriptionId;
    private final boolean decompressData;
    private final String supervisorTaskId;

    private final Supplier<PubSubIndexTaskRunner> runnerSupplier;

    @JsonCreator
    public PubSubIndexTask(
            @JsonProperty("id") String id,
            @JsonProperty("projectId") String projectId,
            @JsonProperty("subscriptionId") String subscriptionId,
            @JsonProperty("supervisorTaskId") String supervisorTaskId,
            @JsonProperty("decompressData") boolean decompressData,
            @JsonProperty("resource") TaskResource taskResource,
            @JsonProperty("dataSchema") DataSchema dataSchema,
            @JsonProperty("tuningConfig") PubSubIndexTaskTuningConfig tuningConfig,
            @JsonProperty("ioConfig") PubSubIndexTaskIOConfig ioConfig,
            @JsonProperty("context") Map<String, Object> context,
            @JacksonInject ChatHandlerProvider chatHandlerProvider,
            @JacksonInject AuthorizerMapper authorizerMapper,
            @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory,
            @JacksonInject ObjectMapper configMapper,
            @JacksonInject final String groupId
    ) {
        super(
                id,
                groupId,
                taskResource,
                dataSchema.getDataSource(),
                context
        );

        this.projectId = projectId;
        this.subscriptionId = subscriptionId;
        this.decompressData = decompressData;
        this.supervisorTaskId = supervisorTaskId;
        this.dataSchema = Preconditions.checkNotNull(dataSchema, "dataSchema");
        this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig");
        this.ioConfig = Preconditions.checkNotNull(ioConfig, "ioConfig");
        this.chatHandlerProvider = Optional.fromNullable(chatHandlerProvider);
        if (tuningConfig.getMaxSavedParseExceptions() > 0) {
            savedParseExceptions = new CircularBuffer<>(tuningConfig.getMaxSavedParseExceptions());
        } else {
            savedParseExceptions = null;
        }
        this.context = context;
        this.authorizerMapper = authorizerMapper;
        this.rowIngestionMetersFactory = rowIngestionMetersFactory;
        this.runnerSupplier = Suppliers.memoize(this::createTaskRunner);
        this.runner = createTaskRunner();
    }

    protected PubSubIndexTaskRunner createTaskRunner() {
        //noinspection unchecked
        return new PubSubIndexTaskRunner(
                this,
                dataSchema.getParser(),
                authorizerMapper,
                savedParseExceptions,
                rowIngestionMetersFactory
        );
    }

    @Override
    public TaskStatus run(final TaskToolbox toolbox) {
        return getRunner().run(toolbox);
    }

    private static String makeTaskId(String dataSource, String type) {
        final String suffix = RandomIdUtils.getRandomId();
        return Joiner.on("_").join(type, dataSource, suffix);
    }

    protected static String getFormattedId(String dataSource, String type) {
        return makeTaskId(dataSource, type);
    }

    protected static String getFormattedGroupId(String dataSource, String type) {
        return StringUtils.format("%s_%s", type, dataSource);
    }

    @Override
    public int getPriority() {
        return getContextValue(Tasks.PRIORITY_KEY, Tasks.DEFAULT_REALTIME_TASK_PRIORITY);
    }

    @JsonProperty
    public DataSchema getDataSchema() {
        return dataSchema;
    }

    @Override
    public boolean isReady(TaskActionClient taskActionClient) {
        return true;
    }

    @JsonProperty
    public PubSubIndexTaskTuningConfig getTuningConfig() {
        return tuningConfig;
    }

    @JsonProperty("ioConfig")
    public PubSubIndexTaskIOConfig getIOConfig() {
        return ioConfig;
    }

    @JsonProperty
    public boolean getDecompressData() {
        return decompressData;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public Appenderator newAppenderator(FireDepartmentMetrics metrics, TaskToolbox toolbox) {
        return Appenderators.createRealtime(
                dataSchema,
                tuningConfig.withBasePersistDirectory(toolbox.getPersistDir()),
                metrics,
                toolbox.getSegmentPusher(),
                toolbox.getObjectMapper(),
                toolbox.getIndexIO(),
                toolbox.getIndexMergerV9(),
                toolbox.getQueryRunnerFactoryConglomerate(),
                toolbox.getSegmentAnnouncer(),
                toolbox.getEmitter(),
                toolbox.getQueryExecutorService(),
                toolbox.getCache(),
                toolbox.getCacheConfig(),
                toolbox.getCachePopulatorStats()
        );
    }

    public BatchAppenderatorDriver newDriver(
            final Appenderator appenderator,
            final TaskToolbox toolbox,
            final SegmentAllocator segmentAllocator
    )
    {
        return new BatchAppenderatorDriver(
                appenderator,
                segmentAllocator,
                new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
                toolbox.getDataSegmentKiller()
        );
    }

    public SegmentAllocator createSegmentAllocator(TaskToolbox toolbox)
    {
        // todo: append to existing
        return new ActionBasedSegmentAllocator(
                toolbox.getTaskActionClient(),
                dataSchema,
                (schema, row, sequenceName, previousSegmentId, skipSegmentLineageCheck) -> new SurrogateAction<>(
                        supervisorTaskId,
                        new SegmentAllocateAction(
                                schema.getDataSource(),
                                row.getTimestamp(),
                                schema.getGranularitySpec().getQueryGranularity(),
                                schema.getGranularitySpec().getSegmentGranularity(),
                                sequenceName,
                                previousSegmentId,
                                skipSegmentLineageCheck
                        )
                )
        );
    }

    public boolean withinMinMaxRecordTime(final InputRow row) {
        final boolean beforeMinimumMessageTime = ioConfig.getMinimumMessageTime().isPresent()
                && ioConfig.getMinimumMessageTime().get().isAfter(row.getTimestamp());

        final boolean afterMaximumMessageTime = ioConfig.getMaximumMessageTime().isPresent()
                && ioConfig.getMaximumMessageTime().get().isBefore(row.getTimestamp());

        if (!Intervals.ETERNITY.contains(row.getTimestamp())) {
            final String errorMsg = StringUtils.format(
                    "Encountered row with timestamp that cannot be represented as a long: [%s]",
                    row
            );
            throw new ParseException(errorMsg);
        }

        if (log.isDebugEnabled()) {
            if (beforeMinimumMessageTime) {
                log.debug(
                        "CurrentTimeStamp[%s] is before MinimumMessageTime[%s]",
                        row.getTimestamp(),
                        ioConfig.getMinimumMessageTime().get()
                );
            } else if (afterMaximumMessageTime) {
                log.debug(
                        "CurrentTimeStamp[%s] is after MaximumMessageTime[%s]",
                        row.getTimestamp(),
                        ioConfig.getMaximumMessageTime().get()
                );
            }
        }

        return !beforeMinimumMessageTime && !afterMaximumMessageTime;
    }

    protected PubSubMessageSupplier newTaskMessageSupplier() {
        return new PubSubMessageSupplier(
                this.projectId,
                this.subscriptionId,
                ioConfig.getMaxMessagesPerPoll(),
                ioConfig.getMaxMessageSizePerPoll(),
                ioConfig.getKeepAliveTime(),
                ioConfig.getKeepAliveTimeout()
        );
    }

    public Appenderator getAppenderator() {
        return runner.getAppenderator();
    }

    @VisibleForTesting
    public PubSubIndexTaskRunner getRunner() {
        return runnerSupplier.get();
    }
}
