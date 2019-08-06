package org.apache.druid.indexing.pubsub.supervisor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.IndexTaskClient;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.HadoopIndexTask;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.overlord.*;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorReport;
import org.apache.druid.indexing.pubsub.*;
import org.apache.druid.indexing.seekablestream.*;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.utils.RandomIdUtils;
import org.apache.druid.java.util.common.*;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.timeline.DataSegment;
import org.jcodings.util.Hash;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static javax.xml.crypto.dsig.SignatureProperties.TYPE;


public class PubSubSupervisor implements Supervisor {
    private static final String TASK_PREFIX = "index_pubsub";
    private static final EmittingLogger log = new EmittingLogger(PubSubSupervisor.class);
    private static final long MINIMUM_GET_OFFSET_PERIOD_MILLIS = 5000;
    private static final long INITIAL_GET_OFFSET_DELAY_MILLIS = 15000;
    private static final long MINIMUM_FUTURE_TIMEOUT_IN_SECONDS = 120;
    private static final Interval ALL_INTERVAL = Intervals.of("0000-01-01/3000-01-01");
    private static final int DEFAULT_MAX_TASK_COUNT = 1;
    private static final long INITIAL_EMIT_LAG_METRIC_DELAY_MILLIS = 25000;
    private static final Long NOT_SET = -1L;
    private static final Long END_OF_PARTITION = Long.MAX_VALUE;
    private static final long MAX_RUN_FREQUENCY_MILLIS = 1000;
    private static final int MAX_INITIALIZATION_RETRIES = 20;

    final IndexerMetadataStorageCoordinator metadataStorageCoordinator;
    final PubSubIndexTaskClientFactory taskClientFactory;
    final PubSubIndexTaskClient taskClient;
    private final RowIngestionMetersFactory rowIngestionMetersFactory;
    final TaskStorage taskStorage;
    final TaskMaster taskMaster;
    private final String supervisorId;
    private final TaskInfoProvider taskInfoProvider;
    private final PubSubSupervisorIOConfig ioConfig;
    private final PubSubSupervisorTuningConfig tuningConfig;
    private final String dataSource;
    private final Object stopLock = new Object();
    private final Object stateChangeLock = new Object();
    private final Object recordSupplierLock = new Object();
    private final long futureTimeoutInSeconds; // how long to wait for async operations to complete
    private ListeningScheduledExecutorService exec = null;
    private ListenableFuture<?> future = null;
    private final Object taskLock = new Object();
    private final PubSubIndexTaskIOConfig taskConfig;
    private final Map<Interval, PubSubIndexTask> runningTasks = new HashMap<>();
    private final Map<Interval, String> runningVersion = new HashMap<>();
    private final int maxTaskCount;

    private boolean listenerRegistered = false;
    private long lastRunTime;
    private int initRetryCounter = 0;
    private volatile DateTime firstRunTime;
    private final PubSubSupervisorSpec spec;
    private volatile PubSubMessageSupplier supplier;
    private volatile boolean started = false;
    private volatile boolean stopped = false;
    private volatile boolean lifecycleStarted = false;


    public PubSubSupervisor(
            final String supervisorId,
            final TaskStorage taskStorage,
            final TaskMaster taskMaster,
            final IndexerMetadataStorageCoordinator metadataStorageCoordinator,
            final PubSubIndexTaskClientFactory taskClientFactory,
            final ObjectMapper mapper,
            final PubSubSupervisorSpec spec,
            final RowIngestionMetersFactory rowIngestionMetersFactory,
            final PubSubIndexTaskIOConfig taskConfig
    ) {
        this.supervisorId = supervisorId;
        this.spec = spec;
        this.taskStorage = taskStorage;
        this.taskMaster = taskMaster;
        this.metadataStorageCoordinator = metadataStorageCoordinator;
        this.rowIngestionMetersFactory = rowIngestionMetersFactory;
        this.taskClientFactory = taskClientFactory;
        this.ioConfig = spec.getIoConfig();
        this.tuningConfig = spec.getTuningConfig();
        this.dataSource = spec.getDataSchema().getDataSource();
        this.taskConfig = taskConfig;
        this.maxTaskCount = spec.getContext().containsKey("maxTaskCount")
                ? Integer.parseInt(String.valueOf(spec.getContext().get("maxTaskCount")))
                : DEFAULT_MAX_TASK_COUNT;

        this.taskInfoProvider = new TaskInfoProvider()
        {
            @Override
            public TaskLocation getTaskLocation(final String id)
            {
                Preconditions.checkNotNull(id, "id");
                Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
                if (taskRunner.isPresent()) {
                    Optional<? extends TaskRunnerWorkItem> item = Iterables.tryFind(
                            taskRunner.get().getRunningTasks(),
                            (Predicate<TaskRunnerWorkItem>) taskRunnerWorkItem -> id.equals(taskRunnerWorkItem.getTaskId())
                    );

                    if (item.isPresent()) {
                        return item.get().getLocation();
                    }
                } else {
                    log.error("Failed to get task runner because I'm not the leader!");
                }

                return TaskLocation.unknown();
            }

            @Override
            public Optional<TaskStatus> getTaskStatus(String id)
            {
                return taskStorage.getStatus(id);
            }
        };

        this.futureTimeoutInSeconds = Math.max(
                MINIMUM_FUTURE_TIMEOUT_IN_SECONDS,
                tuningConfig.getChatRetries() * (tuningConfig.getHttpTimeout().getStandardSeconds()
                        + IndexTaskClient.MAX_RETRY_WAIT_SECONDS)
        );

        int chatThreads = (this.tuningConfig.getChatThreads() != null
                ? this.tuningConfig.getChatThreads()
                : Math.min(10, this.ioConfig.getTaskCount() * this.ioConfig.getReplicas()));
        this.taskClient = taskClientFactory.build(
                taskInfoProvider,
                dataSource,
                chatThreads,
                this.tuningConfig.getHttpTimeout(),
                this.tuningConfig.getChatRetries()
        );
        log.info(
                "Created taskClient with dataSource[%s] chatThreads[%d] httpTimeout[%s] chatRetries[%d]",
                dataSource,
                chatThreads,
                this.tuningConfig.getHttpTimeout(),
                this.tuningConfig.getChatRetries()
        );
    }


    protected PubSubMessageSupplier setupMessageSupplier() {
        return new PubSubMessageSupplier(
                ioConfig.getProjectId(),
                ioConfig.getSubscriptionId(),
                ioConfig.getMaxMessagesPerPoll(),
                ioConfig.getMaxMessageSizePerPoll(),
                ioConfig.getKeepAliveTime(),
                ioConfig.getKeepAliveTimeout()
        );
    }

    @Override
    public void start()
    {
        synchronized (stateChangeLock) {
            Preconditions.checkState(!lifecycleStarted, "already started");
            Preconditions.checkState(!exec.isShutdown(), "already stopped");

            exec = MoreExecutors.listeningDecorator(Execs.scheduledSingleThreaded(supervisorId));
            final Duration delay = taskConfig.getTaskCheckDuration();
            future = exec.scheduleWithFixedDelay(
                    PubSubSupervisor.this::run,
                    0,
                    delay.getMillis(),
                    TimeUnit.MILLISECONDS
            );
            lifecycleStarted = true;
        }
    }

    public void run()
    {
        try {
            if (spec.isSuspended()) {
                log.info(
                        "PubSub view supervisor[%s:%s] is suspended",
                        spec.getId(),
                        this.dataSource
                );
                return;
            }

            runInternal();
        }
        catch (Exception e) {
            log.makeAlert(e, StringUtils.format("uncaught exception in %s.", supervisorId)).emit();
        }
    }

    @Override
    public void stop(boolean stopGracefully)
    {
        synchronized (stateChangeLock) {
            Preconditions.checkState(started, "not started");
            // stop all schedulers and threads
            if (stopGracefully) {
                synchronized (taskLock) {
                    future.cancel(false);
                    future = null;
                    exec.shutdownNow();
                    exec = null;
                    clearTasks();
                }
            } else {
                future.cancel(true);
                future = null;
                exec.shutdownNow();
                exec = null;
                synchronized (taskLock) {
                    clearTasks();
                }
            }
            started = false;
        }
    }

    @Override
    public SupervisorReport getStatus()
    {
        return new PubSubSupervisorReport(
                dataSource,
                DateTimes.nowUtc(),
                spec.isSuspended()
        );
    }

    @Override
    public void reset(DataSourceMetadata dataSourceMetadata)
    {
        synchronized (taskLock) {
            clearTasks();
            clearSegments();
        }
    }

    @Override
    public void checkpoint(
            @Nullable Integer taskGroupId,
            String baseSequenceName,
            DataSourceMetadata previousCheckPoint,
            DataSourceMetadata currentCheckPoint
    )
    {
        // do nothing
    }

    private void runInternal() {
        // todo: overwrite taskio config?

        synchronized (taskLock) {
            List<Interval> intervalsToRemove = new ArrayList<>();
            for (Map.Entry<Interval, PubSubIndexTask> entry : runningTasks.entrySet()) {
                Optional<TaskStatus> taskStatus = taskStorage.getStatus(entry.getValue().getId());
                if (!taskStatus.isPresent() || !taskStatus.get().isRunnable()) {
                    intervalsToRemove.add(entry.getKey());
                }
            }
            for (Interval interval : intervalsToRemove) {
                runningTasks.remove(interval);
                runningVersion.remove(interval);
            }

            if (runningTasks.size() == maxTaskCount) {
                //if the number of running tasks reach the max task count, supervisor won't submit new tasks.
                return;
            }

            PubSubIndexTask indexTask = createTask();

            Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();

            if (taskQueue.isPresent()) {
                try {
                    taskQueue.get().add(indexTask);
                } catch (EntryExistsException e) {
                    log.error("Tried to add task [%s] but it already exists", indexTask.getId());
                }
            } else {
                log.error("Failed to get task queue because I'm not the leader!");
            }
        }
    }

    private PubSubIndexTask createTask() {
        String taskId = Joiner.on("_").join("pubsub_index", RandomIdUtils.getRandomId());

        return new PubSubIndexTask(
                taskId,
                ioConfig.getProjectId(),
                ioConfig.getSubscriptionId(),
                supervisorId,
                ioConfig.getDecompressData(),
                new TaskResource(taskId, 1),
                spec.getDataSchema(),
                tuningConfig,
                taskConfig,
                new HashMap<String, Object>(),
                null,
                null,
                rowIngestionMetersFactory,
                null,
                getFormattedGroupId(dataSource, TYPE)
        );
    }

    protected static String getFormattedGroupId(String dataSource, String type)
    {
        return StringUtils.format("%s_%s", type, dataSource);
    }

    @VisibleForTesting
    Pair<Map<Interval, PubSubIndexTask>, Map<Interval, String>> getRunningTasks()
    {
        return new Pair<>(runningTasks, runningVersion);
    }

    private void clearTasks()
    {
        for (PubSubIndexTask task : runningTasks.values()) {
            if (taskMaster.getTaskQueue().isPresent()) {
                taskMaster.getTaskQueue().get().shutdown(task.getId(), "killing all tasks");
            }
        }
        runningTasks.clear();
        runningVersion.clear();
    }

    private void clearSegments()
    {
        log.info("Clear all metadata of dataSource %s", dataSource);
        metadataStorageCoordinator.deletePendingSegments(dataSource, ALL_INTERVAL);
        metadataStorageCoordinator.deleteDataSourceMetadata(dataSource);
    }

    protected String baseTaskName() {
        return "index_pubsub";
    }

    @VisibleForTesting
    public PubSubSupervisorIOConfig getIoConfig() {
        return spec.getIoConfig();
    }
}
