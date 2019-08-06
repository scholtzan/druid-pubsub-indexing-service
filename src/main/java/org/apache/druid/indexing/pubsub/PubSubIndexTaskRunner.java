package org.apache.druid.indexing.pubsub;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.io.IOUtils;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.discovery.NodeType;
import org.apache.druid.indexer.IngestionState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.stats.RowIngestionMeters;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.RealtimeIndexTask;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.realtime.appenderator.*;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.utils.CircularBuffer;
import org.joda.time.DateTime;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPInputStream;

public class PubSubIndexTaskRunner {
    private static final EmittingLogger log = new EmittingLogger(PubSubIndexTaskRunner.class);

    public enum Status {
        NOT_STARTED,
        STARTING,
        READING,
        PAUSED,
        PUBLISHING
    }

    private final Lock pauseLock = new ReentrantLock();
    private final Condition hasPaused = pauseLock.newCondition();
    private final Condition shouldResume = pauseLock.newCondition();

    protected final AtomicBoolean stopRequested = new AtomicBoolean(false);
    private final AtomicBoolean publishOnStop = new AtomicBoolean(false);

    private final Object statusLock = new Object();

    protected final Lock pollRetryLock = new ReentrantLock();
    protected final Condition isAwaitingRetry = pollRetryLock.newCondition();

    private final PubSubIndexTask task;
    private final PubSubIndexTaskIOConfig ioConfig;
    private final PubSubIndexTaskTuningConfig tuningConfig;
    private final InputRowParser<ByteBuffer> parser;
    private final AuthorizerMapper authorizerMapper;
    private final CircularBuffer<Throwable> savedParseExceptions;
    private final RowIngestionMeters rowIngestionMeters;

    private final Set<String> publishingSequences = Sets.newConcurrentHashSet();
    private final List<ListenableFuture<SegmentsAndMetadata>> publishWaitList = new ArrayList<>();
    private final List<ListenableFuture<SegmentsAndMetadata>> handOffWaitList = new ArrayList<>();

    private volatile DateTime startTime;
    private volatile Status status = Status.NOT_STARTED; // this is only ever set by the task runner thread (runThread)
    private volatile TaskToolbox toolbox;
    private volatile Thread runThread;
    private volatile Appenderator appenderator;
    private volatile BatchAppenderatorDriver driver;
    private volatile IngestionState ingestionState;
    private volatile Throwable backgroundThreadException;

    protected volatile boolean pauseRequested = false;


    public PubSubIndexTaskRunner(
            PubSubIndexTask task,
            InputRowParser<ByteBuffer> parser,
            AuthorizerMapper authorizerMapper,
            CircularBuffer<Throwable> savedParseExceptions,
            RowIngestionMetersFactory rowIngestionMetersFactory
    ) {
        Preconditions.checkNotNull(task);
        this.task = task;
        this.ioConfig = task.getIOConfig();
        this.tuningConfig = task.getTuningConfig();
        this.parser = parser;
        this.authorizerMapper = authorizerMapper;
        this.savedParseExceptions = savedParseExceptions;
        this.rowIngestionMeters = rowIngestionMetersFactory.createRowIngestionMeters();
        this.ingestionState = IngestionState.NOT_STARTED;
    }

    @Nonnull
    protected List<PubSubReceivedMessage> getMessages(
            PubSubMessageSupplier messageSupplier
    ) {
        return messageSupplier.poll();
    }

    public TaskStatus run(TaskToolbox toolbox) {
        try {
            return runInternal(toolbox);
        } catch (Exception e) {
            log.error(e, "Encountered exception while running task.");
            final String errorMsg = Throwables.getStackTraceAsString(e);
            return TaskStatus.failure(
                    task.getId(),
                    errorMsg
            );
        }
    }

    public void setToolbox(TaskToolbox toolbox) {
        this.toolbox = toolbox;
    }

    private TaskStatus runInternal(TaskToolbox toolbox) throws Exception {
        log.info("PubSubIndexTaskRunner starting up!");
        startTime = DateTimes.nowUtc();
        status = Status.STARTING;

        setToolbox(toolbox);

        runThread = Thread.currentThread();

        final String lookupTier = task.getContextValue(RealtimeIndexTask.CTX_KEY_LOOKUP_TIER);
        final LookupNodeService lookupNodeService = lookupTier == null ?
                toolbox.getLookupNodeService() :
                new LookupNodeService(lookupTier);

        final DiscoveryDruidNode discoveryDruidNode = new DiscoveryDruidNode(
                toolbox.getDruidNode(),
                NodeType.PEON,
                ImmutableMap.of(
                        toolbox.getDataNodeService().getName(), toolbox.getDataNodeService(),
                        lookupNodeService.getName(), lookupNodeService
                )
        );

        PubSubMessageSupplier messageSupplier = task.newTaskMessageSupplier();

        toolbox.getDataSegmentServerAnnouncer().announce();
        toolbox.getDruidNodeAnnouncer().announce(discoveryDruidNode);

        SegmentAllocator segmentAllocator = task.createSegmentAllocator(toolbox);
        driver = task.newDriver(appenderator, toolbox, segmentAllocator);

        // Start up, set up initial sequences.
        final Object restoredMetadata = driver.startJob();
        if (restoredMetadata == null) {
            // no persist has happened so far
            // so either this is a brand new task or replacement of a failed task
            // todo?
        } else {
            @SuppressWarnings("unchecked") final Map<String, Object> restoredMetadataMap = (Map) restoredMetadata;
            // todo?
        }

        ingestionState = IngestionState.BUILD_SEGMENTS;

        // Main loop.
        boolean stillReading = true;
        status = Status.READING;

        try {
            while (stillReading) {
                if (possiblyPause()) {
                    stopRequested.set(true);
                }

                if (stopRequested.get()) {
                    status = Status.PUBLISHING;
                }

                if (stopRequested.get()) {
                    break;
                }

                if (backgroundThreadException != null) {
                    throw new RuntimeException(backgroundThreadException);
                }

                List<PubSubReceivedMessage> messages = messageSupplier.poll();

                for (PubSubReceivedMessage message : messages) {
                    log.trace(
                            "Got message attributes[%s].",
                            message.getAttributes()
                    );

                    String decompressedData = new String(message.getData().toByteArray());
                    ObjectMapper mapper = new ObjectMapper();

                    if (task.getDecompressData()) {
                        GZIPInputStream inputStream = new GZIPInputStream(new ByteArrayInputStream(message.getData().toByteArray()));
                        decompressedData = IOUtils.toString(inputStream);
                    }

                    TypeReference<HashMap<String, Object>> typeRef
                            = new TypeReference<HashMap<String, Object>>() {
                    };
                    HashMap<String, Object> dataMap = mapper.readValue(decompressedData, typeRef);
                    dataMap.putAll(message.getAttributes());

                    String jsonData = mapper.writeValueAsString(dataMap);
                    byte[] jsonBytes = jsonData.getBytes();

                    final List<InputRow> rows;
                    rows = new ArrayList<>(parser.parseBatch(ByteBuffer.wrap(jsonBytes)));

                    for (InputRow row : rows) {
                        if (row != null && task.withinMinMaxRecordTime(row)) {
                            String sequenceName = task.getId();
                            final AppenderatorDriverAddResult addResult = driver.add(row, sequenceName);

                            if (addResult.isOk()) {
                                if (addResult.isPushRequired(ioConfig.getMaxRowsPerSegment(), ioConfig.getMaxTotalRows())) {
                                    final SegmentsAndMetadata pushed = driver.pushAllAndClear(ioConfig.getPushTimeout());
                                    log.info("Pushed segments[%s]", pushed.getSegments());
                                }
                            } else {
                                throw new ISE("Could not allocate segment for row with timestamp[%s]", row.getTimestamp());
                            }

                            if (addResult.getParseException() != null) {
                                rowIngestionMeters.incrementUnparseable();
                            } else {
                                rowIngestionMeters.incrementProcessed();
                            }
                        } else {
                            rowIngestionMeters.incrementThrownAway();
                        }
                    }
                }

                if (messages.isEmpty()) {
                    stillReading = false;
                }
            }

            synchronized (statusLock) {
                if (stopRequested.get() && !publishOnStop.get()) {
                    throw new InterruptedException("Stopping without publishing");
                }

                status = Status.PUBLISHING;
            }

            rowIngestionMeters.incrementProcessed();

            appenderator.close();
        } catch (Exception e) {
            log.info("The task was asked to stop before completing");
        }

        return TaskStatus.success(task.getId());
    }

    private boolean possiblyPause() throws InterruptedException {
        pauseLock.lockInterruptibly();
        try {
            if (pauseRequested) {
                status = Status.PAUSED;
                hasPaused.signalAll();

                while (pauseRequested) {
                    log.info("Pausing ingestion until resumed");
                    shouldResume.await();
                }

                status = Status.READING;
                shouldResume.signalAll();
                log.info("Ingestion loop resumed");
                return true;
            }
        } finally {
            pauseLock.unlock();
        }

        return false;
    }

    public Appenderator getAppenderator() {
        return appenderator;
    }
}
