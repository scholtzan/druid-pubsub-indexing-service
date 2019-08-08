package org.apache.druid.indexing.pubsub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.joda.time.Duration;

public class PubSubIndexTaskClientFactory implements IndexTaskClientFactory<PubSubIndexTaskClient> {
    private static final EmittingLogger log = new EmittingLogger(PubSubIndexTaskClientFactory.class);

    private final HttpClient httpClient;
    private final ObjectMapper mapper;

    @Inject
    public PubSubIndexTaskClientFactory(
            @EscalatedGlobal HttpClient httpClient,
            @Json ObjectMapper mapper
    ) {
        log.info("Init Pub/Sub IndexTaskClientFactory");
        this.httpClient = httpClient;
        this.mapper = mapper;
    }

    @Override
    public PubSubIndexTaskClient build(
            TaskInfoProvider taskInfoProvider,
            String callerId,
            int numThreads,
            Duration httpTimeout,
            long numRetries
    ) {
        log.info("Init PubSubIndexTaskClient");
        return new PubSubIndexTaskClient(
                httpClient,
                mapper,
                taskInfoProvider,
                httpTimeout,
                callerId,
                numThreads,
                numRetries
        );
    }
}
