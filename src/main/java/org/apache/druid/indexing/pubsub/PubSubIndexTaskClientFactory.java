package org.apache.druid.indexing.pubsub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClientFactory;
import org.apache.druid.java.util.http.client.HttpClient;
import org.joda.time.Duration;

public class PubSubIndexTaskClientFactory extends SeekableStreamIndexTaskClientFactory<PubSubIndexTaskClient> {
    @Inject
    public PubSubIndexTaskClientFactory(
            @EscalatedGlobal HttpClient httpClient,
            @Json ObjectMapper mapper
    )
    {
        super(httpClient, mapper);
    }

    @Override
    public PubSubIndexTaskClient build(
            TaskInfoProvider taskInfoProvider,
            String dataSource,
            int numThreads,
            Duration httpTimeout,
            long numRetries
    )
    {
        return new PubSubIndexTaskClient(
                getHttpClient(),
                getMapper(),
                taskInfoProvider,
                dataSource,
                numThreads,
                httpTimeout,
                numRetries
        );
    }
}
