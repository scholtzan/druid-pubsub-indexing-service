package org.apache.druid.indexing.pubsub.supervisor;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.pubsub.PubSubIndexTaskClientFactory;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorSpec;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;

import java.util.Map;


public class PubSubSupervisorSpec extends SeekableStreamSupervisorSpec {
    @JsonCreator
    public PubSubSupervisorSpec(
            @JsonProperty("dataSchema") DataSchema dataSchema,
            @JsonProperty("tuningConfig") PubSubSupervisorTuningConfig tuningConfig,
            @JsonProperty("ioConfig") PubSubSupervisorIOConfig ioConfig,
            @JsonProperty("context") Map<String, Object> context,
            @JsonProperty("suspended") Boolean suspended,
            @JacksonInject TaskStorage taskStorage,
            @JacksonInject TaskMaster taskMaster,
            @JacksonInject IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
            @JacksonInject PubSubIndexTaskClientFactory pubSubIndexTaskClientFactory,
            @JacksonInject @Json ObjectMapper mapper,
            @JacksonInject ServiceEmitter emitter,
            @JacksonInject DruidMonitorSchedulerConfig monitorSchedulerConfig,
            @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory,
            @JacksonInject SupervisorStateManagerConfig supervisorStateManagerConfig
    )
    {
        super(
                dataSchema,
                tuningConfig != null
                        ? tuningConfig
                        : new PubSubSupervisorTuningConfig(
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null
                ),
                ioConfig,
                context,
                suspended,
                taskStorage,
                taskMaster,
                indexerMetadataStorageCoordinator,
                pubSubIndexTaskClientFactory,
                mapper,
                emitter,
                monitorSchedulerConfig,
                rowIngestionMetersFactory,
                supervisorStateManagerConfig
        );
    }

    @Override
    public Supervisor createSupervisor()
    {
        return new PubSubSupervisor(
                taskStorage,
                taskMaster,
                indexerMetadataStorageCoordinator,
                (PubSubIndexTaskClientFactory) indexTaskClientFactory,
                mapper,
                this,
                rowIngestionMetersFactory
        );
    }

    @Override
    @JsonProperty
    public PubSubSupervisorTuningConfig getTuningConfig()
    {
        return (PubSubSupervisorTuningConfig) super.getTuningConfig();
    }

    @Override
    @JsonProperty
    public PubSubSupervisorIOConfig getIoConfig()
    {
        return (PubSubSupervisorIOConfig) super.getIoConfig();
    }

    @Override
    protected PubSubSupervisorSpec toggleSuspend(boolean suspend)
    {
        return new PubSubSupervisorSpec(
                getDataSchema(),
                getTuningConfig(),
                getIoConfig(),
                getContext(),
                suspend,
                taskStorage,
                taskMaster,
                indexerMetadataStorageCoordinator,
                (PubSubIndexTaskClientFactory) indexTaskClientFactory,
                mapper,
                emitter,
                monitorSchedulerConfig,
                rowIngestionMetersFactory,
                supervisorStateManagerConfig
        );
    }

    @Override
    public String toString()
    {
        return "PubSubSupervisorSpec{" +
                "dataSchema=" + getDataSchema() +
                ", tuningConfig=" + getTuningConfig() +
                ", ioConfig=" + getIoConfig() +
                ", context=" + getContext() +
                ", suspend=" + isSuspended() +
                '}';
    }
}
