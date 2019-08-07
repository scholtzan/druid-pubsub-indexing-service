package org.apache.druid.indexing.pubsub.supervisor;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.pubsub.PubSubIndexTaskClientFactory;
import org.apache.druid.indexing.pubsub.PubSubIndexTaskIOConfig;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;


public class PubSubSupervisorSpec implements SupervisorSpec {
    protected final TaskStorage taskStorage;
    protected final TaskMaster taskMaster;
    protected final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
    protected final PubSubIndexTaskClientFactory indexTaskClientFactory;
    protected final ObjectMapper mapper;
    protected final RowIngestionMetersFactory rowIngestionMetersFactory;
    private final DataSchema dataSchema;
    private final PubSubSupervisorTuningConfig tuningConfig;
    private final PubSubSupervisorIOConfig ioConfig;
    @Nullable
    private final Map<String, Object> context;
    protected final ServiceEmitter emitter;
    protected final DruidMonitorSchedulerConfig monitorSchedulerConfig;
    private final boolean suspended;
    private final PubSubIndexTaskIOConfig taskIOConfig;

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
            @JacksonInject PubSubIndexTaskIOConfig taskIOConfig
            ) {
        this.dataSchema = Preconditions.checkNotNull(dataSchema, "dataSchema");
        this.tuningConfig = tuningConfig; // null check done in concrete class
        this.ioConfig = Preconditions.checkNotNull(ioConfig, "ioConfig");
        this.context = context;

        this.taskStorage = taskStorage;
        this.taskMaster = taskMaster;
        this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
        this.indexTaskClientFactory = pubSubIndexTaskClientFactory;
        this.mapper = mapper;
        this.emitter = emitter;
        this.monitorSchedulerConfig = monitorSchedulerConfig;
        this.rowIngestionMetersFactory = rowIngestionMetersFactory;
        this.suspended = suspended != null ? suspended : false;
        this.taskIOConfig = taskIOConfig;
    }

    @Override
    public Supervisor createSupervisor() {
        return new PubSubSupervisor(
                getId(),
                taskStorage,
                taskMaster,
                indexerMetadataStorageCoordinator,
                indexTaskClientFactory,
                mapper,
                this,
                rowIngestionMetersFactory,
                taskIOConfig
                );
    }

    @JsonProperty
    public PubSubSupervisorTuningConfig getTuningConfig() {
        return tuningConfig;
    }

    @JsonProperty
    public PubSubSupervisorIOConfig getIoConfig() {
        return ioConfig;
    }

    @Override
    public String toString() {
        return "PubSubSupervisorSpec{" +
                "dataSchema=" + getDataSchema() +
                ", tuningConfig=" + getTuningConfig() +
                ", ioConfig=" + getIoConfig() +
                ", context=" + getContext() +
                ", suspend=" + isSuspended() +
                '}';
    }

    @Override
    public boolean isSuspended() {
        return suspended;
    }

    public DataSchema getDataSchema() {
        return dataSchema;
    }

    @Nullable
    public Map<String, Object> getContext() {
        return context;
    }

    @Override
    public String getId() {
        return dataSchema.getDataSource();
    }

    @Override
    public List<String> getDataSources() {
        return ImmutableList.of(getDataSchema().getDataSource());
    }
}
