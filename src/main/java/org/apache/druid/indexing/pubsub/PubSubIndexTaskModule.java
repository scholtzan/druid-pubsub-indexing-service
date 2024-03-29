package org.apache.druid.indexing.pubsub;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.druid.indexing.pubsub.supervisor.PubSubSupervisorSpec;
import org.apache.druid.indexing.pubsub.supervisor.PubSubSupervisorTuningConfig;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.emitter.EmittingLogger;

import java.util.List;


public class PubSubIndexTaskModule implements DruidModule {
    private static final EmittingLogger log = new EmittingLogger(PubSubIndexTaskModule.class);

    @Override
    public List<? extends Module> getJacksonModules() {
        log.info("Pub/Sub register modules");

        return ImmutableList.of(
                new SimpleModule(getClass().getSimpleName())
                        .registerSubtypes(
                                new NamedType(PubSubIndexTask.class, "index_pubsub"),
                                new NamedType(PubSubDataSourceMetadata.class, "pubsub"),
                                new NamedType(PubSubIndexTaskIOConfig.class, "pubsub"),
                                new NamedType(PubSubIndexTaskTuningConfig.class, "PubSubTuningConfig"),
                                new NamedType(PubSubSupervisorTuningConfig.class, "pubsub"),
                                new NamedType(PubSubSupervisorSpec.class, "pubsub"))
        );
    }

    @Override
    public void configure(Binder binder) {
        log.info("Bind Pub/Sub indexing service");

//        binder.bind(
//                new TypeLiteral<PubSubIndexTaskClientFactory>() {
//                }
//        ).to(PubSubIndexTaskClientFactory.class).in(LazySingleton.class);
    }
}
