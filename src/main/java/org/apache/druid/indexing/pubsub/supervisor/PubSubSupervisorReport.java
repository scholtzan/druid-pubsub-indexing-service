package org.apache.druid.indexing.pubsub.supervisor;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.overlord.supervisor.SupervisorReport;
import org.joda.time.DateTime;

public class PubSubSupervisorReport extends SupervisorReport {
    public PubSubSupervisorReport(String dataSource,
                                  DateTime generationTime,
                                  boolean suspended) {
        super(dataSource,
            generationTime,
            ImmutableMap.builder().put("suspended", suspended));
    }
}
