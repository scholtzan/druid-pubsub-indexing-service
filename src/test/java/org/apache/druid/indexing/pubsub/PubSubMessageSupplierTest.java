package org.apache.druid.indexing.pubsub;

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import junit.framework.Assert;
import org.junit.jupiter.api.Test;
import org.threeten.bp.Duration;

import java.io.IOException;

public class PubSubMessageSupplierTest {
    @Test
    public void initSubscriberStub() throws IOException {
        SubscriberStubSettings subscriberStubSettings =
                SubscriberStubSettings.newBuilder()
                        .setTransportChannelProvider(
                                SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                                        .setMaxInboundMessageSize(20000)
                                        .setKeepAliveTimeout(Duration.ofHours(5))
                                        .setKeepAliveTime(Duration.ofHours(5))
                                        .build())
                        .build();

        SubscriberStub s = GrpcSubscriberStub.create(subscriberStubSettings);
        System.out.println(s);
        Assert.assertNotSame(GrpcSubscriberStub.create(subscriberStubSettings), null);
    }
}
