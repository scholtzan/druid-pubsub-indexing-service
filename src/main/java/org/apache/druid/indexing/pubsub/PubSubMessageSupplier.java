package org.apache.druid.indexing.pubsub;

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.*;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.threeten.bp.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class PubSubMessageSupplier {
    private final SubscriberStub subscriber;
    private final String projectId;
    private final String subscriptionId;
    private final int maxMessageSizePerPoll;
    private final int maxMessagesPerPoll;
    private final Duration keepAliveTime;
    private final Duration keepAliveTimeout;

    private boolean closed;

    private static final Logger log = new Logger(PubSubMessageSupplier.class);

    public PubSubMessageSupplier(
            String projectId,
            String subscriptionId,
            int maxMessagesPerPoll,
            int maxMessageSizePerPoll,
            Duration keepAliveTime,
            Duration keepAliveTimeout) {
        this.projectId = projectId;
        this.subscriptionId = subscriptionId;
        this.subscriber = getPubSubSubscriber();
        this.maxMessageSizePerPoll = maxMessageSizePerPoll;
        this.maxMessagesPerPoll = maxMessagesPerPoll;
        this.keepAliveTime = keepAliveTime;
        this.keepAliveTimeout = keepAliveTimeout;
    }

    public void close() {
        if (closed) {
            return;
        }
        closed = true;
    }

    private SubscriberStub getPubSubSubscriber() {
        try {
            SubscriberStubSettings subscriberStubSettings =
                    SubscriberStubSettings.newBuilder()
                            .setTransportChannelProvider(
                                    SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                                            .setMaxInboundMessageSize(maxMessageSizePerPoll)
                                            .setKeepAliveTimeout(keepAliveTimeout)
                                            .setKeepAliveTime(keepAliveTime)
                                            .build())
                            .build();

            return GrpcSubscriberStub.create(subscriberStubSettings);
        } catch (Exception e) {
            log.error("Could not initialize PubSub subscriber stub.");
            return null;
        }
    }

    public List<PubSubReceivedMessage> poll() {
        checkIfClosed();

        String subscriptionName = ProjectSubscriptionName.format(this.projectId, this.subscriptionId);

        PullRequest pullRequest =
                PullRequest.newBuilder()
                        .setMaxMessages(maxMessagesPerPoll)
                        .setReturnImmediately(false)
                        .setSubscription(subscriptionName)
                        .build();


        PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);

        List<String> ackIds = new ArrayList<>();
        List<PubSubReceivedMessage> messages = new ArrayList<>();

        for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
            messages.add(new PubSubReceivedMessage(message.getMessage().getData(), message.getMessage().getAttributesMap()));
            ackIds.add(message.getAckId());
        }

        // acknowledge received messages
        AcknowledgeRequest acknowledgeRequest =
                AcknowledgeRequest.newBuilder()
                        .setSubscription(subscriptionName)
                        .addAllAckIds(ackIds)
                        .build();

        subscriber.acknowledgeCallable().call(acknowledgeRequest);

        return messages;
    }

    private void checkIfClosed() {
        if (closed) {
            throw new ISE("Invalid operation - PubSubMessageSupplier has already been closed");
        }
    }

    private static <T> T wrapExceptions(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw new StreamException(e);
        }
    }

    private static void wrapExceptions(Runnable runnable) {
        wrapExceptions(() -> {
            runnable.run();
            return null;
        });
    }
}
