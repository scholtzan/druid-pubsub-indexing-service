package org.apache.druid.indexing.pubsub;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.*;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.threeten.bp.Duration;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingDeque;

public class PubSubMessageSupplier {
    private final Subscriber subscriber;
    private final String projectId;
    private final String subscriptionId;
    private final int maxMessageSizePerPoll;
    private final int maxMessagesPerPoll;
    private final Duration keepAliveTime;
    private final Duration keepAliveTimeout;
    private final long maxOutstandingElements;
    private final long maxOutstandingRequestBytes;

    private boolean closed;
    private BlockingQueue<PubSubReceivedMessage> messages;

    private static final Logger log = new Logger(PubSubMessageSupplier.class);

    public PubSubMessageSupplier(
            String projectId,
            String subscriptionId,
            int maxMessagesPerPoll,
            int maxMessageSizePerPoll,
            Duration keepAliveTime,
            Duration keepAliveTimeout,
            int maxQueueSize,
            long maxOutstandingElements,
            long maxOutstandingRequestBytes) {
        log.info("Init PubSubMessageSupplier");
        this.projectId = projectId;
        this.subscriptionId = subscriptionId;
        this.subscriber = getPubSubSubscriber();
        this.maxMessageSizePerPoll = maxMessageSizePerPoll;
        this.maxMessagesPerPoll = maxMessagesPerPoll;
        this.keepAliveTime = keepAliveTime;
        this.keepAliveTimeout = keepAliveTimeout;
        this.messages = new LinkedBlockingDeque<>(maxQueueSize);
        this.maxOutstandingRequestBytes = maxOutstandingRequestBytes;
        this.maxOutstandingElements = maxOutstandingElements;
    }

    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        if (subscriber != null) {
            subscriber.stopAsync();
        }
    }

    private Subscriber getPubSubSubscriber() {
        log.info("Init SubscriberStub");
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);

        MessageReceiver receiver =
            new MessageReceiver() {
                @Override
                public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                    if (!messages.offer(
                            new PubSubReceivedMessage(
                                    message.getData(),
                                    message.getAttributesMap(),
                                    consumer
                            )
                    )) {
                        log.info("Pubsub queue full");
                    }
                }
            };

        FlowControlSettings flowControlSettings =
                FlowControlSettings.newBuilder()
                        .setMaxOutstandingElementCount(maxOutstandingElements)
                        .setMaxOutstandingRequestBytes(maxOutstandingRequestBytes)
                        .build();

        Subscriber subscriber = null;
        try {
            subscriber = Subscriber.newBuilder(subscriptionName, receiver)
                    .setFlowControlSettings(flowControlSettings)
                    .build();
            subscriber.startAsync();
        } catch (Exception e) {
            log.error("Error starting Pub/Sub message receiver: " + e.toString());
        }

        return subscriber;
    }

    public List<PubSubReceivedMessage> poll() {
        checkIfClosed();

        log.info("poll");

        List<PubSubReceivedMessage> polledMessages = new LinkedList<>();

        for (int i = 0; i < maxMessagesPerPoll; i++) {
            if (messages.isEmpty()) {
                break;
            } else {
                PubSubReceivedMessage m = messages.poll();
                m.ack();
                polledMessages.add(m);
            }
        }

        return polledMessages;
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
