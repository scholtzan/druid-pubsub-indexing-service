package org.apache.druid.indexing.pubsub;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.jcodings.util.Hash;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class PubSubMessageSupplier {
    private final String projectId;
    private final String subscriptionId;
    private final int maxMessageSizePerPoll;
    private final int maxMessagesPerPoll;
    private final Duration keepAliveTime;
    private final Duration keepAliveTimeout;
    private final String subscription;
    private final CredentialsProvider credentials;

    private boolean closed;

    private static final Logger log = new Logger(PubSubMessageSupplier.class);

    public PubSubMessageSupplier(
            String projectId,
            String subscriptionId,
            int maxMessagesPerPoll,
            int maxMessageSizePerPoll,
            Duration keepAliveTime,
            Duration keepAliveTimeout) {
        log.info("Init PubSubMessageSupplier");
        this.projectId = projectId;
        this.subscriptionId = subscriptionId;
        this.maxMessageSizePerPoll = maxMessageSizePerPoll;
        this.maxMessagesPerPoll = maxMessagesPerPoll;
        this.keepAliveTime = keepAliveTime;
        this.keepAliveTimeout = keepAliveTimeout;
        this.subscription = "projects/" + projectId  + "/subscriptions/" + subscriptionId;
        this.credentials = new CredentialsProvider();
    }

    public void close() {
        if (closed) {
            return;
        }
        closed = true;
    }

    public List<PubSubReceivedMessage> poll() {
        checkIfClosed();

        log.info("poll");

        HttpClient httpclient = HttpClients.createDefault();
        String url = "https://pubsub.googleapis.com/v1/" + subscription + ":pull";
        HttpPost request = new HttpPost(url);
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        List<PubSubReceivedMessage> messages = new ArrayList<>();

        try {
            request.addHeader("Authorization", "Bearer " + credentials.getAccessToken());
            try {
                log.info("Pull pub sub messages");
                // create event JSON payload
                Map<String, Object> payload = new HashMap<String, Object>();
                payload.put("returnImmediately", true);
                payload.put("maxMessages", maxMessagesPerPoll);

                String jsonPayload = ow.writeValueAsString(payload);
                request.setEntity(new StringEntity(jsonPayload));
                request.addHeader("Content-type", "application/json");

                try {
                    // get pulled messages
                    HttpResponse response = httpclient.execute(request);
                    ObjectMapper om = new ObjectMapper();
                    ReceivedMessages receivedMessages = om.readValue(response.getEntity().getContent(), ReceivedMessages.class);
                    messages = receivedMessages.getReceivedPubSubMessages();

                    log.info("Received messages: " + messages);

                    String ackUrl = "https://pubsub.googleapis.com/v1/" + subscription + ":acknowledge";
                    HttpPost ackRequest = new HttpPost(ackUrl);
                    Map<String, Object> ackPayload = new HashMap<String, Object>();

                    List<String> ackIds = new ArrayList<>();
                    for (PubSubReceivedMessage message: messages) {
                        ackIds.add(message.getAckId());
                    }

                    log.info("Ack messages");
                    ackPayload.put("ackId", ackIds);

                    ackRequest.setEntity(new StringEntity(ow.writeValueAsString(ackPayload)));
                    ackRequest.addHeader("Content-type", "application/json");

                    try {
                        httpclient.execute(request);
                    } catch (Exception e) {
                        log.error("Error acknowledging messages: " + e);
                    }
                } catch (IOException ex) {
                    log.error(ex, "Failed to get events from Pub/Sub.");
                } finally {
                    request.releaseConnection();
                }
            } catch (Exception e) {
                log.error("Could not serialize payload: " + e.toString());
            }
        } catch (Exception e) {
            log.error("Error getting access token: " + e.toString());
        }

        return messages;
    }

    private void checkIfClosed() {
        if (closed) {
            throw new ISE("Invalid operation - PubSubMessageSupplier has already been closed");
        }
    }

    private class ReceivedMessages {
        @JsonProperty("receivedMessages") private final List<ReceivedMessage> receivedMessages;

        public ReceivedMessages(List<ReceivedMessage> receivedMessages) {
            this.receivedMessages = receivedMessages;
        }

        public List<PubSubReceivedMessage> getReceivedPubSubMessages() {
            List<PubSubReceivedMessage> messages = new ArrayList<>();

            for (ReceivedMessage message: receivedMessages) {
                messages.add(message.getMessage());
            }

            return messages;
        }
    }

    private class ReceivedMessage {
        @JsonProperty("ackId") private final String ackId;
        @JsonProperty("message") private final PubSubReceivedMessage message;

        public ReceivedMessage(String ackId, PubSubReceivedMessage message) {
            this.ackId = ackId;
            this.message = message;
            message.setAckId(ackId);
        }

        public PubSubReceivedMessage getMessage() {
            if (message.getAckId() == null) {
                message.setAckId(ackId);
            }

            return message;
        }
    }
}
