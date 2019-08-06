package org.apache.druid.indexing.pubsub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.indexing.common.IndexTaskClient;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.response.FullResponseHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.IOException;

public class PubSubIndexTaskClient extends IndexTaskClient {
    private static final EmittingLogger log = new EmittingLogger(PubSubIndexTaskClient.class);

    PubSubIndexTaskClient(
            HttpClient httpClient,
            ObjectMapper objectMapper,
            TaskInfoProvider taskInfoProvider,
            Duration httpTimeout,
            String callerId,
            int numThreads,
            long numRetries
    ) {
        super(
                httpClient,
                objectMapper,
                taskInfoProvider,
                httpTimeout,
                callerId,
                numThreads,
                numRetries
        );
    }

    public boolean stop(final String id, final boolean publish) {
        log.debug("Stop task[%s] publish[%s]", id, publish);

        try {
            final FullResponseHolder response = submitRequestWithEmptyContent(
                    id,
                    HttpMethod.POST,
                    "stop",
                    publish ? "publish=true" : null,
                    true
            );
            return isSuccess(response);
        } catch (NoTaskLocationException e) {
            return false;
        } catch (TaskNotRunnableException e) {
            log.info("Task [%s] couldn't be stopped because it is no longer running", id);
            return true;
        } catch (Exception e) {
            log.warn(e, "Exception while stopping task [%s]", id);
            return false;
        }
    }

    public boolean resume(final String id) {
        log.debug("Resume task[%s]", id);

        try {
            final FullResponseHolder response = submitRequestWithEmptyContent(id, HttpMethod.POST, "resume", null, true);
            return isSuccess(response);
        } catch (NoTaskLocationException | IOException e) {
            log.warn(e, "Exception while stopping task [%s]", id);
            return false;
        }
    }

    // todo: timestamp necessary?
    // pauses task and returns time from when to resume
    public String pause(final String id) {
        log.debug("Pause task[%s]", id);

        try {
            final FullResponseHolder response = submitRequestWithEmptyContent(
                    id,
                    HttpMethod.POST,
                    "pause",
                    null,
                    true
            );

            final HttpResponseStatus responseStatus = response.getStatus();
            final String responseContent = response.getContent();

            if (responseStatus.equals(HttpResponseStatus.OK)) {
                log.info("Task [%s] paused successfully", id);
                return deserialize(responseContent, String.class);
            } else if (responseStatus.equals(HttpResponseStatus.ACCEPTED)) {
                // The task received the pause request, but its status hasn't been changed yet.
                while (true) {
                    final PubSubIndexTaskRunner.Status status = getStatus(id);
                    if (status == PubSubIndexTaskRunner.Status.PAUSED) {
                        return getCurrentTimestamp(id, true);
                    }

                    final Duration delay = newRetryPolicy().getAndIncrementRetryDelay();
                    if (delay == null) {
                        throw new ISE(
                                "Task [%s] failed to change its status from [%s] to [%s], aborting",
                                id,
                                status,
                                PubSubIndexTaskRunner.Status.PAUSED
                        );
                    } else {
                        final long sleepTime = delay.getMillis();
                        log.info(
                                "Still waiting for task [%s] to change its status to [%s]; will try again in [%s]",
                                id,
                                PubSubIndexTaskRunner.Status.PAUSED,
                                new Duration(sleepTime).toString()
                        );
                        Thread.sleep(sleepTime);
                    }
                }
            } else {
                throw new ISE(
                        "Pause request for task [%s] failed with response [%s] : [%s]",
                        id,
                        responseStatus,
                        responseContent
                );
            }
        } catch (NoTaskLocationException e) {
            log.error("Exception [%s] while pausing Task [%s]", e.getMessage(), id);
            return "";
        } catch (IOException | InterruptedException e) {
            throw new RE(e, "Exception [%s] while pausing Task [%s]", e.getMessage(), id);
        }
    }

    public PubSubIndexTaskRunner.Status getStatus(final String id) {
        log.debug("GetStatus task[%s]", id);

        try {
            final FullResponseHolder response = submitRequestWithEmptyContent(id, HttpMethod.GET, "status", null, true);
            return deserialize(response.getContent(), PubSubIndexTaskRunner.Status.class);
        } catch (NoTaskLocationException e) {
            return PubSubIndexTaskRunner.Status.NOT_STARTED;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getCurrentTimestamp(final String id, final boolean retry) {
        log.debug("GetCurrentTimestamp task[%s] retry[%s]", id, retry);

        try {
            final FullResponseHolder response = submitRequestWithEmptyContent(
                    id,
                    HttpMethod.GET,
                    "time/current",
                    null,
                    retry
            );
            return deserialize(response.getContent(), String.class);
        } catch (NoTaskLocationException e) {
            return "";
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Nullable
    public DateTime getStartTime(final String id) {
        log.debug("GetStartTime task[%s]", id);

        try {
            final FullResponseHolder response = submitRequestWithEmptyContent(id, HttpMethod.GET, "time/start", null, true);
            return response.getContent() == null || response.getContent().isEmpty()
                    ? null
                    : deserialize(response.getContent(), DateTime.class);
        } catch (NoTaskLocationException e) {
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ListenableFuture<Boolean> stopAsync(final String id, final boolean publish) {
        return doAsync(() -> stop(id, publish));
    }


    public ListenableFuture<Boolean> resumeAsync(final String id) {
        return doAsync(() -> resume(id));
    }

    public ListenableFuture<DateTime> getStartTimeAsync(final String id) {
        return doAsync(() -> getStartTime(id));
    }


    public ListenableFuture<String> pauseAsync(final String id) {
        return doAsync(() -> pause(id));
    }

    public ListenableFuture<PubSubIndexTaskRunner.Status> getStatusAsync(final String id) {
        return doAsync(() -> getStatus(id));
    }
}
