package org.apache.druid.indexing.pubsub;

import com.google.auth.oauth2.GoogleCredentials;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

/**
 * Retrieves access token from the GOOGLE_APPLICATION_CREDENTIALS environment variable.
 */
public class CredentialsProvider {
    private static final Logger log = new Logger(CredentialsProvider.class);

    private GoogleCredentials credentials;

    CredentialsProvider() {
        String credentialsFile = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
        File initialFile = new File(credentialsFile);

        try {
            InputStream targetStream = new FileInputStream(initialFile);
            credentials = GoogleCredentials.fromStream(targetStream).createScoped(
                    Collections.singletonList("https://www.googleapis.com/auth/monitoring")
            );
        } catch (Exception e) {
            log.error("Cannot initialize Google credentials: " + e);
        }
    }

    public String getAccessToken() throws IOException {
        return credentials.refreshAccessToken().getTokenValue();
    }
}
