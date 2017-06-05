/*
 * Copyright 2017 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jbingham.dview;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.Operation;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use the Google Genomics API for checking status of nodes in the pipeline graph.
 */
public class GooglePipelinesProvider {
  static final Logger LOG = LoggerFactory.getLogger(GooglePipelinesProvider.class);
  private final static int POLL_INTERVAL = 30;

  /**
   * Call the Google Genomics Operations API.
   */
  public static Operation getOperation(String name) 
      throws IOException, GeneralSecurityException {
    Genomics g = createGenomicsService();
    Genomics.Operations.Get req = g.operations().get(name);
    Operation o = req.execute();
    return o;
  }

  /**
   * Block until the operation is done.
   */
  public static Operation wait(String name) {
    Operation status = null;
    do {
      LOG.debug("Sleeping for " + POLL_INTERVAL + " sec");
      try {
        TimeUnit.SECONDS.sleep(POLL_INTERVAL);
      } catch (InterruptedException e) {
        // ignore
      }
      try {
        status = getOperation(status.getName());
      } catch (Exception e) {
        LOG.warn("Error checking operation status: " + e.getMessage());
      }
    } while (status.getDone() == null || !status.getDone());

    LOG.info("Done! " + status.getName());
    return status;
  }

  private static Genomics createGenomicsService() throws IOException, GeneralSecurityException {
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

    GoogleCredential credential = GoogleCredential.getApplicationDefault();
    if (credential.createScopedRequired()) {
      credential =
          credential.createScoped(Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));
    }

    return new Genomics.Builder(httpTransport, jsonFactory, credential)
        .setApplicationName("Google-GenomicsSample/0.1")
        .build();
  }
}