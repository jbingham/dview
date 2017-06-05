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
import com.google.api.services.genomics.Genomics.Pipelines.Run;
import com.google.api.services.genomics.model.Operation;
import com.google.api.services.genomics.model.Pipeline;
import com.google.api.services.genomics.model.RunPipelineArgs;
import com.google.api.services.genomics.model.RunPipelineRequest;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use the Google Genomics Pipelines API for checking status of jobs.
 */
public class GooglePipelinesProvider {
  static final Logger LOG = LoggerFactory.getLogger(GooglePipelinesProvider.class);
  private final static int POLL_INTERVAL = 30;

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

  private Operation getJobStatus(String jobId) throws IOException{
    Operation status;
    try {
      Genomics service = createGenomicsService();
      Genomics.Operations.Get request = service.operations().get(jobId);
      status = request.execute();
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    }
    return status;
  }

  /**
   * @throws RuntimeException if the job failed
   */
  public Operation getJobStatus(String jobId, boolean block) {
    LOG.info("Checking job status for job " + jobId);
    Operation status = null;

    if (!block) {
      try {
        status = getJobStatus(jobId);
      } catch (IOException e) {
        LOG.error("Failed to get job status");
        throw new RuntimeException(e);
      }
    } else {
      do {
        try {
          TimeUnit.SECONDS.sleep(POLL_INTERVAL);
        } catch (InterruptedException e) {
          // ignore
        }
  
        try {
          status = getJobStatus(status.getName());
        } catch (IOException e) {
          LOG.warn("Error getting operation status. Retrying in " + POLL_INTERVAL + " sec");
          LOG.warn(e.toString());
        }
      } while (status.getDone() == null || !status.getDone());

      if (status.getError() != null) {
        LOG.warn("Job failed! " + status.getName());
        throw new RuntimeException("Job failed: " + status.getError());
      }

      LOG.info("Job succeeded! " + status.getName());
    }
    return status;
  }

  public String getJobName(String jobId) {
    Operation operation;
    try {
      operation = getJobStatus(jobId);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get operation " + jobId, e);
    }
    Map<?,?> request = (Map<?,?>)operation.getMetadata().get("request");
    Map<?,?> pipeline = (Map<?,?>)request.get("ephemeralPipeline");
    String jobName = (String)pipeline.get("name");
    return jobName;
  }
  
  public Operation submitJob(Pipeline pipeline, RunPipelineArgs args)
      throws IOException, GeneralSecurityException {
    RunPipelineRequest request = new RunPipelineRequest();
    request.setEphemeralPipeline(pipeline);
    request.setPipelineArgs(args);

    Genomics g = createGenomicsService();
    Run run = g.pipelines().run(request);
    Operation op = run.execute();
    return op;    
  }
}