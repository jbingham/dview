/*
 * Copyright 2017 Verily Life Sciences.
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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * Create a viewer for a DAG in a dsub pipeline.
 * <p>
 * The DAG contains job IDs defined as a JSON string.
 * On Google Cloud, the job IDs are Google Genomics Operation IDs.
 * E.g.:
 * <pre>
 * - jobId1
 * - jobId2
 * - jobId3
 * </pre>
 * The special word "BRANCH" can be used to add branches to the graph:
 * <pre>
 * - jobId1
 * - BRANCH:
 *   - - jobId2
 *     - jobId3
 *   - jobId4
 * - jobId5
 * </pre>
 * </p>
 */
public class Dview {

  public interface DviewOptions extends PipelineOptions {
   @Description("DAG definition as YAML list of job IDs")
    @Required
    String getDAG();

    void setDAG(String value);
  }

  public static void main(String[] args) {
    System.out.println("Hello World!");
    DviewOptions options = 
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DviewOptions.class);
    Pipeline p = DAG.createPipeline(options);
    p.run();
  }
}