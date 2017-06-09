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

import java.util.Collection;
import java.util.Map;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * Create a viewer for a DAG in a dsub or other pipeline. A job provider
 * for the Google Genomics Pipelines API is used to check for job status.
 * <p>
 * The DAG must be explicitly defined in a YAML string containing job IDs.
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
 * <p>
 * In the future, it ought to be possible to lookup the whole DAG from the first job ID.
 * This would require that later operations store the job IDs of the jobs they're
 * dependent on.
 * </p>
 */
public class Dview {
  static final Logger LOG = LoggerFactory.getLogger(Dview.class);
  private static GooglePipelinesProvider provider = new GooglePipelinesProvider();

  public interface DviewOptions extends PipelineOptions {
    @Description("DAG definition as a YAML list of job IDs and branches")
    @Required
    String getDag();
    void setDag(String value);
  }

  @SuppressWarnings("serial")
  public static void main(String[] args) {
    PipelineOptionsFactory.register(DviewOptions.class);
    DviewOptions options = 
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DviewOptions.class);

    Yaml yaml = new Yaml();
    Object graph = yaml.load(options.getDag());
    LOG.info("Graph: \n" + yaml.dump(graph));

    DataflowPipelineOptions dpo = options.as(DataflowPipelineOptions.class);
    Pipeline p = Pipeline.create(dpo);

    PCollection<String> input = p.begin()
        .apply("StartPipeline", new PTransform<PBegin,PCollection<String>>() {
          @Override
          public PCollection<String> expand(PBegin begin) {
            return begin.apply(Create.of("pipeline"));
          }
        });
    createGraph(graph, input);

    p.run().waitUntilFinish();
    LOG.info("Dview job completed");
  }

  /**
   * Recursively construct the Beam pipeline graph.
   *
   * @param graphItem a node, edge, or branch point
   * @param input the inputs to the graph item
   * @return the root node in the (sub)graph
   */
  static PCollection<String> createGraph(Object graphItem, PCollection<String> input) {
    PCollection<String> output = null;

    // A single task
    if (graphItem instanceof String) {
      LOG.info("Adding job: " + graphItem);

      String jobId = (String)graphItem;
      String jobName = provider.getJobName(jobId);
      LOG.info("Job name: " + jobName);

      output = input.apply(jobName, ParDo.of(new WaitForJob(jobId)));

    // A list of tasks
    } else if (graphItem instanceof Collection<?>) {
      output = input;

      // The output of each task is input to the next
      Collection<?> tasks = (Collection<?>)graphItem;
      for (Object task : tasks) {
        output = createGraph(task, output);
      }

    // A branch in the graph
    } else if (graphItem instanceof Map<?,?>) {
      LOG.info("Adding branches");

      Collection<?> branches =
          (Collection<?>)((Map<?,?>) graphItem).values().iterator().next();
      output = createBranches(branches, input);

    } else {
      throw new IllegalStateException("Invalid graph item: " + graphItem);
    }
    return output;
  }

  private static PCollection<String> createBranches(Collection<?> branches, PCollection<String> input) {  
    LOG.info("Branch count: " + branches.size());
    PCollectionList<String> list = null;

    for (Object branch : branches) {
      LOG.info("Adding branch");

      // Recursively create a graph for each branch
      PCollection<String> branchOutput = createGraph(branch, input);
      list = list == null ? PCollectionList.of(branchOutput) : list.and(branchOutput);
    }
    
    LOG.info("Merging " + list.size() + " branches");
    return list.apply("MergeBranches", new MergeBranches());
  }
  
  @SuppressWarnings("serial")
  public static class MergeBranches extends PTransform<PCollectionList<String>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollectionList<String> input) {
       return input
           .apply(Flatten.<String>pCollections())
           .apply(Combine.globally(new SerializableFunction<Iterable<String>,String>() {
               public String apply(Iterable<String> input) {
                 // the Dataflow UI messes up the graph if this value is null
                 String output = "merge";
  
                 for (String s : input) {
                   LOG.info("Merge: " + s);
                   output = output == null ? s : output + "-" + s;
                 }
                 LOG.info("Merge output: " + output);
                 return output;
               }
           }));    
    } 
  }

  @SuppressWarnings("serial")
  public static class WaitForJob extends DoFn<String, String> {
    private String jobId;
    
    public WaitForJob(String jobId) {
      this.jobId = jobId;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      LOG.info("Wait for job: " + jobId);
      LOG.info("Input: " + c.element());
      provider.getJobStatus(jobId);
      c.output(jobId);
    } 
  }
}