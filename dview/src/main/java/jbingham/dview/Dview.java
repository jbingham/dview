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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * Create a viewer for a DAG in a dsub or other Google Pipelines API pipeline.
 * <p>
 * Currently, the DAG must be explicitly defined in a YAML string containing job IDs.
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
 * dependent.
 * </p>
 */
public class Dview {
  private static final Logger LOG = LoggerFactory.getLogger(Dview.class);
  private static GooglePipelinesProvider provider;

  public interface DviewOptions extends PipelineOptions {
    @Description("DAG definition as YAML list of job IDs")
    @Required
    String getDAG();
    void setDAG(String value);
    
    // Could get/set provider here, if there were more than one.
  }

  public static void main(String[] args) {
    DviewOptions options = 
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DviewOptions.class);

    Yaml yaml = new Yaml();
    Object graph = yaml.load(options.getDAG());
    
    // Here's where we could support multiple providers
    provider = new GooglePipelinesProvider();
   
    Pipeline p = Pipeline.create(options);
    PCollection<String> input = p.apply(Create.of("Start"));
    input = createGraph(graph, input);

    p.run();
  }

  /**
   * Recursively construct the Beam pipeline.
   *
   * @param graphItem a node, edge, or branch point
   * @param input the inputs to the graph item
   * @return the root node in the (sub)graph
   */
  static PCollection<String> createGraph(Object graphItem, PCollection<String> input) {
    PCollection<String> output = input;

    // Node
    if (graphItem instanceof String) {
      LOG.info("Adding jobId: " + graphItem);

      String jobId = (String)graphItem;
      String jobName = provider.getJobName(jobId);
      output = input.apply(new WaitForJob(jobId, jobName));

    // Branch
    } else if (graphItem instanceof Map) {
      LOG.info("Adding branches");

      @SuppressWarnings("unchecked")
      Collection<?> branches = ((Map<String,Object>) graphItem).values();
      output = createBranches(branches, input);

    // Edge
    } else if (graphItem instanceof Collection<?>) {
      LOG.info("Adding steps");

      // The output of each step is input to the next
      Collection<?> steps = (Collection<?>)graphItem;
      for (Object item : steps) {
        output = createGraph(item, output);
      }
    } else {
      throw new IllegalStateException("Invalid graph item: " + graphItem);
    }
    return output;
  }

  private static PCollection<String> createBranches(Collection<?> branches, PCollection<String> input) {  
    LOG.info("Branch count: " + branches.size());

    PCollectionList<String> outputs = null;

    // For each edge, apply a transform to the input collection
    for (Object branch : branches) {
      LOG.info("Adding branch");

      PCollection<String> branchOutput = createGraph(branch, input);
      outputs = 
          outputs == null ? 
              PCollectionList.of(branchOutput) :
              outputs.and(branchOutput);
    }

    LOG.info("Merging " + outputs.size() + " branches");
    return outputs.apply(new MergeBranches());
  }

  @SuppressWarnings("serial")
  public static class WaitForJob extends PTransform<PCollection<String>, PCollection<String>> {
    private String jobId;
 
    public WaitForJob(String jobId, String name) {
      super(name);
      this.jobId = jobId;
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
      provider.getJobStatus(jobId, true);
      return input;
    } 
  }
  
  /**
   * Merge branches in the graph.
   */
  @SuppressWarnings("serial")
  public static class MergeBranches extends PTransform<PCollectionList<String>, PCollection<String>> {
    private static int numMerges = 0;

    public MergeBranches() {
      // Give the transform a friendly name in the UI.
      super("MergeBranches" + ++numMerges);
    }

    @Override
    public PCollection<String> expand(PCollectionList<String> input) {
      return input
          .apply(Flatten.<String>pCollections())
          .apply(Combine.globally(new SerializableFunction<Iterable<String>,String>() {
              public String apply(Iterable<String> input) {
                return "Merged";
              }
          }));
    }
  }
}