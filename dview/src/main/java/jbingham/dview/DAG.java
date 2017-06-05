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

import java.util.Collection;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
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

import jbingham.dview.Dview.DviewOptions;

public class DAG {
  static final Logger LOG = LoggerFactory.getLogger(DAG.class);
  private Pipeline pipeline;

  /**
   * Create a Beam pipeline from a DAG.
   */
  static public Pipeline createPipeline(DviewOptions opt) {
    Yaml yaml = new Yaml();
    Object graph = yaml.load(opt.getDAG());
   
    DAG dag = new DAG();
    dag.pipeline = Pipeline.create(opt);
    PCollection<String> input = dag.pipeline.apply(Create.of("Start"));
    input = graph(graph, input);

    return dag.pipeline;
  }

  /**
   * Recursively construct the Beam pipeline.
   * @param graphItem a node, edge, or branch point
   * @param input the inputs to the graph item
   * @return the root node in the (sub)graph
   */
  private static PCollection<String> graph(Object graphItem, PCollection<String> input) {
    PCollection<String> output = input;
    

    // Node
    if (graphItem instanceof String) {
      LOG.info("Adding jobId: " + graphItem);

      String jobId = (String)graphItem;
      output = input.apply(waitForJob(jobId));

    // Branch
    } else if (graphItem instanceof Map) {
      LOG.info("Adding branches");

      @SuppressWarnings("unchecked")
      Collection<?> branches = ((Map<String,Object>) graphItem).values();
      output = branches(branches, input);

    // Edge
    } else if (graphItem instanceof Collection<?>) {
      LOG.info("Adding steps");

      // The output of each step is input to the next
      Collection<?> steps = (Collection<?>)graphItem;
      for (Object item : steps) {
        output = graph(item, output);
      }
    } else {
      throw new IllegalStateException("Invalid graph item: " + graphItem);
    }
    
    return output;
  }

  private static PCollection<String> branches(Collection<?> branches, PCollection<String> input) {  
    LOG.info("Branch count: " + branches.size());

    PCollectionList<String> outputs = null;

    // For each edge, apply a transform to the input collection
    for (Object branch : branches) {
      LOG.info("Adding branch");

      PCollection<String> branchOutput = graph(branch, input);
      outputs = 
          outputs == null ? 
              PCollectionList.of(branchOutput) :
              outputs.and(branchOutput);
    }

    LOG.info("Merging " + outputs.size() + " branches");
    return outputs.apply(new MergeBranches());
  }

  private static PTransform<PCollection<String>, PCollection<String>> waitForJob(String jobId) {
    
    return null;
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
