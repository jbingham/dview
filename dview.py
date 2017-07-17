# Copyright 2017 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Create a viewer for dsub pipelines using Apache Beam.

dview can be used as a shell command or a python library.

By running the dview command at the top of a script that executes
multiple dsub commands, a live-updating execution graph can be
displayed in the Google Dataflow UI using Beam's --runner option.

For example usage see the README and dview_example.sh.
"""
from __future__ import absolute_import

import os
import sys
import argparse
import yaml
import getpass
import time
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import logging
from dsub.providers import provider_base

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProviderOptions(object):
  def __init__(self, **kwargs):
    self.provider = 'local'
    self.project = 'your-project-id'
    self.__dict__.update(kwargs)

class BeamOptions(object):
  def __init__(self, **kwargs):
    now = datetime.now().strftime('%m%d%H%M%S')
    self.name = ('dview-%s-%s' % (getpass.getuser(), now))
    self.setup_file = (os.path.dirname(os.path.realpath(sys.argv[0])) + '/setup.py')
    self.runner = 'direct'
    self.max_num_workers = '1'
    self.__dict__.update(kwargs)

  def __iter__(self):
    return iter(self.__dict__.keys())

def parse_args(argv):
  """Parse command-line arguments"""

  provider_defaults = ProviderOptions()
  beam_defaults = BeamOptions()

  parser = argparse.ArgumentParser(
      prog='dview',
      description='Create a viewer for dsub pipelines using Apache Beam.')

  parser.add_argument(
      '--dag',
      required=True,
      help='YAML graph of job names.')

  dstat = parser.add_argument_group(
      title='dsub/dstat',
      description='Options for dsub/dstat.')
  dstat.add_argument(
      '--provider',
      choices=['google', 'local', 'test-fails'],
      default=provider_defaults.provider,
      help='Service provider for batch jobs.')
  dstat.add_argument(
      '--dry-run',
      default=False,
      action='store_true',
      help='For testing, construct the workflow graph without calling any provider methods.')

  beam = parser.add_argument_group(
      title='Beam',
      description='Options for Apache Beam.')
  beam.add_argument(
      '--name',
      default=beam_defaults.name,
      help='Display name for the job.')
  beam.add_argument(
      '--runner',
      choices=['direct', 'dataflow'],
      default=beam_defaults.runner,
      help='Apache Beam runner.')

  dataflow = parser.add_argument_group(
      title='Google',
      description='Options for the Google provider or Dataflow runner.\
          The --project is required for both; all others apply to Dataflow only.')
  dataflow.add_argument(
      '--project',
      default=provider_defaults.project,
      help='Google Cloud project ID, required for Google provider or Dataflow.')
  dataflow.add_argument(
      '--temp-location',
      default='your-temp-path',
      help='Storage path for temp files.')
  dataflow.add_argument(
      '--setup-file',
      default=beam_defaults.setup_file,
      help='Path to setup.py in the dsub root directory, if not installed as a package.')
  dataflow.add_argument(
      '--extra-package',
      default='abs-path',
      help='Absolute path to the dsub package, if installed as a package.')
  dataflow.add_argument(
      '--max-num-workers',
      default=beam_defaults.max_num_workers,
      help='Maximum number of worker VMs.')

  known_args, pipeline_args = parser.parse_known_args(argv)

  # Dataflow pipeline args
  pipeline_args.extend([
      '--runner=' + known_args.runner,
      '--project=' + known_args.project,
      '--staging_location=' + known_args.temp_location,
      '--temp_location=' + known_args.temp_location,
      '--max_num_workers=' + known_args.max_num_workers,
      '--job_name=' + known_args.name
  ])

  return known_args, pipeline_args

class MergeBranches(beam.PTransform):
  """Merge branches in the DAG"""

  def expand(self, pcoll):
    return (pcoll
        | 'Flatten' >> beam.Flatten()
        | 'Combine.globally' >> beam.CombineGlobally(lambda x: 'merged'))

class WaitForJob(beam.PTransform):
  """Block until a job completes so that the Beam graph shows the currently
  executing PTransform
  """
  logging.basicConfig(level=logging.INFO)
  logger = logging.getLogger(__name__)

  def __init__(self, provider_options, job_name):
    super(beam.PTransform, self).__init__()
    self.provider_options = provider_options
    self.job_name = job_name
    self.poll_interval = 30

  # Block until job completes and raise an exception if it failed
  def wait_for_job(self, value):
    """Get job status from the job queue provider."""
    logger.info('Waiting for job %s' % self.job_name)

    if self.provider_options.dry_run == True:
      logger.info('Dry run: continuing')
    else:
      # Wait for dsub to start the next task
      time.sleep(self.poll_interval)

      logger.info('Checking job status...')
      provider = provider_base.get_provider(self.provider_options)

      while True:
        tasks = provider.lookup_job_tasks('*', job_name_list=[self.job_name], max_jobs=1)
        logger.debug('Tasks: %s' % tasks)

        if not tasks:
          raise RuntimeError('Job not found: %s' % self.job_name)

        is_running = False
        status = None

        # Wait until all tasks succeed; abort if any task fails or is canceled
        for task in tasks:
          status = provider.get_task_field(task, 'job-status')

          if status == 'RUNNING':
            is_running = True
          elif status == 'CANCELED':
            raise RuntimeException('Job %s: CANCELED' % self.job_name)
          elif status == 'FAILURE':
            error = provider.get_task_field(task, 'error-message')
            raise RuntimeException('Job %s: FAILURE. Error message: %s' % (self.job_name, error))

        if is_running:
          time.sleep(self.poll_interval)
        else:
          break

    logger.info('Job %s: SUCCESS' % self.job_name)
    return 'Success'

  def expand(self, pcoll):
    return (pcoll
        | 'JobName' >> beam.Map(lambda x: self.job_name)
        | 'BreakFusion' >> beam.Map(lambda x: (x, id(x),))
        | 'CombinePerKey' >> beam.CombinePerKey(beam.combiners.TopCombineFn(1))
        | 'UnbreakFusion' >> beam.Map(lambda x: x[0])
        | 'Wait' >> beam.Map(self.wait_for_job))

def create_branches(branches, pcoll, provider_options):
  """Create branches in the DAG."""

  logger.info('Branch count: %i' % len(branches))
  pcoll_tuple = ()

  for branch in branches:
    logger.info('Adding branch')
    output = create_graph(branch, pcoll, provider_options)
    pcoll_tuple = pcoll_tuple + (output,)

  logger.info('Transform: MergeBranches')
  output = pcoll_tuple | 'MergeBranches' >> MergeBranches()
  return output

def create_graph(graph_item, pcoll, provider_options):
  """Recursively construct the Beam graph."""

  output = None

  if isinstance(graph_item, basestring):
    logger.info('Adding job %s' % graph_item)
    job_name = graph_item

    logger.info("Transform: WaitForJob")
    output = pcoll | job_name >> WaitForJob(provider_options, job_name)

  elif isinstance(graph_item, (list, tuple)):
    logger.info('Adding job list with length %i' % len(graph_item))
    output = pcoll
    tasks = graph_item
    for task in tasks:
      output = create_graph(task, output, provider_options)

  elif isinstance(graph_item, dict):
    logger.info('Adding branches')
    branches = graph_item['BRANCH']
    output = create_branches(branches, pcoll, provider_options)

  else:
    raise ValueError('Invalid graph item %s' % graph_item)

  return output

def dag(argv):
  """Create the workflow graph from command-line arguments"""
  known_args, beam_options = parse_args(argv)
  yaml_string = known_args.dag.decode('string_escape')
  dag = yaml.load(yaml_string)
  return dag

def provider_options(argv):
  """Create the provider options from command-line arguments"""
  provider_options, beam_options = parse_args(argv)
  return provider_options

def beam_options(argv):
  """Create the Apache Beam pipeline options from command-line arguments"""
  known_args, beam_options = parse_args(argv)
  return beam_options

def view(dag, provider_options, beam_options):
  """Create a Beam graph to visualize execution of a workflow

  Args:
    dag: a list of job names or dictionaries with key 'BRANCH' and
        value a dag (a job name, list, or dictionary...)
    provider_options: options for the batch job provider
    beam_options: options for Apache Beam
  """
  pipeline_options = PipelineOptions(beam_options)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  with beam.Pipeline(options=pipeline_options) as p:
    # Kick off the pipeline with a dummy value
    pcoll = p | 'Create' >> beam.Create(['pipeline'])
    output = create_graph(dag, pcoll, provider_options)
    print output

def main(argv=None):
  view(
      dag(argv),
      provider_options(argv),
      beam_options(argv))

if __name__ == '__main__':
  main()
