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

By running the dview command at the top of a script that executes
multiple dsub commands, a live-updating execution graph can be
displayed in a Spark or Dataflow UI using Beam's --runner option.
It's not necessary to use Spark or Dataflow for any dsub step, or
even to run the dsub steps using the same provider. E.g., you
can run the dsub steps locally, and generate an execution graph
in the cloud with Dataflow.

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


def parse_args(argv):
  """Parse command-line arguments"""

  now = datetime.now().strftime('%m%d%H%M%S')
  default_job_name=('dview-%s-%s' % (getpass.getuser(), now))
  setup_file=(os.path.dirname(os.path.realpath(sys.argv[0])) + '/setup.py')

  parser = argparse.ArgumentParser(
      prog='dview',
      description='Create a viewer for dsub pipelines using Apache Beam.')

  parser.add_argument(
      '--dag',
      required=True,
      help='YAML graph of job names.')
  parser.add_argument(
      '--provider',
      choices=['google', 'test-fails'],
      default='google',
      help='Service to submit jobs to.')
  parser.add_argument(
      '--runner',
      choices=['direct', 'dataflow'],
      default='direct',
      help='Apache Beam runner.')
  parser.add_argument(
      '--temp-path',
      default='your-temp-path',
      help='Storage path for temp files.')
  parser.add_argument(
      '--dry-run',
      default=False,
      action='store_true',
      help='Construct the Beam pipeline, but don\'t call any provider methods.')
  parser.add_argument(
      '--name',
      default=default_job_name,
      help='Display name for the job.')
  parser.add_argument(
      '--setup-file',
      default=setup_file,
      help='Path to setup.py in the dsub root directory, if not installed as a package.')
  parser.add_argument(
      '--extra-package',
      default='abs-path',
      help='Absolute path to the dsub package, if installed as a package.')

  google = parser.add_argument_group(
      title='google',
      description='Options for the Google provider.')
  google.add_argument(
      '--project',
      default='your-project-id',
      help='Google Cloud project ID.')
  google.add_argument(
      '--max-workers',
      default='1',
      help='Maximum number of worker VMs.')

  known_args, pipeline_args = parser.parse_known_args(argv)

  # Dataflow pipeline args
  pipeline_args.extend([
      '--runner=' + known_args.runner,
      '--project=' + known_args.project,
      '--staging_location=' + known_args.temp_path,
      '--temp_location=' + known_args.temp_path,
      '--max_num_workers=' + known_args.max_workers,
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
#  from dsub.providers import provider_base
#  import logging
  logging.basicConfig(level=logging.INFO)
  logger = logging.getLogger(__name__)

  def __init__(self, args, job_name):
    super(beam.PTransform, self).__init__()
    self.args = args
    self.job_name = job_name
    self.poll_interval = 30
    self.provider = provider_base.get_provider(self.args)

  # Block until job completes and raise an exception if it failed
  def wait_for_job(self, value):
    """Get job status from the job queue provider."""
    logger.info('Waiting for job %s' % self.job_name)

    if self.args.dry_run == True:
      logger.info('Dry run: continuing')
    else:
      # Wait for dsub to start the next task
      #time.sleep(self.poll_interval)

      logger.info('Checking job status...')
      provider = provider_base.get_provider(self.args)

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
        | 'JobName' >> beam.Map(lambda x: self.args.name)
        | 'BreakFusion' >> beam.Map(lambda x: (x, id(x),))
        | 'CombinePerKey' >> beam.CombinePerKey(beam.combiners.TopCombineFn(1))
        | 'UnbreakFusion' >> beam.Map(lambda x: x[0])
        | 'Wait' >> beam.Map(self.wait_for_job))

def create_branches(branches, pcoll, args):
  """Create branches in the DAG."""

  logger.info('Branch count: %i' % len(branches))
  pcoll_tuple = ()

  for branch in branches:
    logger.info('Adding branch')
    output = create_graph(branch, pcoll, args)
    pcoll_tuple = pcoll_tuple + (output,)

  logger.info('Transform: MergeBranches')
  output = pcoll_tuple | 'MergeBranches' >> MergeBranches()
  return output


def create_graph(graph_item, pcoll, args):
  """Recursively construct the Beam graph from the parsed YAML definition."""

  output = None

  if isinstance(graph_item, basestring):
    logger.info('Adding job %s' % graph_item)
    job_name = graph_item

    logger.info("Transform: WaitForJob")
    output = pcoll | job_name >> WaitForJob(args, job_name)

  elif isinstance(graph_item, (list, tuple)):
    logger.info('Adding job list with length %i' % len(graph_item))
    output = pcoll
    tasks = graph_item
    for task in tasks:
      output = create_graph(task, output, args)

  elif isinstance(graph_item, dict):
    logger.info('Adding branches')
    branches = graph_item['BRANCH']
    output = create_branches(branches, pcoll, args)

  else:
    raise ValueError('Invalid graph item %s' % graph_item)

  return output


def main(argv=None):
  known_args, pipeline_args = parse_args(argv)
  dry_run = known_args.dry_run
  provider = known_args.provider

  # Fix newlines before parsing YAML
  yaml_string = known_args.dag.decode('string_escape')
  dag = yaml.load(yaml_string)

  options = PipelineOptions(pipeline_args)
  options.view_as(SetupOptions).save_main_session = True

  with beam.Pipeline(options=options) as p:
    # Kick off the pipeline with a dummy value
    pcoll = p | 'Create' >> beam.Create(['pipeline'])
    output = create_graph(dag, pcoll, known_args)
    print output


if __name__ == '__main__':
  main()
