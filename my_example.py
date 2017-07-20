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

"""Example using dview from a python workflow."""

from dsub.commands import dsub
import os
import dview
import thread

# Comment out the first line for a dry run or the second to run on Dataflow
#DRY_RUN=""
DRY_RUN='--dry-run'

PROJECT='long-stack-300'
LOGGING='gs://jbingham-scratch/dsub'
DVIEW_ARGS=[
    '--runner', 'direct',
    '--provider', 'google',
    '--project', PROJECT,
    '--setup-file', '/Users/binghamj/code/dview/setup.py',
    '--extra-package', '/Users/binghamj/code/dsub/dist/dsub-0.0.0.tar.gz',
    '--temp-location', 'gs://jbingham-scratch/dview/temp',
    DRY_RUN]
DSUB_ARGS=[
    '--provider', 'google',
    '--project', PROJECT,
    '--zones', "['us-central*']",
    DRY_RUN]

def main():
  """Run a sample workflow with a viewer"""

  # Define unique friendly names for the individual jobs for display in the UI.
  pid = os.getpid()
  job1_name = 'job1-%s' % pid
  job2_name = 'job2-%s' % pid
  job3_name = 'job3-%s' % pid
  job4_name = 'job4-%s' % pid

  # Define a YAML graph with job names and a BRANCH
  dag = """
- ${JOB1_NAME}
- BRANCH:
  - ${JOB2_NAME}
  - ${JOB3_NAME}
- ${JOB4_NAME}
"""

  # Start the viewer *before* the tasks, non-blocking in a separate thread
  thread.start_new_thread(dview.call, (['--dag', dag] + DVIEW_ARGS,))

  # Submit the jobs in the graph with some sleep time because they run so fast
  job1_id = dsub.call([
      '--name', job1_name,
      '--command', 'sleep 4m; echo hello_1',
      '--logging', LOGGING + '/' + job1_name]
      + DSUB_ARGS)['job-id']
  job2_id = dsub.call([
      '--name', job2_name,
      '--after', job1_id,
      '--command', 'sleep 30s; echo hello_2',
      '--logging', LOGGING + '/' + job2_name]
      + DSUB_ARGS)['job-id']
  job3_id = dsub.call([
      '--name', job3_name,
      '--after', job1_id,
      '--command', 'sleep 30s; echo hello_3',
      '--logging', LOGGING + '/' + job3_name]
      + DSUB_ARGS)['job-id']
  job4_id = dsub.call([
      '--name', job4_name,
      '--after', '%s %s' % (job2_id, job3_id),
      '--command', 'sleep 30s; echo hello_4',
      '--logging', LOGGING + '/' + job3_name]
      + DSUB_ARGS)['job-id']

if __name__ == '__main__':
  main()
