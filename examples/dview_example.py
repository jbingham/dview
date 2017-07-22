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
from dview.commands import dview
import os
import sys

# Comment out the first line for a dry run or the second to run on Dataflow
#DRY_RUN=""
DRY_RUN='--dry-run'

PROJECT='YOUR-PROJECT'
LOGGING='YOUR-PATH'
DVIEW_ARGS=[
    '--runner', 'direct',
    '--provider', 'google',
    '--project', PROJECT,
    '--temp-location', 'gs://jbingham-scratch/dview/temp',
    DRY_RUN]
DSUB_ARGS=[
    '--provider', 'google',
    '--project', PROJECT,
    '--logging', LOGGING,
    '--zones', 'us-central*',
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
- %s
- BRANCH:
  - %s
  - %s
- %s
""" % (job1_name, job2_name, job3_name, job4_name)

  # Start the viewer *before* the tasks because --after blocks with the google provider
  dview.call(['--dag', dag] + DVIEW_ARGS,)

  # Submit the jobs in the graph with some sleep time because they run so fast
  job1_id = dsub.call([
      '--name', job1_name,
      '--command', 'sleep 4m; echo hello_1',
      + DSUB_ARGS)['job-id']
  job2_id = dsub.call([
      '--name', job2_name,
      '--after', job1_id,
      '--command', 'sleep 30s; echo hello_2',
      + DSUB_ARGS)['job-id']
  job3_id = dsub.call([
      '--name', job3_name,
      '--after', job1_id,
      '--command', 'sleep 30s; echo hello_3',
      + DSUB_ARGS)['job-id']
  job4_id = dsub.call([
      '--name', job4_name,
      '--after', '%s %s' % (job2_id, job3_id),
      '--command', 'sleep 30s; echo hello_4',
      + DSUB_ARGS)['job-id']

if __name__ == '__main__':
  main()
