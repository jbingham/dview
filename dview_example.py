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

from dsub.providers import provider_base
from dsub.lib import dsub_util
from dsub.lib import param_util
from dsub.lib import job_util
import os

JOB_NAME='job1-%s' % os.getpid()
MESSAGE='hello, world'
COMMAND='echo ${MESSAGE}'
IMAGE='ubuntu:14.04'

PROVIDER='google'
PROJECT='long-stack-300'
ZONES=['us-central*']
LOGGING_PATH='gs://jbingham-scratch/dsub'

DRY_RUN=True

class Provider:
  project = PROJECT
  dry_run = DRY_RUN

def submit_job():
  """Submit a dsub job"""

  provider = provider_base.get_provider(Provider())

  input_file_param_util = param_util.InputFileParamUtil('input')
  output_file_param_util = param_util.OutputFileParamUtil('output')

  metadata = provider.prepare_job_metadata(
      JOB_NAME,
      JOB_NAME,
      dsub_util.get_default_user())
  metadata['script'] = job_util.Script(JOB_NAME, COMMAND)

  print provider.submit_job(
      job_util.JobResources(
          image=IMAGE,
          zones=ZONES,
          logging=(LOGGING_PATH)),
      metadata,
      param_util.args_to_job_data(
          ['MESSAGE=%s' % MESSAGE],
          [],
          [],
          [],
          [],
          input_file_param_util,
          output_file_param_util))

if __name__ == '__main__':
  submit_job()
