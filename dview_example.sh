#/bin/bash

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

# dview_example.sh
#
# An example of dview with an execution graph that branches and then recombines.
# The script creates a viewer using Apache Beam + Google Dataflow and then
# launches the jobs in the graph with task dependencies.

set -o errexit
set -o nounset

# Set these variables to connect to Google Cloud and pass all dependencies.

declare DSUB_PATH="/Users/binghamj/code/jbingham/dsub"
declare GCP_PROJECT="long-stack-300"
declare GCP_ZONES="us-central1-*"
declare TEMP_PATH="gs://jbingham-scratch/dview"
declare LOGGING_PATH="gs://jbingham-scratch/logs"

# Settings for dview and the specific Beam runner it uses.

declare RUNNER="dataflow"
declare RUNNER_OPTS="--runner ${RUNNER} --project ${GCP_PROJECT}"

# Settings for dsub and the specific provider it uses.

declare PROVIDER="google"
declare PROVIDER_OPTS="\
  --provider ${PROVIDER}\
  --project ${GCP_PROJECT}\
  --zones ${GCP_ZONES}\
  --logging ${LOGGING_PATH}"

# Define some friendly names for the individual jobs for display in the UI.
# Add a unique suffix to distinguish multiple runs of the same workflow.

declare PID=$$
declare JOB1_NAME="job1-${PID}"
declare JOB2_NAME="job2-${PID}"
declare JOB3_NAME="job3-${PID}"
declare JOB4_NAME="job4-${PID}"

# The graph (--dag) is defined in YAML with job names as nodes
# and the special BRANCH keyword to indicate a branch point.

DAG="\
 - ${JOB1_NAME}\n\
 - BRANCH:\n\
   - ${JOB2_NAME}\n\
   - ${JOB3_NAME}\n\
 - ${JOB4_NAME}\n"\

# Create the viewer *before* starting the workflow.
# Run in a separate process (&), so that it doesn't block.

./dview \
  ${RUNNER_OPTS}\
  --dag "${DAG}"\
  --temp-path ${TEMP_PATH}\
  --setup_file ${DSUB_PATH}/setup.py &

# Submit the jobs in the graph.

JOB1_ID=$(dsub \
  ${PROVIDER_OPTS}\
  --name ${JOB1_NAME}\
  --command "sleep 2m; echo hello_1"\
  --wait)

JOB2_ID=$(dsub \
  ${PROVIDER_OPTS}\
  --name ${JOB2_NAME}\
  --after ${JOB1_ID}\
  --command "sleep 1m; echo hello_2"\
  --wait)

JOB3_ID=$(dsub \
  ${PROVIDER_OPTS}\
  --name ${JOB3_NAME}\
  --after ${JOB1_ID}\
  --command "sleep 2m; echo hello_3"\
  --wait)

JOB4_ID=$(dsub \
  ${PROVIDER_OPTS}\
  --name ${JOB4_NAME}\
  --after ${JOB2_ID} ${JOB3_ID}\
  --command "sleep 2m; echo hello_4")

