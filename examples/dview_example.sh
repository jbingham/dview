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
# Example using dview from a bash workflow.

set -o errexit
set -o nounset

# Comment out the first line for a dry run or the second to run the tasks
#declare DRY_RUN=""
declare DRY_RUN="--dry-run"

declare PROJECT="YOUR-PROJECT-ID"
declare LOGGING="YOUR-LOGGING-PATH"
declare DVIEW_OPTS="
    --runner direct  # Change to `dataflow` to run with Google Dataflow
    --provider google
    --project ${PROJECT}
    --temp-location ${LOGGING}
    ${DRY_RUN}"
declare DSUB_OPTS="
    --provider google
    --project ${PROJECT}
    --logging ${LOGGING}
    --zones 'us-central1-*'
    ${DRY_RUN}"

# Define unique friendly names for the individual jobs for display in the UI.
declare PID=$$
declare JOB1_NAME="job1-${PID}"
declare JOB2_NAME="job2-${PID}"
declare JOB3_NAME="job3-${PID}"
declare JOB4_NAME="job4-${PID}"

# Define a YAML graph with job names and a BRANCH
DAG="
- ${JOB1_NAME}
- BRANCH:
  - ${JOB2_NAME}
  - ${JOB3_NAME}
- ${JOB4_NAME}"

# Start the viewer *before* the tasks, non-blocking in a separate process (&)
dview ${DVIEW_OPTS} --dag "${DAG}" &

# Submit the jobs in the graph with some sleep time because they run so fast
JOB1_ID=$(dsub \
    --name ${JOB1_NAME}\
    --command "sleep 4m; echo hello_1"\
    ${DSUB_OPTS})
JOB2_ID=$(dsub \
    --name ${JOB2_NAME}\
    --after ${JOB1_ID}\
    --command "sleep 30s; echo hello_2"\
    ${DSUB_OPTS})
JOB3_ID=$(dsub \
    --name ${JOB3_NAME}\
    --after ${JOB1_ID}\
    --command "sleep 30s; echo hello_3"\
    ${DSUB_OPTS})
JOB4_ID=$(dsub \
    ${DSUB_OPTS}\
    --name ${JOB4_NAME}\
    --after ${JOB2_ID} ${JOB3_ID}\
    --command "sleep 30s; echo hello_4"\
    ${DSUB_OPTS})
