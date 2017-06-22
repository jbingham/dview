#/bin/bash

dview \
  --dag "\
  - job1\
  - BRANCH:\
- job2\
  - job3\
  - job4"\
  --runner dataflow\
  --temp-path gs://my-bucket/logs\
  --setup_file /abs/path/to/dsub/setup.py\
  --project my-cloud-project \
  --zones "us-central1-*" \
  --logging gs://my-bucket/logs

JOB1 = $(dsub \
  --name job1\
  --project my-cloud-project \
  --zones "us-central1-*" \
  --logging gs://my-bucket/logs \
  --script "echo hello_1")

JOB2 = $(dsub \
  --name job2\
  --after ${JOB1}\
  --project my-cloud-project \
  --zones "us-central1-*" \
  --logging gs://my-bucket/logs \
  --script "echo hello_2")

JOB3 = $(dsub \
  --name job3\
  --after ${JOB1}\
  --project my-cloud-project \
  --zones "us-central1-*" \
  --logging gs://my-bucket/logs \
  --script "echo hello_3")

JOB2 = $(dsub \
  --name job2 job3\
  --after ${JOB1}\
  --project my-cloud-project \
  --zones "us-central1-*" \
  --logging gs://my-bucket/logs \
  --script "echo hello_4")
