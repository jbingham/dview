### Disclaimer

This is not an official Google product.

# dview: a viewer for batch processing pipelines

## Overview

dview is an open-source viewer for batch processing pipelines. 

dview works by creating Apache Beam jobs, and relying on the Beam runners to
provide the visualization. For example, Google Cloud Dataflow provides a
live-updating view of the pipeline execution graph. When running dview
with Dataflow as the runner, you can open the Dataflow viewer to track
progress. Apache Spark also offers a viewer. 

dview supports any Apache Beam runner. You can also run it locally first
for testing, without visualization.

If others find dview useful, our hope is to contribute dsub to an open-source
foundation for use by the wider batch computing community.

## Getting started

1.  Create and activate a Python virtualenv (optional but strongly recommended).

        # (You can do this in a directory of your choosing.)
        virtualenv dview_env
        source dview_env/bin/activate

1.  Clone this repository as well as the dsub repository.

        git clone https://github.com/jbingham/dview
        git clone https://github.com/jbingham/dsub
        cd dview

1.  Install dview (this will also install the dependencies, including dsub)

        python setup.py install

1.  Set up Bash tab completion (optional).

        source bash_tab_complete

1.  Verify the installation by running:

        dview --help

### Getting started on Google Cloud

1.  Sign up for a Google Cloud Platform account and
    [create a project](https://console.cloud.google.com/project?).

1.  [Enable the APIs](https://console.cloud.google.com/flows/enableapi?apiid=genomics,storage_component,compute_component&redirect=https://console.cloud.google.com).

1.  [Install the Google Cloud SDK](https://cloud.google.com/sdk/) and run

        gcloud init

    This will set up your default project and grant credentials to the Google
    Cloud SDK. Now provide [credentials](https://developers.google.com/identity/protocols/application-default-credentials)
    so dsub can call Google APIs:

        gcloud auth application-default login

1.  Create a [Google Cloud Storage](https://cloud.google.com/storage) bucket.

    The dsub logs and output files will be written to a bucket. Create a
    bucket using the [storage browser](https://cloud.google.com/storage/browser?project=)
    or run the command-line utility [gsutil](https://cloud.google.com/storage/docs/gsutil)
    , included in the Cloud SDK.

        gsutil mb gs://my-bucket

    Change `my-bucket` to a unique name that follows the
    [bucket-naming conventions](https://cloud.google.com/storage/docs/bucket-naming).

    (By default, the bucket will be in the US, but you can change or
    refine the [location](https://cloud.google.com/storage/docs/bucket-locations)
    setting with the `-l` option.)

## Running a job

To view a multi-step batch job with dview, you first have to define your pipeline.
You launch dview at the top of your pipeline script, passing to dview the
names of the individual jobs in your pipeline. Then run the script.

Here's a bash example of a graph that branches and runs two jobs in parallel,
then runs a final job after the two branches succeed. The graph is defined in YAML.
A copy of this example can be found in the dview folder.

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

Change `my-cloud-project` to your Google Cloud project, and `my-bucket`
to the bucket you created above. Then save the file as `my_dview_example.sh`.

To run the pipeline and visualize the execution graph, go back to
your shell prompt and run:

    test_dview.sh

Your shell prompt will block until the pipeline completes. If you'd rather
you can use the Linux command `screen` or run the script in the background
by appending an `&` to the shell command.

After running dview:
* open https://console.cloud.google.com in your browser
* login
* open the hamburger menu in the upper left
* click on the Dataflow link
* click on the running job

If all is well, you will see an execution graph that live-updates as
your job runs.

### Explanation

## What next?

* See more documentation for:
  * [dsub](https://github.com/googlegenomics/dsub)
