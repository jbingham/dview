### Disclaimer

This is not an official Google product.

# dview: a viewer for batch processing pipelines

## Overview

dview is an open-source viewer for batch processing pipelines submitted
using [dsub]((https://github.com/googlegenomics/dsub).

dview works by creating Apache Beam jobs, and relying on the Beam runners to
provide the visualization. For example, Google Cloud Dataflow provides a
live-updating view of the pipeline execution graph. When running dview
with Dataflow as the runner, you can open the Dataflow viewer to track
progress.

![dview in Dataflow](screenshot.png?raw=true "dview in Dataflow")

dview supports any Apache Beam runner that offers graph visualization.
**However, as of July 2017 Dataflow is the only runner that works with
dview.** Beam python runners for Spark and Flink are in development.

For testing, you can also run dview locally, without visualization.

## Getting started

1.  Create and activate a Python virtualenv (optional but strongly recommended).

        # (You can do this in a directory of your choosing.)
        virtualenv dview_env
        source dview_env/bin/activate

1.  Install dsub.

        git clone https://github.com/jbingham/dsub
        python dsub/setup.py install

1. Clone the dview github repository.

        git clone https://github.com/jbingham/dview
        cd dview

1.  Verify the installation by running:

        ./dview --help

### Getting started on Google Cloud

1.  Sign up for a Google Cloud Platform account and
    [create a project](https://console.cloud.google.com/project?).

1.  [Enable billing](https://support.google.com/cloud/answer/6293499#enable-billing).

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

The best way to understand is to look at an example: `dview_example.sh` is
just that. Make a copy of it called `my_dview_example`, and then set a few
parameters at the top of the script based on your Google Cloud Project details,
including the project and bucket you created above. Then run:

    ./my_dview_example.sh

Your shell prompt will block until the pipeline completes. If you'd prefer
you can use the Linux command `screen` or run the script in the background
by appending an `&` to shell command.

After running dview, open your browser to:

    https://console.developers.google.com/project/my-project/dataflow/job

Change `my-project` to the name of your Cloud project. You will see a list
of your Dataflow jobs, and you can click on your new Dataflow job name
to view the execution graph that will live-update as your job runs.

## What next?

* See more documentation for:
  * [dsub](https://github.com/googlegenomics/dsub)
