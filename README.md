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

The best way to understand is to look at an example: `dview_example.sh` is
just that. Make a copy of it called `my_dview_example`, and then set a few
parameters at the top of the script based on your Google Cloud Project details,
including the project and bucket you created above. Then run:

    ./my_dview_example.sh

Your shell prompt will block until the pipeline completes. If you'd prefer
you can use the Linux command `screen` or run the script in the background
by appending an `&` to shell command.

After running dview, open your browser to:

    https://console.developers.google.com/project/MY-PROJECT/dataflow/job

Change `MY-PROJECT` to the name of your Cloud project. You will see a list
of your Dataflow jobs, and you can click on your new Dataflow job name
to view the execution graph that will live-update as your job runs.

### Explanation

## What next?

* See more documentation for:
  * [dsub](https://github.com/googlegenomics/dsub)
