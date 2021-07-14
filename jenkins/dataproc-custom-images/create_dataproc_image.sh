#!/usr/bin/env bash

set -e -x

# This script creates custom VM image for Dataproc - for local development and documentation purposes.
#
# It should be run from the root of the Leonardo repo.
#
# gsutil must have been installed.
#
# application_default_credentials.json needs to be copied to jenkins/gce-custom-images/ which is mounted on Daisy container
# Credentials can be refreshed via 'gcloud auth application-default login' with project set to 'broad-dsde-dev' using
# Broad account. They are saved at '~/.config/gcloud/application_default_credentials.json' by default.
#
# Usage: under `leonardo` root dir, `jenkins/dataproc-custom-images/create_dataproc_image.sh qi-713-1008`
WORK_DIR=`pwd`/jenkins/dataproc-custom-images/dataproc-custom-images

pushd $WORK_DIR

# Your testing project
GOOGLE_PROJECT="broad-dsp-gcr-public"
REGION="us-central1"
ZONE="${REGION}-a"

customDataprocImageBaseName="test"
dp_version_formatted="1-4-51-debian10"
# This needs to be unique for each run
imageID=$1

gcloud config set dataproc/region us-central1

python generate_custom_image.py \
    --image-name "$customDataprocImageBaseName-$dp_version_formatted-$imageID" \
    --dataproc-version "1.4.51-debian10" \
    --customization-script ../prepare-custom-leonardo-jupyter-dataproc-image.sh \
    --zone $ZONE \
    --gcs-bucket "gs://qi-test" \
    --project-id=$GOOGLE_PROJECT \
    --disk-size=60

popd