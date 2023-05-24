#!/usr/bin/env bash

set -e -x

# This script creates custom VM image for Dataproc - for local development and documentation purposes.
#
# It should be run from the root of the Leonardo repo.
#
# gsutil must have been installed.
#
# application_default_credentials.json needs to be copied to jenkins/dataproc-custom-images/ which is mounted on Daisy container
# Credentials can be refreshed via 'gcloud auth application-default login' with project set to 'broad-dsde-dev' using
# Broad account. They are saved at '~/.config/gcloud/application_default_credentials.json' by default.
#
# Usage: under `leonardo` root dir, `jenkins/dataproc-custom-images/create_dataproc_image.sh`
WORK_DIR=`pwd`/jenkins/dataproc-custom-images/dataproc-custom-images
# Your testing project
GOOGLE_PROJECT="broad-dsp-gcr-public"
REGION="us-central1"
ZONE="${REGION}-a"

if [ -z "$DATAPROC_IMAGE_BUCKET" ]; then
  DATAPROC_IMAGE_BUCKET="gs://leo-dataproc-image-creation-logs"
fi
TEST_BUCKET="gs://leo-dataproc-image-creation-logs"

pushd $WORK_DIR

DATAPROC_BASE_NAME="leo-dataproc-image"
DP_VERSION_FORMATTED="2-1-11-debian11"
# This needs to be unique for each run
IMAGE_ID=$(date +"%Y-%m-%d-%H-%M-%S")
OUTPUT_IMAGE_NAME="$DATAPROC_BASE_NAME-$DP_VERSION_FORMATTED-$IMAGE_ID"

gcloud config set dataproc/region us-central1

python generate_custom_image.py \
    --image-name "$OUTPUT_IMAGE_NAME" \
    --dataproc-version "2.1.11-debian11" \
    --customization-script ../prepare-custom-leonardo-jupyter-dataproc-image.sh \
    --zone $ZONE \
    --gcs-bucket $DATAPROC_IMAGE_BUCKET \
    --project-id=$GOOGLE_PROJECT \
    --disk-size=120

if ! [ -z "$OUTPUT_FILE_PATH" ]; then
  echo "projects/$GOOGLE_PROJECT/global/images/$OUTPUT_IMAGE_NAME" > $OUTPUT_FILE_PATH
fi

popd