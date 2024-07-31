#!/usr/bin/env bash

set -e -x

# This script is a wrapper around the command that runs the GCE VM snapshot
# creation tool - Daisy - for local development and documentation purposes.
#
# It should be run from the root of the Leonardo repo.
#
# gsutil must have been installed.
#
# application_default_credentials.json needs to be copied to jenkins/gce-custom-images/ which is mounted on Daisy container
# Credentials can be refreshed via 'gcloud auth application-default login' with project set to 'broad-dsde-dev' using
# Broad account. They are saved at '~/.config/gcloud/application_default_credentials.json' by default.

# Set this to "true" if you want to validate the workflow without actually executing it
VALIDATE_WORKFLOW="false"
PWD=`pwd`
# The source directory should contain `application_default_credentials.json`
# which can be generated via `gcloud auth application-default login` and is saved at
# `~/.config/gcloud/application_default_credentials.json` by default.
SOURCE_DIR="$PWD/jenkins/gce-custom-images"

# Underscores are not accepted as image name
OUTPUT_IMAGE_NAME=leo-gce-image-$(date +"%Y-%m-%d-%H-%M-%S")

GOOGLE_PROJECT="broad-dsp-gcr-public"
REGION="us-central1"
ZONE="${REGION}-a"

# The bucket that Daisy uses as scratch area to store source and log files.
# If it doesn't exist, we create it prior to launching Daisy and
# the Daisy workflow cleans up all but daisy.log at the end.
if [ -z "$GCE_IMAGE_BUCKET" ]; then
  GCE_IMAGE_BUCKET="gs://leo-gce-image-creation-logs"
fi


# Set this to the tag of the Daisy image you had pulled
DAISY_IMAGE_TAG="release"

# When updating, to find the resource path:
#    1. run `gcloud compute images list --no-standard-images --project=cos-cloud | grep lts` to get the list of available container-optimized OS images
#    2. select the image of interest, say, `cos-89-16108-403-22`
#    3. run `gcloud compute images describe cos-89-16108-403-22 --project cos-cloud | grep selfLink`
#    4. extract the segments starting with 'projects'
BASE_IMAGE="/projects/cos-cloud/global/images/cos-113-18244-85-64"

if [[ "$VALIDATE_WORKFLOW" == "true" ]]; then
  DAISY_CONTAINER="gcr.io/compute-image-tools/daisy:${DAISY_IMAGE_TAG} -validate"
else
  DAISY_CONTAINER="gcr.io/compute-image-tools/daisy:${DAISY_IMAGE_TAG}"
fi

docker run --rm -v "$SOURCE_DIR":/gce-custom-images \
  $DAISY_CONTAINER \
  -project $GOOGLE_PROJECT \
  -zone $ZONE \
  -gcs_path $GCE_IMAGE_BUCKET \
  -default_timeout 60m \
  -oauth /gce-custom-images/application_default_credentials.json \
  -var:base_image "$BASE_IMAGE" \
  -var:output_image "$OUTPUT_IMAGE_NAME" \
  -var:gce_images_dir /gce-custom-images \
  -var:installation_script_name prepare_gce_image.sh \
  /gce-custom-images/gce_image.wf.json

gcloud compute images add-iam-policy-binding \
    projects/$GOOGLE_PROJECT/global/images/$OUTPUT_IMAGE_NAME \
    --member='allAuthenticatedUsers' \
    --role='roles/compute.imageUser'

if ! [ -z "$OUTPUT_FILE_PATH" ]; then
  echo "projects/$GOOGLE_PROJECT/global/images/$OUTPUT_IMAGE_NAME" > $OUTPUT_FILE_PATH
fi