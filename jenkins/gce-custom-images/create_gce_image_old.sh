#!/usr/bin/env bash

set -e -x

# This script is a wrapper around the command that runs the custom GCE image
# creation tool - Daisy - for local development and documentation purposes.
#
# It should be run from the root of the Leonardo repo.
#
# gsutil must have been installed.
#
# application_default_credentials.json needs to be copied to jenkins/gce-custom-images/ which is mounted on Daisy container
# Credentials can be refreshed via 'gcloud auth application-default login' with project set to 'broad-dsde-dev' using
# Broad account
#
# Also make sure that you have the right version of the hardening repo via:
# `git -C jenkins/gce-custom-images/dsp-appsec-base-image-hardening checkout <desired version hash>`

# Set this to "true" if you want to validate the workflow without actually executing it
VALIDATE_WORKFLOW="false"

# The source directory should contain `application_default_credentials.json`
# which can be generated via `gcloud auth application-default login` and is saved at
# `~/.config/gcloud/application_default_credentials.json` by default.
SOURCE_DIR="/Users/kyuksel/github/leonardo/jenkins/gce-custom-images"

# Underscores are not accepted as image name
OUTPUT_IMAGE_NAME=leo-custom-gce-image-$(whoami)-$(date +"%Y-%m-%d-%H-%M-%S")

PROJECT="broad-dsde-dev"
REGION="us-central1"
ZONE="${REGION}-a"

# The bucket that Daisy uses as scratch area to store source and log files.
# If it doesn't exist, we create it prior to launching Daisy and
# the Daisy workflow cleans up all but daisy.log at the end.
DAISY_BUCKET_PATH="gs://test-leo-custom-gce-image-daisy-scratch-bucket"

# Set this to the tag of the Daisy image you had pulled
DAISY_IMAGE_TAG="release"

# Create the Daisy scratch bucket if it doesn't exist. The Daisy workflow will clean it up at the end.
gsutil ls $DAISY_BUCKET_PATH || gsutil mb -b on -p $PROJECT -l $REGION $DAISY_BUCKET_PATH

if [[ "$VALIDATE_WORKFLOW" == "true" ]]; then
  DAISY_CONTAINER="gcr.io/compute-image-tools/daisy:${DAISY_IMAGE_TAG} -validate"
else
  DAISY_CONTAINER="gcr.io/compute-image-tools/daisy:${DAISY_IMAGE_TAG}"
fi

docker run -it --rm -v "$SOURCE_DIR":/gce-custom-images \
  $DAISY_CONTAINER \
  -project $PROJECT \
  -zone $ZONE \
  -gcs_path $DAISY_BUCKET_PATH \
  -default_timeout 60m \
  -oauth /gce-custom-images/application_default_credentials.json \
  -var:base_image projects/debian-cloud/global/images/debian-9-stretch-v20200210 \
  -var:output_image "$OUTPUT_IMAGE_NAME" \
  -var:gce_custom_images_dir /gce-custom-images \
  -var:installation_script_name prepare_custom_leonardo_gce_image.sh \
  -var:image_hardening_script cis-harden-images/debian9/harden-images.sh \
  -var:cis_hardening_playbook_config cis-harden-images/debian9/deb9-cis-playbook.yml \
  -var:cis_hardening_playbook_requirements cis-harden-images/debian9/requirements.yml \
  /gce-custom-images/leo_custom_gce_image.wf.json

# Daisy doesn't clean it up all so we remove the bucket manually
gsutil rm -r $DAISY_BUCKET_PATH
