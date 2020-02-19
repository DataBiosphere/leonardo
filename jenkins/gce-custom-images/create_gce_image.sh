#!/usr/bin/env bash

set -e -x

# This script is a wrapper around the command that runs the custom GCE image
# creation tool - Daisy - for local development and documentation purposes.
# It should be run from the root of the Leonardo repo.
# It assumes that a Daisy image was pulled; e.g. docker pull gcr.io/compute-image-tools/daisy:latest

# The date-time suffix is in the format yymmdd-hh-mm but it could be anything.
OUTPUT_IMAGE_NAME="leo-custom-gce-image-200219-16-05"

# Set this to the tag of the Daisy image you had pulled
DAISY_IMAGE_TAG="latest"

# The source directory should contain `application_default_credentials.json`
# which can be generated via `gcloud auth application-default login` and is saved at
# `~/.config/gcloud/application_default_credentials.json` by default.
SOURCE_DIR="/Users/kyuksel/github/leonardo/jenkins/gce-custom-images"

# Set this to "true" if you want to validate the workflow without actually executing it
VALIDATE_WORKFLOW="false"

if [[ "$VALIDATE_WORKFLOW" == "true" ]] ; then
  DAISY_CONTAINER="gcr.io/compute-image-tools/daisy:${DAISY_IMAGE_TAG} -validate"
else
  DAISY_CONTAINER="gcr.io/compute-image-tools/daisy:${DAISY_IMAGE_TAG}"
fi

docker run -it --rm -v "$SOURCE_DIR":/daisy_source_files \
  $DAISY_CONTAINER \
  -project broad-dsde-dev \
  -zone us-central1-a \
  -default_timeout 60m \
  -oauth /daisy_source_files/application_default_credentials.json \
  -var:base_image projects/debian-cloud/global/images/debian-9-stretch-v20200210 \
  -var:output_image "$OUTPUT_IMAGE_NAME" \
  -var:installation_script_name prepare_custom_leonardo_gce_image.sh \
  -var:installation_script_dir /daisy_source_files \
  /daisy_source_files/leo_custom_gce_image.wf.json
