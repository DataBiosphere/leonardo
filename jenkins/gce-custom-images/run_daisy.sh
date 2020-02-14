#!/usr/bin/env bash

OUTPUT_IMAGE_NAME="leo-custom-gce-image-200214-10-25"

docker run -it --rm -v /Users/kyuksel/github/leonardo/jenkins/gce-custom-images:/daisy_source_files \
  gcr.io/compute-image-tools/daisy \
  -project broad-dsde-dev \
  -zone us-central1-a \
  -default_timeout 60m \
  -oauth /daisy_source_files/application_default_credentials.json \
  -var:base_image projects/debian-cloud/global/images/debian-9-stretch-v20200210 \
  -var:output_image "$OUTPUT_IMAGE_NAME" \
  -var:installation_script_name prepare_custom_leonardo_gce_image.sh \
  -var:installation_script_dir /daisy_source_files \
  /daisy_source_files/leo_custom_gce_image.wf.json
