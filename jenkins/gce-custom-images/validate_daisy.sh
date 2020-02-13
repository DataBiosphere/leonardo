#!/usr/bin/env bash

docker run -it --rm -v /Users/kyuksel/github/leonardo/jenkins/gce-custom-images:/daisy_source_files \
  gcr.io/compute-image-tools/daisy \
  -validate \
  -project broad-dsde-dev \
  -zone us-central1-a \
  -default_timeout 60m \
  -oauth /daisy_source_files/application_default_credentials.json \
  -var:base_image projects/centos-cloud/global/images/family/centos-7 \
  -var:output_image leo-custom-gce-image-200213-1 \
  /daisy_source_files/leo_custom_gce_image.wf.json
