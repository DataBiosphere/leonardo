#!/usr/bin/env bash

# Generate and push a test Leonardo image
# Run using "./automation/push-bee-image.sh <image name>" at the root of the leonardo repo clone

set -eu

IMAGE=$1

 ./docker/install.sh .
docker build . -t gcr.io/broad-dsp-gcr-public/leonardo:$IMAGE
docker push gcr.io/broad-dsp-gcr-public/leonardo:$IMAGE