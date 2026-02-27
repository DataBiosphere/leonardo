#!/usr/bin/env bash
set -e -x

# The SBT command to run in the docker image
# SBT_TEST_COMMAND

# Path to leonardo-account.json for the qa domain
# LEONARDO_ACCOUNT_JSON_PATH

# Install Python 3.10 for gcloud compatibility (3.9 is no longer supported)
apt-get update -qq && apt-get install -y python3.10 && rm -rf /var/lib/apt/lists/*
export CLOUDSDK_PYTHON=python3.10

# Install gcloud CLI
# Downloading gcloud package
# https://cloud.google.com/sdk/docs/install#linux
# https://stackoverflow.com/questions/28372328/how-to-install-the-google-cloud-sdk-in-a-docker-image
export CLOUDSDK_CORE_DISABLE_PROMPTS=1
curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz

# Installing the package
mkdir -p /usr/local/gcloud
tar -C /usr/local/gcloud -xf /tmp/google-cloud-sdk.tar.gz
/usr/local/gcloud/google-cloud-sdk/install.sh > /dev/null

# Adding the package path to local
export PATH=$PATH:/usr/local/gcloud/google-cloud-sdk/bin

mkdir -p /root/.ssh

gcloud auth activate-service-account --key-file=$LEONARDO_ACCOUNT_JSON_PATH
export GOOGLE_APPLICATION_CREDENTIALS=$LEONARDO_ACCOUNT_JSON_PATH


echo "Installing lsof"
yes | apt update > /dev/null
yes | apt install lsof > /dev/null

echo "Done installing lsof, running tests"

# Run the SBT tests
sbt -batch -Dheadless=true "project automation" "$SBT_TEST_COMMAND"
