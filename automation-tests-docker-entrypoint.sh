#!/usr/bin/env bash

SBT_CMD=${1-"test"}
echo $SBT_CMD

set -o pipefail

cp -rf src/test/resources/* automation/src/test/resources
gcloud auth activate-service-account --key-file=/app/key.json
export GOOGLE_APPLICATION_CREDENTIALS=/app/key.json

sbt -batch -Dheadless=true "project automation" "${SBT_CMD}"
TEST_EXIT_CODE=$?
sbt clean

if [[ $TEST_EXIT_CODE != 0 ]]; then exit $TEST_EXIT_CODE; fi
