#!/usr/bin/env bash

SBT_CMD=${1-"test"}
echo $SBT_CMD

set -o pipefail

sbt -batch -Djsse.enableSNIExtension=false -Dheadless=true "${SBT_CMD}"
TEST_EXIT_CODE=$?
sbt clean

if [[ $TEST_EXIT_CODE != 0 ]]; then exit $TEST_EXIT_CODE; fi
