#!/usr/bin/env bash

SBT_CMD=${1-"test"}
echo $SBT_CMD

set -o pipefail

cp -rf src/test/resources/* automation/src/test/resources

sbt -batch -Dheadless=true "project automation" "${SBT_CMD}"
TEST_EXIT_CODE=$?
sbt clean

if [[ $TEST_EXIT_CODE != 0 ]]; then exit $TEST_EXIT_CODE; fi
