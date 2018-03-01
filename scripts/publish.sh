#!/usr/bin/env bash

set -e

# sbt publish publishes libs to Artifactory for the scala version sbt is running as.
# sbt +publish publishes libs to Artifactory for all scala versions listed in crossScalaVersions.
# We only do sbt publish here because Travis runs against 2.11 and 2.12 in separate jobs, so each one publishes its version to Artifactory.
if [[ "$TRAVIS_PULL_REQUEST" == "false" && "$TRAVIS_BRANCH" == "develop" ]]; then
	sbt ++$TRAVIS_SCALA_VERSION publish -Dproject.isSnapshot=false
else
	sbt ++$TRAVIS_SCALA_VERSION publish -Dproject.isSnapshot=true
fi

