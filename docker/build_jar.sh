#!/bin/bash

# This script provides an entry point to assemble the Leonardo jar file.
# Used by the leonardo-build.yaml workflow in terra-github-workflows.
# chmod +x must be set for this script
set -e

# Get the last commit hash and set it as an environment variable
GIT_HASH=$(git log -n 1 --pretty=format:%h)

# make jar.  cache sbt dependencies. capture output and stop db before returning.
EXIT_CODE=0
docker run --rm -v $PWD:/working \
  -v sbt-cache:/root/.sbt -v jar-cache:/root/.ivy -v jar-cache:/root/.ivy2 \
  -v coursier-cache:/root/.cache/coursier \
  hseeberger/scala-sbt:eclipse-temurin-17.0.2_1.6.2_2.13.8 /working/docker/clean_install.sh /working \
  || EXIT_CODE=$?

if [ $EXIT_CODE != 0 ]; then
    echo "jar build exited with status $EXIT_CODE"
    exit $EXIT_CODE
fi
