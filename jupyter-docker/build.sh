#!/usr/bin/env bash

# Note: in most cases this script should be invoked by
# the build script in "leonardo/docker/build.sh".

# Ensure that commands are run from this directory.
cd "$(dirname "${0}")"

# Get command line options.
JUPYTER_COMMAND="${1}"
DOCKER_REPOSITORY="${2}"
LEO_BRANCH="${3}"
HASH_TAG="${4}"


# Set up docker binary - use gcloud docker if pushing to gcr.
DOCKER_BINARY="docker"
if grep -Fq "gcr.io" <<< "${DOCKER_REPOSITORY}" ; then
	DOCKER_BINARY="gcloud docker --"
fi

build() {
    echo "building jupyter docker image..."
    $DOCKER_BINARY build -t "${DOCKER_REPOSITORY}/leonardo-notebooks:${LEO_BRANCH}" .
}

push() {
    echo "pushing jupyter docker image..."
    $DOCKER_BINARY push "${DOCKER_REPOSITORY}/leonardo-notebooks:${LEO_BRANCH}"
    $DOCKER_BINARY tag "${DOCKER_REPOSITORY}/leonardo-notebooks:${LEO_BRANCH}" "${DOCKER_REPOSITORY}/leonardo-notebooks:${HASH_TAG}"
    $DOCKER_BINARY push "${DOCKER_REPOSITORY}/leonardo-notebooks:${HASH_TAG}"
}

echo "${JUPYTER_COMMAND}ing the jupyter docker image"
if [[ $JUPYTER_COMMAND == "build" ]]; then
    build
elif [[ $JUPYTER_COMMAND == "push" ]]; then
    push
else
    exit 1
fi
