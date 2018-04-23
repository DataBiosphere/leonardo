#!/usr/bin/env bash

# Note: in most cases this script should be invoked by
# the build script in "leonardo/docker/build.sh".

# Ensure that commands are run from this directory.
cd "$(dirname "${0}")"

# Get command line options.
JUPYTER_COMMAND="${1}"
DOCKER_REPOSITORY="${2}"
JUPYTER_TAG="${3}"

# Set up docker binary - use gcloud docker if pushing to gcr.
DOCKER_BINARY="docker"
if grep -Fq "gcr.io" <<< "${DOCKER_REPOSITORY}" ; then
	DOCKER_BINARY="gcloud docker --"
fi

build() {
    echo "building jupyter docker images..."
    $DOCKER_BINARY build \
        --tag "${DOCKER_REPOSITORY}/leonardo-notebooks-base:${JUPYTER_TAG}" \
        --file ./Dockerfile.base \
        .
    $DOCKER_BINARY build \
        --build-arg BASE_IMAGE="${DOCKER_REPOSITORY}/leonardo-notebooks-base:${JUPYTER_TAG}" \
        --tag "${DOCKER_REPOSITORY}/leonardo-notebooks:${JUPYTER_TAG}" \
        --file ./Dockerfile.final \
        .
}

push() {
    echo "pushing jupyter docker image..."
    $DOCKER_BINARY push "${DOCKER_REPOSITORY}/leonardo-notebooks-base:${JUPYTER_TAG}"
    $DOCKER_BINARY push "${DOCKER_REPOSITORY}/leonardo-notebooks:${JUPYTER_TAG}"
}

echo "${JUPYTER_COMMAND}ing the jupyter docker image"
if [[ $JUPYTER_COMMAND == "build" ]]; then
    build
elif [[ $JUPYTER_COMMAND == "push" ]]; then
    push
else
    exit 1
fi