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


build_static_app() {
    echo "Compiling javascript to static application..."
    docker run \
        -v "${PWD}:/build" \
        -w "/build" \
        node:9-slim \
        bash -c "npm install && npm run build"
}

create_docker_image() {
    $DOCKER_BINARY build \
        --tag "${DOCKER_REPOSITORY}/leonardo-ui:${JUPYTER_TAG}" \
        --file ./Dockerfile \
        .
}

push() {
    echo "pushing jupyter docker image..."
    $DOCKER_BINARY push "${DOCKER_REPOSITORY}/leonardo-ui:${JUPYTER_TAG}"
}

echo "${JUPYTER_COMMAND}ing the jupyter docker image"
if [[ $JUPYTER_COMMAND == "build" ]]; then
    build_static_app
    create_docker_image
elif [[ $JUPYTER_COMMAND == "push" ]]; then
    push
else
    exit 1
fi