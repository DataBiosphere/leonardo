#!/bin/bash

# Ensure that commands are run from this directory.
cd "$(dirname "${0}")"

HELP_TEXT="$(cat <<EOF

 Builds a tool docker image.
   -p | --push : (default: only build) if set, pushes the image after build.
           If unset, only perform a build.
   -i | --image : the tool image name.
   -r | --repository : the docker repository to push to.  Can be a dockerhub or GCR repo.
   -t | --tag : the docker tag used for the images.
   -b | --branch : (default: none) the git branch
   -h | --help: print help text.

 Examples:
   Builds and pushes the Jupyter docker image
    ./docker/build-tool.sh --push -i jupyter -r us.gcr.io/broad-dsp-gcr-public -t 12345 -b my-branch
   Builds the RStudio docker image tagged with the git branch name
    ./docker/build-tool.sh -i rstudio -r us.gcr.io/broad-dsp-gcr-public -t my-tag
\t
EOF
)"

# Enable strict evaluation semantics.
set -e

# Set default variables used while parsing command line options.
PRINT_HELP=false
PUSH=false
IMAGE=""
REPOSITORY=""
DOCKER_TAG=""
GIT_BRANCH=""

if [ -z "$1" ]; then
    echo "No argument supplied!"
    echo "run '${0} -h' to see available arguments."
    exit 1
fi

while [ "$1" != "" ]; do
    case $1 in
        -p | --push)
            echo "will perform a PUSH"
            PUSH=true
            ;;
        -i | --image)
            shift
            IMAGE=$1
            ;;
        -r | --repository)
            shift
            REPOSITORY=$1
            ;;
        -t | --tag)
            shift
            DOCKER_TAG=$1
            ;;
        -b | --branch)
            shift
            GIT_BRANCH=$1
            ;;
        -h | --help)
            PRINT_HELP=true
            ;;
        *)
            echo "Unrecognized argument '${1}'."
            echo "run '${0} -h' to see available arguments."
            if grep -Fq "=" <<< "${1}"; then
                echo "note: separate args from flags with a space, not '='."
            fi
            exit 1
            ;;
    esac
    shift
done

if [ -z "$IMAGE" ]; then
    echo "image is required"
    PRINT_HELP=true
fi

if [ -z "$REPOSITORY" ]; then
    echo "repository is required"
    PRINT_HELP=true
fi

if [ -z "$REPOSITORY" ]; then
    echo "tag is required"
    PRINT_HELP=true
fi

# Print help after all flags are parsed successfully
if $PRINT_HELP; then
  echo -e "${HELP_TEXT}"
  exit 0
fi

# Set up docker binary - use gcloud docker if pushing to gcr.
DOCKER_BINARY="docker"
if grep -Fq "gcr.io" <<< "${REPOSITORY}" ; then
	DOCKER_BINARY="gcloud docker --"
fi


function build() {
    echo "building leonardo-$IMAGE docker image..."
    $DOCKER_BINARY build -t "${REPOSITORY}/leonardo-${IMAGE}:${DOCKER_TAG}" $IMAGE
}

function push() {
    echo "pushing leonardo-$IMAGE docker image..."
    $DOCKER_BINARY push "${REPOSITORY}/leonardo-${IMAGE}:${DOCKER_TAG}"
    $DOCKER_BINARY tag "${REPOSITORY}/leonardo-${IMAGE}:${DOCKER_TAG}" "${REPOSITORY}/leonardo-${IMAGE}:${GIT_BRANCH}"
    $DOCKER_BINARY push "${REPOSITORY}/leonardo-${IMAGE}:${GIT_BRANCH}"
}

build
if $PUSH; then
    push
fi
