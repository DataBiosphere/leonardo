#!/bin/bash

# Single source of truth for building Leonardo.
# @ Jackie Roberti
#
# Provide command line options to do one or several things:
#   jar : build leonardo jar
#   -d | --docker : provide arg either "build" or "push", to build and push docker image
# Jenkins build job should run with all options, for example,
#   ./docker/build.sh jar -d push

HELP_TEXT="$(cat <<EOF

 Build the Leonardo code and docker images.
   jar : build Leonardo jar
   -d | --docker : (default: no action) provide either "build" or "push" to
           build or push a docker image.  "push" will also perform build.
   -r | --registry: (default: dockerhub) can be either "dockerhub" or "gcr".
           Users of gcr should have the gcloud tool installed and configured.
   -p | --project: set the project used at either dockerhub or with gcr
           container registries.
   -n | --notebook-repo: (default: --project) the repo to push the notebooks
           image. Can be a dockerhub or GCR repo.
   -k | --service-account-key-file: (optional) path to a service account key json
           file. If set, the script will call "gcloud auth activate-service-account".
           Otherwise, the script will not authenticate with gcloud.
   -h | --help: print help text.

 Examples:
   Jenkins build job should run with all options, for example,
     ./docker/build.sh jar -d push
   To build the jar, the image, and push it to a gcr repository.
     ./docker/build.sh jar -d build -r gcr --project "my-awesome-project"
\t
EOF
)"

# Enable strict evaluation semantics.
set -e

# Set default variables used while parsing command line options.
TARGET="${TARGET:-leonardo}"
DB_CONTAINER="leonardo-mysql"
GIT_BRANCH="${BRANCH:-$(git rev-parse --abbrev-ref HEAD)}"
DOCKER_REGISTRY="dockerhub"  # Must be either "dockerhub" or "gcr"
DOCKER_CMD=""
ENV=${ENV:-""}  # if env is not set, push an image with branch name

MAKE_JAR=false
RUN_DOCKER=false
PRINT_HELP=false


if [ -z "$1" ]; then
    echo "No argument supplied!"
    echo "run '${0} -h' to see available arguments."
    exit 1
fi

while [ "$1" != "" ]; do
    case $1 in
        jar)
            MAKE_JAR=true
            ;;
        -d | --docker)
            shift
            echo "docker command = $1"
            RUN_DOCKER=true
            DOCKER_CMD="$1"
            ;;
        -r | --registry)
            shift
            echo "registry == $1"
            DOCKER_REGISTRY=$1
            ;;
        -p | --project)
            shift
            echo "project == $1"
            DOCKER_PROJECT=$1
            ;;
        -n | --notebook-repo)
            shift
            echo "notebook-repo == $1"
            NOTEBOOK_REPO=$1
            ;;
        -k | --service-account-key-file)
            shift
            echo "service-account-key-file == $1"
            SERVICE_ACCOUNT_KEY_FILE=$1
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

# Print help after all flags are parsed successfully
if $PRINT_HELP; then
  echo -e "${HELP_TEXT}"
  exit 0
fi

# Configure script using arguments.
if [[ $DOCKER_REGISTRY == "dockerhub" ]]; then
  DOCKER_PROJECT="${DOCKER_PROJECT:-broadinstitute}"
  REPO="${DOCKER_PROJECT}"
  IMAGE="${REPO}/${TARGET}"
  DOCKER_REMOTES_BINARY="docker"
  NOTEBOOK_REPO="${NOTEBOOK_REPO:-$REPO}"
elif [[ $DOCKER_REGISTRY == "gcr" ]]; then
  DOCKER_PROJECT="${DOCKER_PROJECT:-$(gcloud config get-value project)}"
  # Domain scoped project IDs need to be modified to work with GCR.
  REPO="gcr.io/$(sed "s_:_/_" <<< "${DOCKER_PROJECT}")"
  IMAGE="${REPO}/${TARGET}"
  DOCKER_REMOTES_BINARY="gcloud docker --"
  NOTEBOOK_REPO="${NOTEBOOK_REPO:-$REPO}"
else
  echo "The docker registry must be either 'dockerhub' or 'gcr'"
  echo "Provided value: ${DOCKER_REGISTRY} is not allowed."
  exit 1
fi

TESTS_IMAGE=$IMAGE-tests

# Run gcloud auth if a service account key file was specified.
if [[ -n $SERVICE_ACCOUNT_KEY_FILE ]]; then
  TMP_DIR=$(mktemp -d tmp-XXXXXX)
  export CLOUDSDK_CONFIG=$(pwd)/${TMP_DIR}
  gcloud auth activate-service-account --key-file="${SERVICE_ACCOUNT_KEY_FILE}"
fi

function make_jar()
{
    echo "building jar..."
    # start test db
    bash ./docker/run-mysql.sh start ${TARGET} ${DB_CONTAINER}

    # Get the last commit hash and set it as an environment variable
    GIT_HASH=$(git log -n 1 --pretty=format:%h)

    # Make jar & cache sbt dependencies.
    JAR_CMD="$(docker run --rm --link $DB_CONTAINER:mysql \
                          -e GIT_HASH=$GIT_HASH \
                          -v $PWD:/working \
                          -v jar-cache:/root/.ivy \
                          -v jar-cache:/root/.ivy2 \
                          broadinstitute/scala-baseimage \
                          /working/docker/install.sh /working)"
    EXIT_CODE=$?

    # stop test db
    bash ./docker/run-mysql.sh stop ${TARGET} ${DB_CONTAINER}

    if [ $EXIT_CODE != 0 ]; then
        echo "Tests/jar build exited with status $EXIT_CODE"
        exit $EXIT_CODE
    fi
}


function docker_cmd()
{
    echo "running docker_cmd with $DOCKER_CMD"
    if [ $DOCKER_CMD = "build" ] || [ $DOCKER_CMD = "push" ]; then
        echo "building $IMAGE docker image..."
        if [ "$ENV" != "dev" ] && [ "$ENV" != "alpha" ] && [ "$ENV" != "staging" ] && [ "$ENV" != "perf" ]; then
            DOCKER_TAG=${GIT_BRANCH}
            DOCKER_TAG_TESTS=${GIT_BRANCH}
        else
            GIT_SHA=$(git rev-parse origin/${GIT_BRANCH})
            echo GIT_SHA=$GIT_SHA > env.properties
            DOCKER_TAG=${GIT_SHA:0:12}
            DOCKER_TAG_TESTS=${GIT_SHA:0:12}
        fi

        # builds the juptyer notebooks docker image that goes on dataproc clusters
        bash ./jupyter-docker/build.sh build "${NOTEBOOK_REPO}" "${DOCKER_TAG}"

        docker build -t "${IMAGE}:${DOCKER_TAG}" .
        cd automation
        echo "building $TESTS_IMAGE docker image..."
        docker build -f Dockerfile-tests -t "${TESTS_IMAGE}:${DOCKER_TAG_TESTS}" .
        cd ..

        if [ $DOCKER_CMD = "push" ]; then
            echo "pushing $IMAGE docker image..."
            $DOCKER_REMOTES_BINARY push $IMAGE:${DOCKER_TAG}
            echo "pushing $TESTS_IMAGE docker image..."
            $DOCKER_REMOTES_BINARY push $TESTS_IMAGE:${DOCKER_TAG_TESTS}
            # pushes the juptyer notebooks docker image that goes on dataproc clusters
            bash ./jupyter-docker/build.sh push "${NOTEBOOK_REPO}" "${DOCKER_TAG}"
        fi
    else
        echo "Not a valid docker option!  Choose either build or push (which includes build)"
    fi
}

function cleanup()
{
    echo "cleaning up..."
    if [[ -n $SERVICE_ACCOUNT_KEY_FILE ]]; then
      gcloud auth revoke
      rm -rf ${CLOUDSDK_CONFIG}
    fi
}

if $MAKE_JAR; then
  make_jar
fi

if $RUN_DOCKER; then
  docker_cmd
fi

cleanup
