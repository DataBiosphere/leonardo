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
   -gr | --gcr-registry: The GCR registry to push to
   -t | --tag: (default: git banch name) the docker tag used for the images.
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
REMOTE=$(if [[ ${GIT_BRANCH} == "update/"* ]]; then echo "scalaSteward"; else echo "origin"; fi)
DOCKER_REGISTRY="dockerhub"  # Must be either "dockerhub" or "gcr"
BUILD_UI=false
DOCKER_CMD=""
DOCKER_TAG=""
DOCKER_TAG_TESTS=""
ENV=${ENV:-""}  # if env is not set, push an image with branch name
SERVICE_ACCOUNT_KEY_FILE=""  # default to no service account
REGEX_TO_REPLACE_ILLEGAL_CHARACTERS_WITH_DASHES="s/[^a-zA-Z0-9_.\-]/-/g"
REGEX_TO_REMOVE_DASHES_AND_PERIODS_FROM_BEGINNING="s/^[.\-]*//g"
DOCKERTAG_SAFE_NAME=$(echo $BRANCH | sed -e $REGEX_TO_REPLACE_ILLEGAL_CHARACTERS_WITH_DASHES -e $REGEX_TO_REMOVE_DASHES_AND_PERIODS_FROM_BEGINNING | cut -c 1-127) # https://docs.docker.com/engine/reference/commandline/tag/#:~:text=A%20tag%20name%20must%20be,a%20maximum%20of%20128%20characters.

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
        -gr | --gcr-registry)
            shift
            echo "gcr registry == $1"
            GCR_REGISTRY=$1
            ;;
        -t | --tag)
            shift
            echo "docker tag == $1"
            DOCKER_TAG=$1
            ;;
        -p | --project)
            shift
            echo "project == $1"
            DOCKER_PROJECT=$1
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

DOCKER_REMOTES_BINARY="docker"
GCR_REMOTES_BINARY="gcloud docker --"
NOTEBOOK_REPO="${NOTEBOOK_REPO:-us.gcr.io/broad-dsp-gcr-public}"
DEFAULT_IMAGE="broadinstitute/$TARGET"
GCR_IMAGE="${GCR_REGISTRY}/$TARGET"
TESTS_IMAGE=$DEFAULT_IMAGE-tests

# Run gcloud auth if a service account key file was specified.
if [[ -n "$SERVICE_ACCOUNT_KEY_FILE" ]]; then
  TMP_DIR=$(mktemp -d tmp-XXXXXX)
  export CLOUDSDK_CONFIG=$(pwd)/${TMP_DIR}
  gcloud auth activate-service-account --key-file="${SERVICE_ACCOUNT_KEY_FILE}"
fi

function make_jar()
{
    echo "building jar..."
    docker version
    # start test db
    bash ./docker/run-mysql.sh start ${TARGET} ${DB_CONTAINER}

    # Get the last commit hash and set it as an environment variable
    GIT_HASH=$(git log -n 1 --pretty=format:%h)

    # Make jar & cache sbt dependencies.
    EXIT_CODE=0
    docker run --rm --link $DB_CONTAINER:mysql \
                          -e GIT_HASH=$GIT_HASH \
                          -v $PWD:/working \
                          -v jar-cache:/home/vsts/.ivy \
                          -v jar-cache:/home/vsts/.ivy2 \
                          sbtscala/scala-sbt:openjdk-17.0.2_1.8.0_2.13.10 \
                          /working/docker/install.sh /working || EXIT_CODE=$?

    # stop test db
    bash ./docker/run-mysql.sh stop ${TARGET} ${DB_CONTAINER}

    if [ $EXIT_CODE != 0 ]; then
        echo "Tests/jar build exited with status $EXIT_CODE"
        exit $EXIT_CODE
    fi
}


function docker_cmd()
{
    if [ $DOCKER_CMD = "build" ] || [ $DOCKER_CMD = "push" ]; then
        echo "building $TARGET docker image..."
        GIT_SHA=$(git rev-parse origin/${GIT_BRANCH})
        echo GIT_SHA=$GIT_SHA > env.properties

        if [ -n "$DOCKER_TAG" ]; then
            DOCKER_TAG_TESTS="${DOCKER_TAG}-tests"
        else
            DOCKER_TAG=${GIT_SHA:0:12}
            DOCKER_TAG_TESTS=${GIT_SHA:0:12}
        fi

        docker build -t "${DEFAULT_IMAGE}:${DOCKER_TAG}" .

        echo "building $TESTS_IMAGE docker image..."
        docker build -f Dockerfile-tests -t "${TESTS_IMAGE}:${DOCKER_TAG_TESTS}" .

        if [ $DOCKER_CMD = "push" ]; then
            if [ -n "$GCR_REGISTRY" ]; then
                echo "pushing $GCR_IMAGE docker image..."
                $DOCKER_REMOTES_BINARY tag $DEFAULT_IMAGE:${DOCKER_TAG} ${GCR_IMAGE}:${DOCKER_TAG}
                $GCR_REMOTES_BINARY push ${GCR_IMAGE}:${DOCKER_TAG}
                $DOCKER_REMOTES_BINARY tag $DEFAULT_IMAGE:${DOCKER_TAG} ${GCR_IMAGE}:${DOCKERTAG_SAFE_NAME}
                $GCR_REMOTES_BINARY push ${GCR_IMAGE}:${DOCKERTAG_SAFE_NAME}
            fi

            # Push tests image no matter what. Currently this is only supported in Dockerhub.
            echo "pushing $TESTS_IMAGE docker image..."
            $DOCKER_REMOTES_BINARY push $TESTS_IMAGE:${DOCKER_TAG_TESTS}
            $DOCKER_REMOTES_BINARY tag $TESTS_IMAGE:${DOCKER_TAG_TESTS} $TESTS_IMAGE:${DOCKERTAG_SAFE_NAME}
            $DOCKER_REMOTES_BINARY push $TESTS_IMAGE:${DOCKERTAG_SAFE_NAME}
        fi
    else
        echo "Not a valid docker option!  Choose either build or push (which includes build)"
    fi
}

function cleanup()
{
    echo "cleaning up..."
    if [[ -n "$SERVICE_ACCOUNT_KEY_FILE" ]]; then
      gcloud auth revoke && echo 'Token revoke succeeded' || echo 'Token revoke failed -- skipping'
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
