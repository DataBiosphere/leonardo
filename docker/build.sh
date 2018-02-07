#!/bin/bash

# Single source of truth for building Leonardo.
# @ Jackie Roberti
#
# Provide command line options to do one or several things:
#   jar : build leonardo jar
#   -d | --docker : provide arg either "build" or "push", to build and push docker image
# Jenkins build job should run with all options, for example,
#   ./docker/build.sh jar -d push

set -ex
PROJECT=leonardo

function make_jar()
{
    echo "building jar..."
    # start test db
    bash ./docker/run-mysql.sh start ${PROJECT}

    # Get the last commit hash and set it as an environment variable
    GIT_HASH=$(git log -n 1 --pretty=format:%h)

    # make jar.  cache sbt dependencies.
    JAR_CMD=`docker run --rm --link mysql:mysql -e GIT_HASH=$GIT_HASH -v $PWD:/working -v jar-cache:/root/.ivy -v jar-cache:/root/.ivy2 broadinstitute/scala-baseimage /working/docker/install.sh /working`
    EXIT_CODE=$?
 
    # stop test db
    bash ./docker/run-mysql.sh stop ${PROJECT}

    if [ $EXIT_CODE != 0 ]; then
        echo "Tests/jar build exited with status $EXIT_CODE"
        exit $EXIT_CODE
	exit $EXIT_CODE
    fi
}


function docker_cmd()
{
    if [ $DOCKER_CMD = "build" ] || [ $DOCKER_CMD = "push" ]; then
        echo "building $PROJECT docker image..."
        if [ "$ENV" != "dev" ] && [ "$ENV" != "alpha" ] && [ "$ENV" != "staging" ] && [ "$ENV" != "perf" ]; then
            DOCKER_TAG=${BRANCH}
            DOCKER_TAG_TESTS=${BRANCH}
        else
            GIT_SHA=$(git rev-parse origin/${BRANCH})
            echo GIT_SHA=$GIT_SHA > env.properties
            DOCKER_TAG=${GIT_SHA:0:12}
            DOCKER_TAG_TESTS=latest
        fi

        # builds the juptyer notebooks docker image that goes on dataproc clusters
        bash ./jupyter-docker/build.sh build ${DOCKER_TAG}

        echo "building $PROJECT-tests docker image..."
        docker build -t $REPO:${DOCKER_TAG} .
        cd automation
        docker build -f Dockerfile-tests -t $TESTS_REPO:${DOCKER_TAG_TESTS} .
        cd ..

        if [ $DOCKER_CMD = "push" ]; then
            echo "pushing $PROJECT docker image..."
            docker push $REPO:${DOCKER_TAG}
            echo "pushing $PROJECT-tests docker image..."
            docker push $TESTS_REPO:${DOCKER_TAG_TESTS}
            # pushes the juptyer notebooks docker image that goes on dataproc clusters
            bash ./jupyter-docker/build.sh push ${DOCKER_TAG}
        fi
    else
        echo "Not a valid docker option!  Choose either build or push (which includes build)"
    fi
}

# parse command line options
DOCKER_CMD=
BRANCH=${BRANCH:-$(git rev-parse --abbrev-ref HEAD)}  # default to current branch
REPO=${REPO:-broadinstitute/$PROJECT}
TESTS_REPO=$REPO-tests
ENV=${ENV:-""}  # if env is not set, push an image with branch name

if [ -z "$1" ]; then
    echo "No argument supplied!  Available choices are jar to build the jar and -d followed by a docker option (build or push)"
fi

while [ "$1" != "" ]; do
    case $1 in
        jar) make_jar ;;
        -d | --docker) shift
                       echo $1
                       DOCKER_CMD=$1
                       docker_cmd
                       ;;
    esac
    shift
done
