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
	exit $EXIT_CODE
    fi
}


function docker_cmd()
{
    if [ $DOCKER_CMD = "build" ] || [ $DOCKER_CMD = "push" ]; then
        echo "building leonardo docker image..."
        GIT_SHA=$(git rev-parse ${GIT_BRANCH})
        echo GIT_SHA=$GIT_SHA > env.properties  # for jenkins jobs
        docker build -t $REPO:${GIT_SHA:0:12} .

        # builds the juptyer notebooks docker image that goes on dataproc clusters
        bash ./jupyter-docker/build.sh build $JUPYTER_REPO:${GIT_SHA:0:12}

        if [ $DOCKER_CMD = "push" ]; then
            echo "pushing leonardo docker image..."
            docker push $REPO:${GIT_SHA:0:12}

            # pushes the juptyer notebooks docker image that goes on dataproc clusters
            bash ./jupyter-docker/build.sh push $JUPYTER_REPO:${GIT_SHA:0:12}
        fi
    else
        echo "Not a valid docker option!  Choose either build or push (which includes build)"
    fi
}

# parse command line options
DOCKER_CMD=
GIT_BRANCH=${GIT_BRANCH:-$(git rev-parse --abbrev-ref HEAD)}  # default to current branch
REPO=${REPO:-broadinstitute/$PROJECT}  # default to leonardo docker repo
JUPYTER_REPO=${JUPYTER_REPO:-broadinstitute/$PROJECT-notebooks}

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
