#!/bin/bash
#
# To rebuild the docker image, run:
#   docker build jupyter-docker/ -t broadinstitute/leonardo-notebooks:local
set -e

DOCKER_IMG=broadinstitute/leonardo-notebooks:local
CONTAINER=jupyter-server

start () {
    # check if jupyter is running
    RUNNING=$(docker inspect -f {{.State.Running}} $CONTAINER 2> /dev/null || echo "false")

    if $RUNNING; then
        stop
    fi

    echo "Starting Jupyter server container..."
    docker create -it --rm --name ${CONTAINER} -p 8000:8000 -e GOOGLE_PROJECT=project -e CLUSTER_NAME=cluster "${DOCKER_IMG}"
    docker cp jupyter-docker/jupyter_notebook_config.py ${CONTAINER}:/etc/jupyter/jupyter_notebook_config.py
    docker cp jupyter-docker/jupyter_localize_extension.py ${CONTAINER}:/etc/jupyter/custom/jupyter_localize_extension.py
    # To debug startup failures, add -a here to attach.
    docker start ${CONTAINER}

    sleep 5
    docker logs ${CONTAINER}
}

stop() {
    echo "Stopping docker $CONTAINER container..."
    docker stop $CONTAINER 2> /dev/null || echo "${CONTAINER} stop failed. Container already stopped."
    docker rm -v $CONTAINER 2> /dev/null || echo "${CONTAINER} rm -v failed. Container already destroyed."
}

if [ ${#@} == 0 ]; then
    echo "Usage: $0 stop|start"
    exit 1
fi

COMMAND=$1
if [ $COMMAND = "start" ]; then
    start
elif [ $COMMAND = "stop" ]; then
    stop
else
    exit 1
fi
