#!/bin/bash

DOCKER_IMG=jupyter/minimal-notebook
CONTAINER=jupyter-server

start () {
    # check if jupyter is running
    RUNNING=$(docker inspect -f {{.State.Running}} $CONTAINER 2> /dev/null || echo "false")

    if $RUNNING; then
        stop
    fi

    echo "Starting Jupyter server container..."
    docker create -it --name ${CONTAINER} --rm -p 8001:8001 $DOCKER_IMG 'export PYTHONPATH=$PYTHONPATH:/etc/jupyter/custom;start-notebook.sh'
    docker cp jupyter-docker/jupyter_notebook_config.py ${CONTAINER}:/etc/jupyter/jupyter_notebook_config.py
    docker cp jupyter-docker/jupyter_localize_extension.py ${CONTAINER}:/etc/jupyter/custom/jupyter_localize_extension.py
    docker start ${CONTAINER}

    sleep 5

    echo $(docker logs ${CONTAINER} | grep token)
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
