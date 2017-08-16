#!/bin/bash

#DOCKER_IMG=ansingh7115/leonardo-notebooks
DOCKER_IMG=jupyter/minimal-notebook
CONTAINER=jupyter-server

start () {
    # check if jupyter is running
    RUNNING=$(docker inspect -f {{.State.Running}} $CONTAINER 2> /dev/null || echo "false")

    if $RUNNING; then
        stop
    fi

    echo "Starting Jupyter server container..."
    docker create -it --name jupyter-server --rm -p 8000:8000 $DOCKER_IMG
    docker cp jupyter-docker/jupyter_notebook_config-test.py jupyter-server:/etc/jupyter/jupyter_notebook_config.py
    docker start jupyter-server

    sleep 5

    echo $(docker logs jupyter-server | grep token)
}

stop() {
    echo "Stopping docker $CONTAINER container..."
    docker stop $CONTAINER 2> /dev/null || echo "jupyter-server stop failed. Container already stopped."
    docker rm -v $CONTAINER 2> /dev/null || echo "jupyter-server rm -v failed. Container already destroyed."
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
