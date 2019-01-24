#!/bin/bash
#
# To rebuild the docker image, run:
#   docker build docker/jupyter/ -t broadinstitute/leonardo-jupyter:local
set -e

DOCKER_IMG=broadinstitute/leonardo-jupyter:local
CONTAINER=jupyter-server

start () {
    # check if jupyter is running
    RUNNING=$(docker inspect -f {{.State.Running}} $CONTAINER 2> /dev/null || echo "false")

    if $RUNNING; then
        stop
    fi

    echo "Starting Jupyter server container..."
    docker create -it --rm --name ${CONTAINER} -p 8000:8000 -e GOOGLE_PROJECT=project -e CLUSTER_NAME=cluster "${DOCKER_IMG}"

    # Substitute templated vars in the notebook config.
    local tmp_config=$(mktemp notebook_config.XXXX)
    cp src/main/resources/jupyter/jupyter_notebook_config.py ${tmp_config}
    sed -i '' 's/$(contentSecurityPolicy)/""/' ${tmp_config}
    chmod a+rw ${tmp_config}
    docker cp ${tmp_config} ${CONTAINER}:/etc/jupyter/jupyter_notebook_config.py
    rm ${tmp_config}

    docker cp docker/jupyter/custom/jupyter_localize_extension.py ${CONTAINER}:/etc/jupyter/custom/jupyter_localize_extension.py
    docker cp docker/jupyter/custom/jupyter_delocalize.py ${CONTAINER}:/etc/jupyter/custom/jupyter_delocalize.py
    # To debug startup failures, add -a here to attach.
    docker start -a ${CONTAINER}

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
