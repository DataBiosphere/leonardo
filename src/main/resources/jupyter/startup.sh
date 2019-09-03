#!/usr/bin/env bash

set -e -x

##
# This is a startup script designed to run on Leo-created Dataproc clusters.
#
# It starts up Jupyter and Welder processes. It also optionally deploys welder on a
# cluster if not already installed.
##

# Templated values
export GOOGLE_PROJECT=$(googleProject)
export CLUSTER_NAME=$(clusterName)
export OWNER_EMAIL=$(loginHint)
export JUPYTER_SERVER_NAME=$(jupyterServerName)
export WELDER_SERVER_NAME=$(welderServerName)
export NOTEBOOKS_DIR=$(notebooksDir)
export WELDER_ENABLED=$(welderEnabled)
export DEPLOY_WELDER=$(deployWelder)
export UPDATE_WELDER=$(updateWelder)
export WELDER_DOCKER_IMAGE=$(welderDockerImage)

if [ "$DEPLOY_WELDER" == "true" ] ; then
    echo "Deploying Welder on cluster $GOOGLE_PROJECT / $CLUSTER_NAME..."

    # Run welder-docker-compose
    gcloud auth configure-docker
    docker-compose -f /etc/welder-docker-compose.yaml up -d

    # Set EVs inside Jupyter container necessary for welder
    docker exec -it $JUPYTER_SERVER_NAME bash -c "echo $'export WELDER_ENABLED=true\nexport NOTEBOOKS_DIR=$NOTEBOOKS_DIR' >> /home/jupyter-user/.bashrc"

    # Move existing notebooks to new notebooks dir
    docker exec -it $JUPYTER_SERVER_NAME bash -c "ls -I jupyter.log -I localization.log -I notebooks /home/jupyter-user | xargs -d '\n'  -I file mv file $NOTEBOOKS_DIR"
fi

if [ "$UPDATE_WELDER" == "true" ] ; then
    # Run welder-docker-compose
    gcloud auth configure-docker
    docker-compose -f /etc/welder-docker-compose.yaml stop
    docker-compose -f /etc/welder-docker-compose.yaml rm -f
    docker-compose -f /etc/welder-docker-compose.yaml up -d
fi

# Start Jupyter
echo "Starting Jupyter on cluster $GOOGLE_PROJECT / $CLUSTER_NAME..."
docker exec -d $JUPYTER_SERVER_NAME /bin/bash -c "/etc/jupyter/scripts/run-jupyter.sh $NOTEBOOKS_DIR || /usr/local/bin/jupyter notebook"

# Start welder, if enabled
if [ "$WELDER_ENABLED" == "true" ] ; then
    echo "Starting Welder on cluster $GOOGLE_PROJECT / $CLUSTER_NAME..."
    docker exec -d $WELDER_SERVER_NAME /opt/docker/bin/entrypoint.sh
fi