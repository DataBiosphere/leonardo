#!/usr/bin/env bash

set -e -x

##
# This is a shutdown script designed to run on Leo-created Dataproc clusters.
##

# Templated values
export RSTUDIO_DOCKER_IMAGE=$(rstudioDockerImage)
export RSTUDIO_SERVER_NAME=$(rstudioServerName)
export SHOULD_DELETE_JUPYTER_DIR=$(shouldDeleteJupyterDir)

# If RStudio is installed, cleanly shut it down
if [ ! -z "$RSTUDIO_DOCKER_IMAGE" ] ; then
    docker exec -u rstudio -i $RSTUDIO_SERVER_NAME rstudio-server stop
fi

if [ -d '/mnt/disks/work/.jupyter' ] && [ "SHOULD_DELETE_JUPYTER_DIR" = "true" ] ; then
    rm -rf /mnt/disks/work/.jupyter
fi