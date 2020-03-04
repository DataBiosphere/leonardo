#!/usr/bin/env bash

set -e -x

##
# This is a shutdown script designed to run on Leo-created Dataproc clusters.
##

# Templated values
export RSTUDIO_DOCKER_IMAGE=$(rstudioDockerImage)
export RSTUDIO_SERVER_NAME=$(rstudioServerName)

# If RStudio is installed, cleanly shut it down
if [ ! -z "$RSTUDIO_DOCKER_IMAGE" ] ; then
    docker exec -u root -i $RSTUDIO_SERVER_NAME rstudio-server stop
fi