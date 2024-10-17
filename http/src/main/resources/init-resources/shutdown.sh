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

# Remove jupyter related files if user decides to delete the VM
if [ -d '/mnt/disks/work/.jupyter' ] && [ "SHOULD_DELETE_JUPYTER_DIR" = "true" ] ; then
    rm -rf /mnt/disks/work/.jupyter
    rm -rf /mnt/disks/work/.local || true
fi

# https://broadworkbench.atlassian.net/browse/IA-3186
# This condition assumes Dataproc's cert directory is different from GCE's cert directory, a better condition would be
# a dedicated flag that distinguishes gce and dataproc. But this will do for now
# Attempt to fix https://broadworkbench.atlassian.net/browse/IA-3258
if [ -f "/var/certs/jupyter-server.crt" ]
then
  DOCKER_COMPOSE='docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v /var:/var docker/compose:1.29.2'
else
  DOCKER_COMPOSE='docker-compose'
fi

$DOCKER_COMPOSE down