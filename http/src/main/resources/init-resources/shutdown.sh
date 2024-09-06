#!/usr/bin/env bash

set -e -x

##
# This is a shutdown script designed to run on Leo-created Google Dataproc clusters and Google Compute Engines (GCE).
##

# The CLOUD_SERVICE is assumed based on the location of the certs directory
if [ -f "/var/certs/jupyter-server.crt" ]
then
  export CLOUD_SERVICE='GCE'
else
  export CLOUD_SERVICE='DATAPROC'
fi

# Set variables
# Values like $(..) are populated by Leo when a cluster is resumed.
# See https://github.com/DataBiosphere/leonardo/blob/e46acfcb409b11198b1f12533cefea3f6c7fdafb/http/src/main/scala/org/broadinstitute/dsde/workbench/leonardo/util/RuntimeTemplateValues.scala#L192
# Avoid exporting variables unless they are needed by external scripts or docker-compose files.
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

if [[ "${CLOUD_SERVICE}" == 'GCE' ]]; then
  # COS images need to run docker-compose as a container by design
  DOCKER_COMPOSE='docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v /var:/var docker/compose:1.29.2'
else
  # Dataproc has docker-compose natively installed
  DOCKER_COMPOSE='docker-compose'
fi

$DOCKER_COMPOSE down