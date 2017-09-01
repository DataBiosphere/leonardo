#!/usr/bin/env bash

set -e -x

# adapted from https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/blob/master/datalab/datalab.sh

# init a Dataproc cluster with a docker image

# The following values are populated by Leo when a cluster is created.
export CLUSTER_NAME=$(clusterName)
export GOOGLE_PROJECT=$(googleProject)
export JUPYTER_SERVER_NAME=$(jupyterServerName)
export PROXY_SERVER_NAME=$(proxyServerName)
export JUPYTER_DOCKER_IMAGE=$(jupyterDockerImage)
export PROXY_DOCKER_IMAGE=$(proxyDockerImage)
export COMPOSE_API_VERSION=1.18   # This is set because docker and docker-compose don't work together otherwise

JUPYTER_SERVER_CRT=$(jupyterServerCrt)
JUPYTER_SERVER_KEY=$(jupyterServerKey)
JUPYTER_ROOT_CA=$(rootCaPem)
JUPYTER_DOCKER_COMPOSE=$(jupyterDockerCompose)

apt-get update
apt-get install -y -q docker.io
mkdir /work
mkdir /certs
chmod a+wx /work

# Add the certificates from the bucket to the VM. They are used by the docker-compose file
gsutil cp ${JUPYTER_SERVER_CRT} /certs
gsutil cp ${JUPYTER_SERVER_KEY} /certs
gsutil cp ${JUPYTER_ROOT_CA} /certs
gsutil cp ${JUPYTER_DOCKER_COMPOSE} /etc

# Install docker-compose
curl -L https://github.com/docker/compose/releases/download/1.15.0/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

touch /hadoop_gcs_connector_metadata_cache

docker-compose -f /etc/cluster-docker-compose.yaml up -d