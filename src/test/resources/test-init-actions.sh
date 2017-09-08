#!/usr/bin/env bash

CLUSTER_NAME=$(clusterName)
GOOGLE_PROJECT=$(googleProject)
JUPYTER_SERVER_NAME=$(jupyterServerName)
PROXY_SERVER_NAME=$(proxyServerName)
JUPYTER_DOCKER_IMAGE=$(jupyterDockerImage)
PROXY_DOCKER_IMAGE=$(proxyDockerImage)
JUPYTER_SERVER_CRT=$(jupyterServerCrt)
JUPYTER_SERVER_KEY=$(jupyterServerKey)
JUPYTER_ROOT_CA=$(rootCaPem)
JUPYTER_DOCKER_COMPOSE=$(jupyterDockerCompose)
JUPYTER_PROXY_SITE_CONF=$(jupyterProxySiteConf)