#!/bin/bash
set -e

# This is to avoid the error Ref BioC
# 'debconf: unable to initialize frontend: Dialog'
export DEBIAN_FRONTEND=noninteractive

## Update apt-get
apt-get update

apt-get install -y --no-install-recommends apt-utils

# Define environment variables for Jupyter Server customization

SERVER_APP_PORT=8080
SERVER_APP_TOKEN=''
SERVER_APP_IP=''
SERVER_APP_CERTFILE=''
SERVER_APP_KEYFILE=''

# Variables for listener
SERVER_APP_BASE_URL=/$2/
SERVER_APP_ALLOW_ORIGIN='*'
SERVER_APP_WEBSOCKET_URL="wss://$1.servicebus.windows.net/\$hc/$2"

# Install relevant libraries

/anaconda/envs/py38_default/bin/pip3 install igv-jupyter

/anaconda/envs/py38_default/bin/pip3 install seaborn

# Create Jupyter Server config- Optional

#/anaconda/bin/jupyter server --generate-config

# Start Jupyter server with custom parameters

/anaconda/bin/jupyter server \
--ServerApp.certfile=$SERVER_APP_CERTFILE \
--ServerApp.keyfile=$SERVER_APP_KEYFILE \
--ServerApp.port=$SERVER_APP_PORT \
--ServerApp.token=$SERVER_APP_TOKEN \
--ServerApp.ip=$SERVER_APP_IP \
--ServerApp.base_url=$SERVER_APP_BASE_URL \
--ServerApp.websocket_url=$SERVER_APP_WEBSOCKET_URL \
--ServerApp.allow_origin=$SERVER_APP_ALLOW_ORIGIN \
--autoreload --allow-root >/dev/null 2>&1&

# Store Jupyter Server parameters for reboot process

sudo crontab -l 2>/dev/null| cat - <(echo "@reboot /anaconda/bin/jupyter server --ServerApp.certfile=$SERVER_APP_CERTFILE --ServerApp.keyfile=$SERVER_APP_KEYFILE --ServerApp.port=$SERVER_APP_PORT --ServerApp.token=$SERVER_APP_TOKEN --ServerApp.ip=$SERVER_APP_IP --ServerApp.base_url=$SERVER_APP_BASE_URL --ServerApp.websocket_url=$SERVER_APP_WEBSOCKET_URL --ServerApp.allow_origin=$SERVER_APP_ALLOW_ORIGIN --autoreload --allow-root >/dev/null 2>&1&") | crontab -

# Login to ACR repo to pull the image for Relay Listener

docker login terradevacrpublic.azurecr.io -u $6 -p $7

#Run docker container with Relay Listener

docker run -d --restart always --name RelayListener \
--env LISTENER_RELAYCONNECTIONSTRING="Endpoint=sb://$1.servicebus.windows.net/;SharedAccessKeyName=$4;SharedAccessKey=$5;EntityPath=$2" \
--env LISTENER_RELAYCONNECTIONNAME="$2" \
--env LISTENER_TARGETPROPERTIES_TARGETHOST="http://$3:8080" \
terradevacrpublic.azurecr.io/terra-azure-relay-listeners:53d7992
