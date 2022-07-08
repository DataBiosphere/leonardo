#!/bin/bash
set -e

# This is to avoid the error Ref BioC
# 'debconf: unable to initialize frontend: Dialog'
export DEBIAN_FRONTEND=noninteractive

#create user to run jupyter
VM_JUP_USER=jupyter

sudo useradd -m -c "Jupyter User" $VM_JUP_USER
sudo usermod -a -G $VM_JUP_USER,adm,dialout,cdrom,floppy,audio,dip,video,plugdev,lxd,netdev $VM_JUP_USER

## Change ownership for the new user

sudo chgrp $VM_JUP_USER /anaconda/bin/*

sudo chown $VM_JUP_USER /anaconda/bin/*

sudo chgrp $VM_JUP_USER /anaconda/envs/py38_default/bin/*

sudo chown $VM_JUP_USER /anaconda/envs/py38_default/bin/*

sudo systemctl disable --now jupyterhub.service

#Read script arguments

RELAY_NAME=$1
RELAY_CONNECTION_NAME=$2
RELAY_TARGET_HOST=$3
RELAY_CONNECTION_POLICY_NAME=$4
RELAY_CONNECTION_POLICY_KEY=$5
LISTENER_DOCKER_IMAGE=$6
SAMURL=$7
SAMRESOURCEID=$8
#CONTENTSECURITYPOLICY_FILE=$9
#SAMRESOURCEID="${10}"

# Define environment variables for Jupyter Server customization

SERVER_APP_PORT=8888
SERVER_APP_TOKEN=''
SERVER_APP_IP=''
SERVER_APP_CERTFILE=''
SERVER_APP_KEYFILE=''
QUIT_BUTTON_VISIBLE=false

# Jupyter variables for listener
SERVER_APP_BASE_URL="/${RELAY_CONNECTION_NAME}/"
SERVER_APP_ALLOW_ORIGIN="*"
HCVAR='\$hc'
SERVER_APP_WEBSOCKET_URL="wss://${RELAY_NAME}.servicebus.windows.net/${HCVAR}/${RELAY_CONNECTION_NAME}"

#Relay listener configuration
RELAY_CONNECTIONSTRING="Endpoint=sb://${RELAY_NAME}.servicebus.windows.net/;SharedAccessKeyName=${RELAY_CONNECTION_POLICY_NAME};SharedAccessKey=${RELAY_CONNECTION_POLICY_KEY};EntityPath=${RELAY_CONNECTION_NAME}"

# Install relevant libraries

/anaconda/envs/py38_default/bin/pip3 install igv-jupyter

/anaconda/envs/py38_default/bin/pip3 install seaborn

# Create Jupyter Server config- Optional

#/anaconda/bin/jupyter server --generate-config

# Start Jupyter server with custom parameters

sudo runuser -l $VM_JUP_USER -c "/anaconda/bin/jupyter server --ServerApp.quit_button=$QUIT_BUTTON_VISIBLE --ServerApp.certfile=$SERVER_APP_CERTFILE --ServerApp.keyfile=$SERVER_APP_KEYFILE --ServerApp.port=$SERVER_APP_PORT --ServerApp.token=$SERVER_APP_TOKEN --ServerApp.ip=$SERVER_APP_IP --ServerApp.base_url=$SERVER_APP_BASE_URL --ServerApp.websocket_url=$SERVER_APP_WEBSOCKET_URL --ServerApp.allow_origin=$SERVER_APP_ALLOW_ORIGIN --autoreload" >/dev/null 2>&1&

# Store Jupyter Server parameters for reboot process

sudo crontab -l 2>/dev/null| cat - <(echo "@reboot sudo runuser -l $VM_JUP_USER -c '/anaconda/bin/jupyter server --ServerApp.quit_button=$QUIT_BUTTON_VISIBLE --ServerApp.certfile=$SERVER_APP_CERTFILE --ServerApp.keyfile=$SERVER_APP_KEYFILE --ServerApp.port=$SERVER_APP_PORT --ServerApp.token=$SERVER_APP_TOKEN --ServerApp.ip=$SERVER_APP_IP --ServerApp.base_url=$SERVER_APP_BASE_URL --ServerApp.websocket_url=$SERVER_APP_WEBSOCKET_URL --ServerApp.allow_origin=$SERVER_APP_ALLOW_ORIGIN --autoreload' >/dev/null 2>&1&") | crontab -

#Run docker container with Relay Listener

docker run -d --restart always --network host --name listener \
--env LISTENER_RELAYCONNECTIONSTRING=$RELAY_CONNECTIONSTRING \
--env LISTENER_RELAYCONNECTIONNAME=$RELAY_CONNECTION_NAME \
--env LISTENER_SAMINSPECTORPROPERTIES_SAMRESOURCEID=$SAMRESOURCEID \
--env LISTENER_SAMINSPECTORPROPERTIES_SAMURL=$SAMURL \
--env LISTENER_TARGETPROPERTIES_TARGETHOST="http://${RELAY_TARGET_HOST}:8888" \
$LISTENER_DOCKER_IMAGE
