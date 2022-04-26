#!/bin/bash
set -e

# This is to avoid the error Ref BioC
# 'debconf: unable to initialize frontend: Dialog'
export DEBIAN_FRONTEND=noninteractive

VM_JUP_USER=$9
VM_JUP_USER_PASSWORD=$10

#sudo groupadd $VM_JUP_USER
#sudo useradd -g adm, $VM_JUP_USER -d /home/$VM_JUP_USER -c "Jupyter User" $VM_JUP_USER

sudo useradd -m -c "Jupyter User" $VM_JUP_USER
(echo "$VM_JUP_USER_PASSWORD"; echo "$VM_JUP_USER_PASSWORD") | sudo passwd $VM_JUP_USER
sudo usermod -a -G $VM_JUP_USER,adm $VM_JUP_USER

## Update apt-get
apt-get update

apt-get install -y --no-install-recommends apt-utils

sudo chgrp $VM_JUP_USER /anaconda/bin/*

sudo chown $VM_JUP_USER /anaconda/bin/*

sudo chgrp $VM_JUP_USER /anaconda/envs/py38_default/bin/*

sudo chown $VM_JUP_USER /anaconda/envs/py38_default/bin/*

systemctl stop jupyterhub.service

#Read script arguments

RELAY_NAME=$1
RELAY_CONNECTION_NAME=$2
RELAY_TARGET_HOST=$3
RELAY_CONNECTION_POLICY_NAME=$4
RELAY_CONNECTION_POLICY_KEY=$5
DOCKER_USER_NAME=$6
DOCKER_USER_PASSWORD=$7
LISTENER_DOCKER_IMAGE=$8

# Define environment variables for Jupyter Server customization

SERVER_APP_PORT=8888
SERVER_APP_TOKEN=''
SERVER_APP_IP=''
SERVER_APP_CERTFILE=''
SERVER_APP_KEYFILE=''

# Jupyter variables for listener
SERVER_APP_BASE_URL="/${RELAY_CONNECTION_NAME}/"
SERVER_APP_ALLOW_ORIGIN="*"
SERVER_APP_WEBSOCKET_URL="wss://${RELAY_NAME}.servicebus.windows.net/\$hc/${RELAY_CONNECTION_NAME}"

#Relay listener configuration
RELAY_CONNECTIONSTRING="Endpoint=sb://${RELAY_NAME}.servicebus.windows.net/;SharedAccessKeyName=${RELAY_CONNECTION_POLICY_NAME};SharedAccessKey=${RELAY_CONNECTION_POLICY_KEY};EntityPath=${RELAY_CONNECTION_NAME}"

# Install relevant libraries

/anaconda/envs/py38_default/bin/pip3 install igv-jupyter

/anaconda/envs/py38_default/bin/pip3 install seaborn

# Create Jupyter Server config- Optional

#/anaconda/bin/jupyter server --generate-config

# Start Jupyter server with custom parameters

sudo runuser -l $VM_JUP_USER -c '/anaconda/bin/jupyter server --ServerApp.certfile=$SERVER_APP_CERTFILE --ServerApp.keyfile=$SERVER_APP_KEYFILE --ServerApp.port=$SERVER_APP_PORT --ServerApp.token=$SERVER_APP_TOKEN --ServerApp.ip=$SERVER_APP_IP --ServerApp.base_url=$SERVER_APP_BASE_URL --ServerApp.websocket_url=$SERVER_APP_WEBSOCKET_URL --ServerApp.allow_origin=$SERVER_APP_ALLOW_ORIGIN autoreload' >/dev/null 2>&1&

# Store Jupyter Server parameters for reboot process

echo "CRON TEST!!!\n"
echo "@reboot sudo runuser -l $VM_JUP_USER -c '/anaconda/bin/jupyter server --ServerApp.certfile=$SERVER_APP_CERTFILE --ServerApp.keyfile=$SERVER_APP_KEYFILE --ServerApp.port=$SERVER_APP_PORT --ServerApp.token=$SERVER_APP_TOKEN --ServerApp.ip=$SERVER_APP_IP --ServerApp.base_url=$SERVER_APP_BASE_URL --ServerApp.websocket_url=$SERVER_APP_WEBSOCKET_URL --ServerApp.allow_origin=$SERVER_APP_ALLOW_ORIGIN --autoreload' >/dev/null 2>&1&"
echo "END CRON TEST!!!\n"

HCVAR='\$hc'
SERVER_APP_WEBSOCKET_URL2="wss://${RELAY_NAME}.servicebus.windows.net/${HCVAR}/${RELAY_CONNECTION_NAME}"
echo "VAR TEST!!!\n"
echo $HCVAR
echo $SERVER_APP_WEBSOCKET_URL2
echo "END VAR TEST!!!\n"

sudo crontab -l 2>/dev/null| cat - <(echo "@reboot sudo runuser -l $VM_JUP_USER -c '/anaconda/bin/jupyter server --ServerApp.certfile=$SERVER_APP_CERTFILE --ServerApp.keyfile=$SERVER_APP_KEYFILE --ServerApp.port=$SERVER_APP_PORT --ServerApp.token=$SERVER_APP_TOKEN --ServerApp.ip=$SERVER_APP_IP --ServerApp.base_url=$SERVER_APP_BASE_URL --ServerApp.websocket_url=$SERVER_APP_WEBSOCKET_URL2 --ServerApp.allow_origin=$SERVER_APP_ALLOW_ORIGIN --autoreload' >/dev/null 2>&1&") | crontab -

# Login to ACR repo to pull the image for Relay Listener

docker login terradevacrpublic.azurecr.io -u $DOCKER_USER_NAME -p $DOCKER_USER_PASSWORD

#Run docker container with Relay Listener

docker run -d --restart always --network host --name RelayListener \
--env LISTENER_RELAYCONNECTIONSTRING=$RELAY_CONNECTIONSTRING \
--env LISTENER_RELAYCONNECTIONNAME=$RELAY_CONNECTION_NAME \
--env LISTENER_TARGETPROPERTIES_TARGETHOST="http://${RELAY_TARGET_HOST}:8888" \
$LISTENER_DOCKER_IMAGE

sudo shutdown -r 1
