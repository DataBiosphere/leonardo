#!/usr/bin/env bash
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

# Read script arguments
echo $# arguments
if [$# -ne 13];
    then echo "illegal number of parameters"
fi

RELAY_NAME=$1
RELAY_CONNECTION_NAME=$2
RELAY_TARGET_HOST=$3
RELAY_CONNECTION_POLICY_KEY=$4
LISTENER_DOCKER_IMAGE=$5
SAMURL=$6
SAMRESOURCEID=$7
CONTENTSECURITYPOLICY_FILE=$8

# Envs for welder
WELDER_WSM_URL=${9:-localhost}
WELDER_WORKSPACE_ID="${10:-dummy}"
WELDER_STORAGE_CONTAINER_RESOURCE_ID="${11:-dummy}"
WELDER_WELDER_DOCKER_IMAGE="${12:-dummy}"
WELDER_OWNER_EMAIL="${13:-dummy}"
WELDER_STAGING_BUCKET="${14:-dummy}"

# Define environment variables for Jupyter Server customization

SERVER_APP_PORT=8888
SERVER_APP_TOKEN=''
SERVER_APP_IP=''
SERVER_APP_CERTFILE=''
SERVER_APP_KEYFILE=''
QUIT_BUTTON_VISIBLE=False

# Jupyter variables for listener
SERVER_APP_BASE_URL="/${RELAY_CONNECTION_NAME}/"
SERVER_APP_ALLOW_ORIGIN="*"
HCVAR='\$hc'
SERVER_APP_WEBSOCKET_URL="wss://${RELAY_NAME}.servicebus.windows.net/${HCVAR}/${RELAY_CONNECTION_NAME}"

#Relay listener configuration
RELAY_CONNECTIONSTRING="Endpoint=sb://${RELAY_NAME}.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=${RELAY_CONNECTION_POLICY_KEY};EntityPath=${RELAY_CONNECTION_NAME}"

# Install relevant libraries

/anaconda/envs/py38_default/bin/pip3 install igv-jupyter

/anaconda/envs/py38_default/bin/pip3 install seaborn

# Create Jupyter Server config- Optional

#/anaconda/bin/jupyter server --generate-config

# Start Jupyter server with custom parameters

sudo runuser -l $VM_JUP_USER -c "/anaconda/bin/jupyter server --ServerApp.quit_button=$QUIT_BUTTON_VISIBLE --ServerApp.certfile=$SERVER_APP_CERTFILE --ServerApp.keyfile=$SERVER_APP_KEYFILE --ServerApp.port=$SERVER_APP_PORT --ServerApp.token=$SERVER_APP_TOKEN --ServerApp.ip=$SERVER_APP_IP --ServerApp.base_url=$SERVER_APP_BASE_URL --ServerApp.websocket_url=$SERVER_APP_WEBSOCKET_URL --ServerApp.allow_origin=$SERVER_APP_ALLOW_ORIGIN --autoreload" >/dev/null 2>&1&

wget -qP /usr/local/share/jupyter https://raw.githubusercontent.com/DataBiosphere/terra-docker/622ce501c10968aae26fdf5f5223bda3ffcba3a3/terra-jupyter-base/custom/jupyter_delocalize.py
# Store Jupyter Server parameters for reboot processls
sudo crontab -l 2>/dev/null| cat - <(echo "@reboot sudo runuser -l $VM_JUP_USER -c '/anaconda/bin/jupyter server --ServerApp.quit_button=$QUIT_BUTTON_VISIBLE --ServerApp.certfile=$SERVER_APP_CERTFILE --ServerApp.keyfile=$SERVER_APP_KEYFILE --ServerApp.port=$SERVER_APP_PORT --ServerApp.token=$SERVER_APP_TOKEN --ServerApp.ip=$SERVER_APP_IP --ServerApp.base_url=$SERVER_APP_BASE_URL --ServerApp.websocket_url=$SERVER_APP_WEBSOCKET_URL --ServerApp.allow_origin=$SERVER_APP_ALLOW_ORIGIN --ServerApp.contents_manager_class=jupyter_delocalize.WelderContentsManager --autoreload' >/dev/null 2>&1&") | crontab -

#Run docker container with Relay Listener
docker run -d --restart always --network host --name listener \
--env LISTENER_RELAYCONNECTIONSTRING=$RELAY_CONNECTIONSTRING \
--env LISTENER_RELAYCONNECTIONNAME=$RELAY_CONNECTION_NAME \
--env LISTENER_SAMINSPECTORPROPERTIES_SAMRESOURCEID=$SAMRESOURCEID \
--env LISTENER_SAMINSPECTORPROPERTIES_SAMURL=$SAMURL \
--env LISTENER_CORSSUPPORTPROPERTIES_CONTENTSECURITYPOLICY="$(cat $CONTENTSECURITYPOLICY_FILE)" \
--env LISTENER_TARGETPROPERTIES_TARGETHOST="http://${RELAY_TARGET_HOST}:8888" \
$LISTENER_DOCKER_IMAGE

docker run -d --restart always --network host --name welder \
--volume "/home/${VM_JUP_USER}":"/work" \
--env WSM_URL=$WELDER_WSM_URL \
--env WORKSPACE_ID=$WELDER_WORKSPACE_ID \
--env STORAGE_CONTAINER_RESOURCE_ID=$WELDER_STORAGE_CONTAINER_RESOURCE_ID \
--env OWNER_EMAIL=$WELDER_OWNER_EMAIL \
--env CLOUD_PROVIDER="azure" \
--env STAGING_BUCKET=$WELDER_STAGING_BUCKET \
--env IS_RSTUDIO_RUNTIME="false" \
$WELDER_WELDER_DOCKER_IMAGE
