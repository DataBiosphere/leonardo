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

# Variables for listener
#SERVER_APP_BASE_URL=''
#SERVER_APP_ALLOW_ORIGIN='*'
#SERVER_APP_WEBSOCKET_URL=''

# Install relevant libraries

/anaconda/envs/py38_default/bin/pip3 install igv-jupyter

/anaconda/envs/py38_default/bin/pip3 install seaborn

# Create Jupyter Server config- Optional

#/anaconda/bin/jupyter server --generate-config

# Start Jupyter server with custom parameters

/anaconda/bin/jupyter server --ServerApp.port=$SERVER_APP_PORT --ServerApp.token=$SERVER_APP_TOKEN --autoreload --ServerApp.ip=$SERVER_APP_IP --allow-root >/dev/null 2>&1&

# Store Jupyter Server parameters for reboot process

sudo crontab -l 2>/dev/null| cat - <(echo "@reboot /anaconda/bin/jupyter server --ServerApp.port=$SERVER_APP_PORT --ServerApp.token=$SERVER_APP_TOKEN --autoreload --ServerApp.ip=$SERVER_APP_IP --allow-root >/dev/null 2>&1&") | crontab -
