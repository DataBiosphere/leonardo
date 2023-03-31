#!/usr/bin/env bash
set -e

# If you update this file, please update azure.custom-script-extension.file-uris in reference.conf so that Leonardo can adopt the new script

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


# Formatting and mounting persistent disk
WORK_DIRECTORY="/home/$VM_JUP_USER/persistent_disk"
## Create the PD working directory
mkdir -p ${WORK_DIRECTORY}

## The PD should be the only `sd` disk that is not mounted yet
AllsdDisks=($(lsblk --nodeps --noheadings --output NAME --paths | grep -i "sd"))
FreesdDisks=()
for Disk in "${AllsdDisks[@]}"; do
    Mounts="$(lsblk -no MOUNTPOINT "${Disk}")"
    if [ -z "$Mounts" ]; then
        echo "Found our unmounted persistent disk!"
        FreesdDisks="${Disk}"
    else
        echo "Not our persistent disk!"
    fi
done
DISK_DEVICE_PATH=${FreesdDisks}

## Only format disk is it hasn't already been formatted
## It the disk has previously been in use, then it should have a partition that we can mount
EXIT_CODE=0
lsblk -no NAME --paths "${DISK_DEVICE_PATH}1" || EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
  ## From https://learn.microsoft.com/en-us/azure/virtual-machines/linux/attach-disk-portal?tabs=ubuntu
  ## Use the partprobe utility to make sure the kernel is aware of the new partition and filesystem.
  ## Failure to use partprobe can cause the blkid or lslbk commands to not return the UUID for the new filesystem immediately.
  sudo partprobe "${DISK_DEVICE_PATH}1"
  # There is a pre-existing partition that we should try to directly mount
  sudo mount -t ext4 "${DISK_DEVICE_PATH}1" ${WORK_DIRECTORY}
  echo "Existing PD successfully remounted"
else
  ## Create one partition on the PD
  (
  echo o #create a new empty DOS partition table
  echo n #add a new partition
  echo p #print the partition table
  echo
  echo
  echo
  echo w #write table to disk and exit
  ) | sudo fdisk ${DISK_DEVICE_PATH}
  echo "successful partitioning"
  ## Format the partition
  echo y | sudo mkfs -t ext4 "${DISK_DEVICE_PATH}1"
  echo "successful formatting"
  ## From https://learn.microsoft.com/en-us/azure/virtual-machines/linux/attach-disk-portal?tabs=ubuntu
  ## Use the partprobe utility to make sure the kernel is aware of the new partition and filesystem.
  ## Failure to use partprobe can cause the blkid or lslbk commands to not return the UUID for the new filesystem immediately.
  sudo partprobe "${DISK_DEVICE_PATH}1"
  ## Mount the PD partition to the working directory
  sudo mount -t ext4 "${DISK_DEVICE_PATH}1" ${WORK_DIRECTORY}
  echo "successful mount"
fi

## Add the PD UUID to fstab to ensure that the drive is remounted automatically after a reboot
OUTPUT="$(lsblk -no UUID --paths "${DISK_DEVICE_PATH}1")"
echo "UUID="$OUTPUT"    ${WORK_DIRECTORY}    ext4    defaults    0    1" | sudo tee -a /etc/fstab
echo "successful write of PD UUID to fstab"

## Change ownership of the mounted drive to the user
sudo chown -R $VM_JUP_USER:$VM_JUP_USER ${WORK_DIRECTORY}


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
WORKSPACE_ID="${10:-dummy}" # Additionally used for welder
WORKSPACE_STORAGE_CONTAINER_ID="${11:-dummy}" # Additionally used for welder
WELDER_WELDER_DOCKER_IMAGE="${12:-dummy}"
WELDER_OWNER_EMAIL="${13:-dummy}"
WELDER_STAGING_BUCKET="${14:-dummy}"
WELDER_STAGING_STORAGE_CONTAINER_RESOURCE_ID="${15:-dummy}"

# Envs for Jupyter
WORKSPACE_NAME="${16:-dummy}"
WORKSPACE_STORAGE_CONTAINER_URL="${17:-dummy}"

# Jupyter variables for listener
SERVER_APP_BASE_URL="/${RELAY_CONNECTION_NAME}/"
SERVER_APP_ALLOW_ORIGIN="*"
HCVAR='\$hc'
SERVER_APP_WEBSOCKET_URL="wss://${RELAY_NAME}.servicebus.windows.net/${HCVAR}/${RELAY_CONNECTION_NAME}"

# Relay listener configuration
RELAY_CONNECTIONSTRING="Endpoint=sb://${RELAY_NAME}.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=${RELAY_CONNECTION_POLICY_KEY};EntityPath=${RELAY_CONNECTION_NAME}"

# Relay listener configuration - setDateAccessed listener
LEONARDO_URL="${18:-dummy}"
RUNTIME_NAME="${19:-dummy}"
DATEACCESSED_SLEEP_SECONDS=60 # supercedes default defined in terra-azure-relay-listeners/service/src/main/resources/application.yml

# Save output to file
echo "Saving variables to file..."
echo "RELAY_NAME = ${RELAY_NAME}" >> ~/env_vars.txt
echo "RELAY_CONNECTION_NAME = ${RELAY_CONNECTION_NAME}" >> ~/env_vars.txt
echo "RELAY_TARGET_HOST = ${RELAY_TARGET_HOST}" >> ~/env_vars.txt
echo "RELAY_CONNECTION_POLICY_KEY = ${RELAY_CONNECTION_POLICY_KEY}" >> ~/env_vars.txt
echo "LISTENER_DOCKER_IMAGE = ${LISTENER_DOCKER_IMAGE}" >> ~/env_vars.txt
echo "SAMURL = ${SAMURL}" >> ~/env_vars.txt
echo "SAMRESOURCEID = ${SAMRESOURCEID}" >> ~/env_vars.txt
echo "CONTENTSECURITYPOLICY_FILE = ${CONTENTSECURITYPOLICY_FILE}" >> ~/env_vars.txt
echo "WELDER_WSM_URL = ${WELDER_WSM_URL}" >> ~/env_vars.txt
echo "WORKSPACE_ID = ${WORKSPACE_ID}" >> ~/env_vars.txt
echo "WORKSPACE_STORAGE_CONTAINER_ID = ${WORKSPACE_STORAGE_CONTAINER_ID}" >> ~/env_vars.txt
echo "WELDER_WELDER_DOCKER_IMAGE = ${WELDER_WELDER_DOCKER_IMAGE}" >> ~/env_vars.txt
echo "WELDER_OWNER_EMAIL = ${WELDER_OWNER_EMAIL}" >> ~/env_vars.txt
echo "WELDER_STAGING_BUCKET = ${WELDER_STAGING_BUCKET}" >> ~/env_vars.txt
echo "WELDER_STAGING_STORAGE_CONTAINER_RESOURCE_ID = ${WELDER_STAGING_STORAGE_CONTAINER_RESOURCE_ID}" >> ~/env_vars.txt
echo "WORKSPACE_NAME = ${WORKSPACE_NAME}" >> ~/env_vars.txt
echo "WORKSPACE_STORAGE_CONTAINER_URL = ${WORKSPACE_STORAGE_CONTAINER_URL}" >> ~/env_vars.txt
echo "SERVER_APP_BASE_URL = ${SERVER_APP_BASE_URL}" >> ~/env_vars.txt
echo "SERVER_APP_ALLOW_ORIGIN = ${SERVER_APP_ALLOW_ORIGIN}" >> ~/env_vars.txt
echo "SERVER_APP_WEBSOCKET_URL = ${SERVER_APP_WEBSOCKET_URL}" >> ~/env_vars.txt
echo "RELAY_CONNECTIONSTRING = ${RELAY_CONNECTIONSTRING}" >> enviornments
echo "LEONARDO_URL = ${LEONARDO_URL}" >> ~/env_vars.txt
echo "RUNTIME_NAME = ${RUNTIME_NAME}" >> ~/env_vars.txt

# Install relevant libraries

/anaconda/envs/py38_default/bin/pip3 install igv-jupyter

/anaconda/envs/py38_default/bin/pip3 install seaborn

# Update rbase

echo "Y"|sudo apt install --no-install-recommends r-base

#Update kernel list

echo "Y"| /anaconda/bin/jupyter kernelspec remove sparkkernel

echo "Y"| /anaconda/bin/jupyter kernelspec remove sparkrkernel

echo "Y"| /anaconda/bin/jupyter kernelspec remove pysparkkernel

echo "Y"| /anaconda/bin/jupyter kernelspec remove spark-3-python

#echo "Y"| /anaconda/bin/jupyter kernelspec remove julia-1.6

echo "Y"| /anaconda/envs/py38_default/bin/pip3 install ipykernel pydevd

echo "Y"| /anaconda/envs/py38_default/bin/python3 -m ipykernel install

# Start Jupyter server with custom parameters
sudo runuser -l $VM_JUP_USER -c "mkdir -p /home/$VM_JUP_USER/.jupyter"
sudo runuser -l $VM_JUP_USER -c "wget -qP /home/$VM_JUP_USER/.jupyter https://raw.githubusercontent.com/DataBiosphere/leonardo/710389b23b6d6ad6e5698632fe5c0eb34ea952e2/http/src/main/resources/init-resources/jupyter_server_config.py"
sudo runuser -l $VM_JUP_USER -c "wget -qP /anaconda/lib/python3.10/site-packages https://raw.githubusercontent.com/DataBiosphere/terra-docker/0ea6d2ebd7fcae7072e01e1c2f2d178390a276b0/terra-jupyter-base/custom/jupyter_delocalize.py"
sudo runuser -l $VM_JUP_USER -c "sed -i 's/http:\/\/welder:8080/http:\/\/127.0.0.1:8081/g' /anaconda/lib/python3.10/site-packages/jupyter_delocalize.py"
sudo runuser -l $VM_JUP_USER -c "/anaconda/bin/jupyter server --ServerApp.base_url=$SERVER_APP_BASE_URL --ServerApp.websocket_url=$SERVER_APP_WEBSOCKET_URL --ServerApp.contents_manager_class=jupyter_delocalize.WelderContentsManager --autoreload &> /home/$VM_JUP_USER/jupyter.log" >/dev/null 2>&1&

# Store Jupyter Server parameters for reboot processes
sudo crontab -l 2>/dev/null| cat - <(echo "@reboot sudo runuser -l $VM_JUP_USER -c '/anaconda/bin/jupyter server --ServerApp.base_url=$SERVER_APP_BASE_URL --ServerApp.websocket_url=$SERVER_APP_WEBSOCKET_URL --ServerApp.contents_manager_class=jupyter_delocalize.WelderContentsManager --autoreload &> /home/$VM_JUP_USER/jupyter.log' >/dev/null 2>&1&") | crontab -

#Run docker container with Relay Listener
docker run -d --restart always --network host --name listener \
--env LISTENER_RELAYCONNECTIONSTRING=$RELAY_CONNECTIONSTRING \
--env LISTENER_RELAYCONNECTIONNAME=$RELAY_CONNECTION_NAME \
--env LISTENER_REQUESTINSPECTORS_0=samChecker \
--env LISTENER_REQUESTINSPECTORS_1=setDateAccessed \
--env LISTENER_SAMINSPECTORPROPERTIES_SAMRESOURCEID=$SAMRESOURCEID \
--env LISTENER_SAMINSPECTORPROPERTIES_SAMURL=$SAMURL \
--env LISTENER_SETDATEACCESSEDINSPECTORPROPERTIES_SERVICEHOST=$LEONARDO_URL \
--env LISTENER_SETDATEACCESSEDINSPECTORPROPERTIES_WORKSPACEID=$WORKSPACE_ID \
--env LISTENER_SETDATEACCESSEDINSPECTORPROPERTIES_CALLWINDOWINSECONDS=$DATEACCESSED_SLEEP_SECONDS \
--env LISTENER_SETDATEACCESSEDINSPECTORPROPERTIES_RUNTIMENAME=$RUNTIME_NAME \
--env LISTENER_CORSSUPPORTPROPERTIES_CONTENTSECURITYPOLICY="$(cat $CONTENTSECURITYPOLICY_FILE)" \
--env LISTENER_TARGETPROPERTIES_TARGETHOST="http://${RELAY_TARGET_HOST}:8888" \
--env LISTENER_TARGETPROPERTIES_TARGETROUTINGRULES_0_PATHCONTAINS="welder" \
--env LISTENER_TARGETPROPERTIES_TARGETROUTINGRULES_0_TARGETHOST="http://${RELAY_TARGET_HOST}:8081" \
--env LISTENER_TARGETPROPERTIES_TARGETROUTINGRULES_0_REMOVEFROMPATH="\$hc-name/welder" \
--env LOGGING_LEVEL_ROOT=INFO \
$LISTENER_DOCKER_IMAGE

docker run -d --restart always --network host --name welder \
--volume "/home/${VM_JUP_USER}":"/work" \
--env WSM_URL=$WELDER_WSM_URL \
--env PORT=8081 \
--env WORKSPACE_ID=$WORKSPACE_ID \
--env STORAGE_CONTAINER_RESOURCE_ID=$WORKSPACE_STORAGE_CONTAINER_ID \
--env STAGING_STORAGE_CONTAINER_RESOURCE_ID=$WELDER_STAGING_STORAGE_CONTAINER_RESOURCE_ID \
--env OWNER_EMAIL=$WELDER_OWNER_EMAIL \
--env CLOUD_PROVIDER="azure" \
--env LOCKING_ENABLED=false \
--env STAGING_BUCKET=$WELDER_STAGING_BUCKET \
--env SHOULD_BACKGROUND_SYNC="false" \
$WELDER_WELDER_DOCKER_IMAGE

# This next command creates a json file which contains the "env" variables to be added to the kernel.json files.
jq --null-input \
--arg workspace_id "${WORKSPACE_ID}" \
--arg workspace_storage_container_id "${WORKSPACE_STORAGE_CONTAINER_ID}" \
--arg workspace_name "${WORKSPACE_NAME}" \
--arg workspace_storage_container_url "${WORKSPACE_STORAGE_CONTAINER_URL}" \
'{ "env": { "WORKSPACE_ID": $workspace_id, "WORKSPACE_STORAGE_CONTAINER_ID": $workspace_storage_container_id, "WORKSPACE_NAME": $workspace_name, "WORKSPACE_STORAGE_CONTAINER_URL": $workspace_storage_container_url }}' \
> wsenv.json

# This next commands iterate through the available kernels, and uses jq to include the env variables from the previous step
/anaconda/bin/jupyter kernelspec list | awk 'NR>1 {print $2}' | while read line; do jq -s add $line"/kernel.json" wsenv.json > tmpkernel.json && mv tmpkernel.json $line"/kernel.json"; done
/anaconda/envs/py38_default/bin/jupyter kernelspec list | awk 'NR>1 {print $2}' | while read line; do jq -s add $line"/kernel.json" wsenv.json > tmpkernel.json && mv tmpkernel.json $line"/kernel.json"; done
/anaconda/envs/azureml_py38/bin/jupyter kernelspec list | awk 'NR>1 {print $2}' | while read line; do jq -s add $line"/kernel.json" wsenv.json > tmpkernel.json && mv tmpkernel.json $line"/kernel.json"; done
/anaconda/envs/azureml_py38_PT_and_TF/bin/jupyter kernelspec list | awk 'NR>1 {print $2}' | while read line; do jq -s add $line"/kernel.json" wsenv.json > tmpkernel.json && mv tmpkernel.json $line"/kernel.json"; done
