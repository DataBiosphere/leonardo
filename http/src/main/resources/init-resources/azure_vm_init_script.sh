#!/usr/bin/env bash
set -e
# Log output is saved at /var/log/azure_vm_init_script.log

# If you update this file, please update azure.custom-script-extension.file-uris in reference.conf so that Leonardo can adopt the new script

# This is to avoid the error Ref BioC
# 'debconf: unable to initialize frontend: Dialog'
export DEBIAN_FRONTEND=noninteractive

##### JUPYTER USER SETUP #####
# Create the jupyter user that corresponds to the jupyter user in the jupyter container
VM_JUP_USER=jupyter-user
VM_JUP_USER_UID=1002

sudo useradd -m -c "Jupyter User" -u $VM_JUP_USER_UID $VM_JUP_USER
sudo usermod -a -G $VM_JUP_USER,adm,dialout,cdrom,floppy,audio,dip,video,plugdev,lxd,netdev $VM_JUP_USER

##### READ SCRIPT ARGUMENT #####
# These are passed in setupCreateVmCreateMessage in the AzurePubsub Handler
echo $# arguments
if [ $# -ne 13 ];
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
JUPYTER_DOCKER_IMAGE="terradevacrpublic.azurecr.io/jupyter-server:test"
# NOTEBOOKS_DIR corresponds to the location INSIDE the jupyter docker container,
# and is not to be used withing the context of the DSVM itself
NOTEBOOKS_DIR="/home/$VM_JUP_USER/persistent_disk"
WORKSPACE_NAME="${16:-dummy}"
WORKSPACE_STORAGE_CONTAINER_URL="${17:-dummy}"

# Jupyter variables for listener
SERVER_APP_BASE_URL="/${RELAY_CONNECTION_NAME}/"
SERVER_APP_ALLOW_ORIGIN="*"
# We need to escape this $ character twice, once for the docker exec arg, and another time for passing it to run-jupyter.sh
HCVAR='\\\$hc'
SERVER_APP_WEBSOCKET_URL="wss://${RELAY_NAME}.servicebus.windows.net/${HCVAR}/${RELAY_CONNECTION_NAME}"
# We need to escape this $ character one extra time to pass it to the crontab for rebooting. The use of $hc in the websocket URL is
# something that we should rethink as it creates a lot of complexity downstream
REBOOT_HCVAR='\\\\\\\$hc'
REBOOT_SERVER_APP_WEBSOCKET_URL="wss://${RELAY_NAME}.servicebus.windows.net/${REBOOT_HCVAR}/${RELAY_CONNECTION_NAME}"
SERVER_APP_WEBSOCKET_HOST="${RELAY_NAME}.servicebus.windows.net"

# Relay listener configuration
RELAY_CONNECTIONSTRING="Endpoint=sb://${RELAY_NAME}.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=${RELAY_CONNECTION_POLICY_KEY};EntityPath=${RELAY_CONNECTION_NAME}"

# Relay listener configuration - setDateAccessed listener
LEONARDO_URL="${18:-dummy}"
RUNTIME_NAME="${19:-dummy}"
VALID_HOSTS="${20:-dummy}"
DATEACCESSED_SLEEP_SECONDS=60 # supercedes default defined in terra-azure-relay-listeners/service/src/main/resources/application.yml

# R version
R_VERSION="4.4.0-1.2004.0"

# Log in script output for debugging purposes.
echo "RELAY_NAME = ${RELAY_NAME}"
echo "RELAY_CONNECTION_NAME = ${RELAY_CONNECTION_NAME}"
echo "RELAY_TARGET_HOST = ${RELAY_TARGET_HOST}"
echo "RELAY_CONNECTION_POLICY_KEY = ${RELAY_CONNECTION_POLICY_KEY}"
echo "LISTENER_DOCKER_IMAGE = ${LISTENER_DOCKER_IMAGE}"
echo "SAMURL = ${SAMURL}"
echo "SAMRESOURCEID = ${SAMRESOURCEID}"
echo "CONTENTSECURITYPOLICY_FILE = ${CONTENTSECURITYPOLICY_FILE}"
echo "WELDER_WSM_URL = ${WELDER_WSM_URL}"
echo "WORKSPACE_ID = ${WORKSPACE_ID}"
echo "WORKSPACE_STORAGE_CONTAINER_ID = ${WORKSPACE_STORAGE_CONTAINER_ID}"
echo "WELDER_WELDER_DOCKER_IMAGE = ${WELDER_WELDER_DOCKER_IMAGE}"
echo "WELDER_OWNER_EMAIL = ${WELDER_OWNER_EMAIL}"
echo "WELDER_STAGING_BUCKET = ${WELDER_STAGING_BUCKET}"
echo "WELDER_STAGING_STORAGE_CONTAINER_RESOURCE_ID = ${WELDER_STAGING_STORAGE_CONTAINER_RESOURCE_ID}"
echo "WORKSPACE_NAME = ${WORKSPACE_NAME}"
echo "WORKSPACE_STORAGE_CONTAINER_URL = ${WORKSPACE_STORAGE_CONTAINER_URL}"
echo "SERVER_APP_BASE_URL = ${SERVER_APP_BASE_URL}"
echo "SERVER_APP_ALLOW_ORIGIN = ${SERVER_APP_ALLOW_ORIGIN}"
echo "SERVER_APP_WEBSOCKET_URL = ${SERVER_APP_WEBSOCKET_URL}"
echo "RELAY_CONNECTIONSTRING = ${RELAY_CONNECTIONSTRING}"
echo "LEONARDO_URL = ${LEONARDO_URL}"
echo "RUNTIME_NAME = ${RUNTIME_NAME}"
echo "VALID_HOSTS = ${VALID_HOSTS}"
echo "R-VERSION = ${R_VERSION}"

##### Persistent Disk (PD) MOUNTING #####
# Formatting and mounting persistent disk
# Note that we cannot mount in /mnt/disks/work as it is a temporary disk on the DSVM!
PD_DIRECTORY="/home/$VM_JUP_USER/persistent_disk"
## Create the working and persistent disk directories
mkdir -p ${PD_DIRECTORY}

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
  sudo mount -t ext4 "${DISK_DEVICE_PATH}1" ${PD_DIRECTORY}
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
  sudo mount -t ext4 "${DISK_DEVICE_PATH}1" ${PD_DIRECTORY}
  echo "successful mount"
fi

## Add the PD UUID to fstab to ensure that the drive is remounted automatically after a reboot
OUTPUT="$(lsblk -no UUID --paths "${DISK_DEVICE_PATH}1")"
echo "UUID="$OUTPUT"    ${PD_DIRECTORY}    ext4    defaults    0    1" | sudo tee -a /etc/fstab
echo "successful write of PD UUID to fstab"

## Make sure that both the jupyter and welder users have access to the persistent disk on the VM
## This needs to happen before we start up containers
sudo chmod a+rwx ${PD_DIRECTORY}

##### JUPYTER SERVER  #####
echo "------ Jupyter version: ${JUPYTER_DOCKER_IMAGE} ------"
echo "Starting Jupyter with command..."

echo "docker run -d --gpus all --restart always --network host --name jupyter \
--entrypoint tail \
--volume ${PD_DIRECTORY}:${NOTEBOOKS_DIR} \
-e CLOUD_PROVIDER=Azure \
-e WORKSPACE_ID=$WORKSPACE_ID \
-e WORKSPACE_NAME=$WORKSPACE_NAME \
-e WORKSPACE_STORAGE_CONTAINER_URL=$WORKSPACE_STORAGE_CONTAINER_URL \
-e STORAGE_CONTAINER_RESOURCE_ID=$WORKSPACE_STORAGE_CONTAINER_ID \
$JUPYTER_DOCKER_IMAGE \
-f /dev/null"

#Run docker container with Jupyter Server
#Override entrypoint with a placeholder (tail -f /dev/null) to keep the container running indefinitely.
#The jupyter server itself will be started via docker exec after.
#Mount the persistent disk directory to the jupyter notebook home directory
docker run -d --gpus all --restart always --network host --name jupyter \
--entrypoint tail \
--volume ${PD_DIRECTORY}:${NOTEBOOKS_DIR} \
--env CLOUD_PROVIDER=Azure \
--env WORKSPACE_ID=$WORKSPACE_ID \
--env WORKSPACE_NAME=$WORKSPACE_NAME \
--env WORKSPACE_STORAGE_CONTAINER_URL=$WORKSPACE_STORAGE_CONTAINER_URL \
--env STORAGE_CONTAINER_RESOURCE_ID=$WORKSPACE_STORAGE_CONTAINER_ID \
$JUPYTER_DOCKER_IMAGE \
-f /dev/null

echo 'Starting Jupyter Notebook...'
echo "docker exec -d jupyter /bin/bash -c '/usr/jupytervenv/run-jupyter.sh ${SERVER_APP_BASE_URL} ${SERVER_APP_WEBSOCKET_URL} ${NOTEBOOKS_DIR}'"
docker exec -d jupyter /bin/bash -c "/usr/jupytervenv/run-jupyter.sh ${SERVER_APP_BASE_URL} ${SERVER_APP_WEBSOCKET_URL} ${NOTEBOOKS_DIR}"

# Store Jupyter Server Docker exec command for reboot processes
# Cron does not play well with escaping backlashes so it is safer to run a script instead of the docker command directly
echo "docker exec -d jupyter /bin/bash -c '/usr/jupytervenv/run-jupyter.sh ${SERVER_APP_BASE_URL} ${REBOOT_SERVER_APP_WEBSOCKET_URL} ${NOTEBOOKS_DIR}'" | sudo tee /home/reboot_script.sh
sudo chmod +x /home/reboot_script.sh
sudo crontab -l 2>/dev/null| cat - <(echo "@reboot /home/reboot_script.sh") | crontab -

echo "------ Jupyter done ------"

##### LISTENER #####
echo "------ Listener version: ${LISTENER_DOCKER_IMAGE} ------"
echo "    Starting listener with command..."

echo "docker run -d --restart always --network host --name listener \
-e LISTENER_RELAYCONNECTIONSTRING=\"$RELAY_CONNECTIONSTRING\" \
-e LISTENER_RELAYCONNECTIONNAME=\"$RELAY_CONNECTION_NAME\" \
-e LISTENER_REQUESTINSPECTORS_0=\"samChecker\" \
-e LISTENER_REQUESTINSPECTORS_1=\"setDateAccessed\" \
-e LISTENER_SAMINSPECTORPROPERTIES_SAMRESOURCEID=\"$SAMRESOURCEID\" \
-e LISTENER_SAMINSPECTORPROPERTIES_SAMURL=\"$SAMURL\" \
-e LISTENER_SETDATEACCESSEDINSPECTORPROPERTIES_SERVICEHOST=\"$LEONARDO_URL\" \
-e LISTENER_SETDATEACCESSEDINSPECTORPROPERTIES_WORKSPACEID=\"$WORKSPACE_ID\" \
-e LISTENER_SETDATEACCESSEDINSPECTORPROPERTIES_CALLWINDOWINSECONDS=\"$DATEACCESSED_SLEEP_SECONDS\" \
-e LISTENER_SETDATEACCESSEDINSPECTORPROPERTIES_RUNTIMENAME=\"$RUNTIME_NAME\" \
-e LISTENER_CORSSUPPORTPROPERTIES_CONTENTSECURITYPOLICY=\"$(cat $CONTENTSECURITYPOLICY_FILE)\" \
-e LISTENER_CORSSUPPORTPROPERTIES_VALIDHOSTS=\"${VALID_HOSTS},${SERVER_APP_WEBSOCKET_HOST}\" \
-e LISTENER_TARGETPROPERTIES_TARGETHOST=\"http://$RELAY_TARGET_HOST:8888\" \
-e LISTENER_TARGETPROPERTIES_TARGETROUTINGRULES_0_PATHCONTAINS=welder \
-e LISTENER_TARGETPROPERTIES_TARGETROUTINGRULES_0_TARGETHOST=http://$RELAY_TARGET_HOST:8081 \
-e LISTENER_TARGETPROPERTIES_TARGETROUTINGRULES_0_REMOVEFROMPATH=\"\$hc-name/welder\" \
-e LOGGING_LEVEL_ROOT=INFO \
$LISTENER_DOCKER_IMAGE"

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
--env LISTENER_CORSSUPPORTPROPERTIES_VALIDHOSTS="${VALID_HOSTS},${SERVER_APP_WEBSOCKET_HOST}" \
--env LISTENER_TARGETPROPERTIES_TARGETHOST="http://${RELAY_TARGET_HOST}:8888" \
--env LISTENER_TARGETPROPERTIES_TARGETROUTINGRULES_0_PATHCONTAINS="welder" \
--env LISTENER_TARGETPROPERTIES_TARGETROUTINGRULES_0_TARGETHOST="http://${RELAY_TARGET_HOST}:8081" \
--env LISTENER_TARGETPROPERTIES_TARGETROUTINGRULES_0_REMOVEFROMPATH="\$hc-name/welder" \
--env LOGGING_LEVEL_ROOT=INFO \
$LISTENER_DOCKER_IMAGE

echo "------ Listener done ------"

##### WELDER #####
echo "------ Welder version: ${WELDER_WELDER_DOCKER_IMAGE} ------"
echo "    Starting Welder with command...."

echo "docker run -d --restart always --network host --name welder \
     --volume "${PD_DIRECTORY}:/work" \
     -e WSM_URL=$WELDER_WSM_URL \
     -e PORT=8081 \
     -e WORKSPACE_ID=$WORKSPACE_ID \
     -e STORAGE_CONTAINER_RESOURCE_ID=$WORKSPACE_STORAGE_CONTAINER_ID \
     -e STAGING_STORAGE_CONTAINER_RESOURCE_ID=$WELDER_STAGING_STORAGE_CONTAINER_RESOURCE_ID \
     -e OWNER_EMAIL=\"$WELDER_OWNER_EMAIL\" \
     -e CLOUD_PROVIDER=\"azure\" \
     -e LOCKING_ENABLED=false \
     -e STAGING_BUCKET=\"$WELDER_STAGING_BUCKET\" \
     -e SHOULD_BACKGROUND_SYNC=\"false\" \
     $WELDER_WELDER_DOCKER_IMAGE"

docker run -d --restart always --network host --name welder \
--volume "${PD_DIRECTORY}:/work" \
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

echo "------ Welder done ------"