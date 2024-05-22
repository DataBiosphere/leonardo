#!/usr/bin/env bash
set -e -x

# This script is meant to be used from within leo automation tests, and starts a tunnel that allows a bastion to be used to ssh into an azure vm
# It depends on the following env vars:
   # BASTION_NAME, the name of the pre-created bastion in the azure cloud
   # RESOURCE_GROUP, the static testing resource group the vm and bastion are in
   # RESOURCE_ID, the unique identifier of the VM resource. i.e. /subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Compute/virtualMachines/$RUNTIME_NAME
   # PORT, the port on the local machine to be used to tunnel traffic to port 22 on the vm

# Outputs: a file with the name tunnel_$PORT.pid with the system pid of the tunnel process

echo "starting tunnel bastion ${BASTION_NAME} with resource id ${RESOURCE_ID} in resource group ${RESOURCE_GROUP} on port ${PORT}"

nohup az network bastion tunnel --name "$BASTION_NAME" --resource-group "$RESOURCE_GROUP" --target-resource-id "${RESOURCE_ID}" --resource-port 22 --port $PORT > /tmp/nohup.out &

echo "displaying nohup output"
cat /tmp/nohup.out
echo "done displaying nohup output"

NUM_LOOPS=0
until lsof -i tcp:3000
do
  echo "looping until tunnel is started"
  if [ $NUM_LOOPS -eq 60 ]
  then
    echo "Waited for a minute and tunnel never started, exiting"
    break
  fi
  sleep 1
  NUM_LOOPS=$((NUM_LOOPS+1))
done

PID=$(lsof -i tcp:$PORT | tail -1 | awk '{print $2}')
# The last line of this script needs to echo the pid for the sbt test suite to record and clean up
echo "$PID"