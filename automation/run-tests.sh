#!/usr/bin/env bash
set -e -x

# Logs into an azure service principal and runs the SBT tests. Requires 5 env vars

# The below three are the leo service principal's credentials, and can be found at secret/dsde/terra/azure/qa/leonardo/managed-app-publisher
# LEO_AZURE_CLIENT_ID
# LEO_AZURE_CLIENT_SECRET
# LEO_AZURE_TENANT_ID

# The subscription ID associated with the managed resource group
# LEO_AZURE_SUBSCRIPTION_ID

# The SBT command to run in the docker image
# SBT_TEST_COMMAND

# Path to leonardo-account.json for the qa domain
# LEONARDO_ACCOUNT_JSON_PATH

# Install gcloud CLI
# Downloading gcloud package
# https://cloud.google.com/sdk/docs/install#linux
# https://stackoverflow.com/questions/28372328/how-to-install-the-google-cloud-sdk-in-a-docker-image
export CLOUDSDK_CORE_DISABLE_PROMPTS=1
curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz

# Installing the package
mkdir -p /usr/local/gcloud
tar -C /usr/local/gcloud -xf /tmp/google-cloud-sdk.tar.gz
/usr/local/gcloud/google-cloud-sdk/install.sh > /dev/null

# Adding the package path to local
export PATH=$PATH:/usr/local/gcloud/google-cloud-sdk/bin

mkdir -p /root/.ssh

gcloud auth activate-service-account --key-file=$LEONARDO_ACCOUNT_JSON_PATH
export GOOGLE_APPLICATION_CREDENTIALS=$LEONARDO_ACCOUNT_JSON_PATH

# Install azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | bash > /dev/null

# Log into service principal and set subscription
az login --service-principal -u "$LEO_AZURE_CLIENT_ID" -p "$LEO_AZURE_CLIENT_SECRET" -t "$LEO_AZURE_TENANT_ID"
az account set -s "$LEO_AZURE_SUBSCRIPTION_ID"
echo "Showing account"
az account show

# Install the bastion portion of the CLI
echo "Installing bastion"
yes | az network bastion list
echo "Bastion installed"

echo "Installing lsof"
yes | apt update > /dev/null
yes | apt install lsof > /dev/null

echo "Done installing lsof, running tests"

# Run the SBT tests
sbt -batch -Dheadless=true "project automation" "$SBT_TEST_COMMAND"
