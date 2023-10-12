# Logs into an azure service principal and runs the SBT tests. Requires 5 env vars

# The below three are the leo service principal's credentials, and can be found at secret/dsde/terra/azure/qa/leonardo/managed-app-publisher
# LEO_AZURE_CLIENT_ID
# LEO_AZURE_CLIENT_SECRET
# LEO_AZURE_TENANT_ID

# The subscription ID associated with the managed resource group
# LEO_AZURE_SUBSCRIPTION_ID

# The SBT command to run in the docker image
# SBT_TEST_COMMAND

# Install azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | bash

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
yes | apt update
yes | apt install lsof

echo "Done installing lsof, running tests"
echo "echoing stuff"
cat /app/automation/src/test/resources/application.conf


# Run the SBT tests
sbt -batch -Dheadless=true "project automation" "$SBT_TEST_COMMAND"
