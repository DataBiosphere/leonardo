ENV=${1:-dev}
VAULT_TOKEN=${2:-$(cat "$HOME"/.vault-token)}

VAULT_ADDR="https://clotho.broadinstitute.org:8200"
LEO_VAULT_PATH="secret/dsde/firecloud/$ENV/leonardo"
SERVICE_OUTPUT_LOCATION="$PWD/rendered"
SECRET_ENV_VARS_LOCATION="${SERVICE_OUTPUT_LOCATION}/secrets.env"

if [ -f "${SECRET_ENV_VARS_LOCATION}" ]; then
  rm "${SECRET_ENV_VARS_LOCATION}"
fi

touch "${SECRET_ENV_VARS_LOCATION}"

# Secret scratch pad to establish variable names, needs testing
cat >"${SECRET_ENV_VARS_LOCATION}" <<EOF
  GALAXY_POSTGRES_PASSWORD="$(vault read -field=password ${LEO_VAULT_PATH}/postgres)";
  DB_USER="$(vault read -field=db_user ${LEO_VAULT_PATH}/secrets)";
  DB_PASSWORD="$(vault read -field=db_password ${LEO_VAULT_PATH}/secrets)";
  LEGACY_GOOGLE_CLIENT_ID="$(vault read -format=json -field=data $LEO_VAULT_PATH/leonardo-oauth-credential.json | jq -r '.web.client_id')";
  AZURE_B2C_CLIENT_ID="$(vault read -field=value secret/dsde/terra/azure/$ENV/b2c/application_id)";
  SSL_CONFIG_PASSWORD="$(vault read -field=client_cert_password $LEO_VAULT_PATH/secrets)";
  AZURE_PUBSUB_ACR_USER="$(vault read -field=username secret/dsde/terra/azure/common/acr_sp)";
  AZURE_PUBSUB_ACR_PASSWORD="$(vault read -field=password secret/dsde/terra/azure/common/acr_sp)";
  AZURE_VM_USER="$(vault read -field=username secret/dsde/terra/azure/dev/leonardo/azure-vm-credential)";
  AZURE_VM_PASSWORD="$(vault read -field=password secret/dsde/terra/azure/dev/leonardo/azure-vm-credential)";
  LEO_MANAGED_APP_CLIENT_ID="$(vault read -field=client-id secret/dsde/terra/azure/dev/leonardo/managed-app-publisher)";
  LEO_MANAGED_APP_CLIENT_SECRET="$(vault read -field=client-secret secret/dsde/terra/azure/dev/leonardo/managed-app-publisher)";
  LEO_MANAGED_APP_TENANT_ID="$(vault read -field=tenant-id secret/dsde/terra/azure/dev/leonardo/managed-app-publisher)";
EOF
sour
