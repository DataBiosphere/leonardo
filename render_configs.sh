#!/bin/bash
# Execute from root dir of leonardo repo to generate local secrets

ENV=${1:-dev}
VAULT_TOKEN=${2:-$(cat "$HOME"/.vault-token)}

VAULT_ADDR="https://clotho.broadinstitute.org:8200"
LEO_VAULT_PATH="secret/dsde/firecloud/$ENV/leonardo"
SERVICE_OUTPUT_LOCATION="$(git rev-parse --show-toplevel)/rendered"
SECRET_ENV_VARS_LOCATION="${SERVICE_OUTPUT_LOCATION}/secrets.env"

gcloud container clusters get-credentials --zone us-central1-a --project broad-dsde-dev terra-dev

if [ -f "${SECRET_ENV_VARS_LOCATION}" ]; then
  rm "${SECRET_ENV_VARS_LOCATION}"
fi

touch "${SECRET_ENV_VARS_LOCATION}"

cat >"${SECRET_ENV_VARS_LOCATION}" <<EOF
GALAXY_POSTGRES_PASSWORD="$(vault read -field=password ${LEO_VAULT_PATH}/postgres)"
DB_USER="$(vault read -field=db_user ${LEO_VAULT_PATH}/secrets)"
DB_PASSWORD="$(vault read -field=db_password ${LEO_VAULT_PATH}/secrets)"
LEGACY_GOOGLE_CLIENT_ID="$(vault read -format=json -field=data $LEO_VAULT_PATH/leonardo-oauth-credential.json | jq -r '.web.client_id')"
AZURE_B2C_CLIENT_ID="$(vault read -field=value secret/dsde/terra/azure/$ENV/b2c/application_id)"
SSL_CONFIG_PASSWORD="$(vault read -field=client_cert_password $LEO_VAULT_PATH/secrets)"
AZURE_PUBSUB_ACR_USER="$(vault read -field=username secret/dsde/terra/azure/common/acr_sp)"
AZURE_PUBSUB_ACR_PASSWORD="$(vault read -field=password secret/dsde/terra/azure/common/acr_sp)"
AZURE_VM_USER="$(vault read -field=username secret/dsde/terra/azure/dev/leonardo/azure-vm-credential)"
AZURE_VM_PASSWORD="$(vault read -field=password secret/dsde/terra/azure/dev/leonardo/azure-vm-credential)"
LEO_MANAGED_APP_CLIENT_ID="$(vault read -field=client-id secret/dsde/terra/azure/dev/leonardo/managed-app-publisher)"
LEO_MANAGED_APP_CLIENT_SECRET="$(vault read -field=client-secret secret/dsde/terra/azure/dev/leonardo/managed-app-publisher)"
LEO_MANAGED_APP_TENANT_ID="$(vault read -field=tenant-id secret/dsde/terra/azure/dev/leonardo/managed-app-publisher)"
EOF

# We need to remove quotes from the rendered file, because quoted env files do not play well with dockers `--env-file` arg.
sed -i.bak 's/\"//g' "${SECRET_ENV_VARS_LOCATION}"
rm "${SECRET_ENV_VARS_LOCATION}.bak"

kubectl -n terra-dev get secret leonardo-sa-secret -o 'go-template={{index .data "leonardo-account.json"}}' | base64 --decode > ${SERVICE_OUTPUT_LOCATION}/leonardo-account.json
kubectl -n terra-dev get secret leonardo-sa-secret -o 'go-template={{index .data "leonardo-account.pem"}}' | base64 --decode > ${SERVICE_OUTPUT_LOCATION}/leonardo-account.pem

GOOGLE_PROJECT=$(kubectl -n terra-dev get secret leonardo-cloudsql-instance -o 'go-template={{ .data.project }}' | base64 --decode)
CLOUDSQL_ZONE=$(kubectl -n terra-dev get secret leonardo-cloudsql-instance -o 'go-template={{ .data.region }}' | base64 --decode)
CLOUDSQL_INSTANCE=$(kubectl -n terra-dev get secret leonardo-cloudsql-instance -o 'go-template={{ .data.name }}' | base64 --decode)

read -p "Override GOOGLE_PROJECT for CloudSQL proxy? (default: ${GOOGLE_PROJECT}, or press enter): " google_project
GOOGLE_PROJECT="${google_project:-${GOOGLE_PROJECT}}"
read -p "Override CLOUDSQL_ZONE? (default: ${CLOUDSQL_ZONE}, or press enter): " cloudsql_zone
CLOUDSQL_ZONE="${cloudsql_zone:-${CLOUDSQL_ZONE}}"
read -p "Override CLOUDSQL_INSTANCE? (default ${CLOUDSQL_INSTANCE}, or press enter): " cloudsql_inst
CLOUDSQL_INSTANCE="${cloudsql_inst:-${CLOUDSQL_INSTANCE}}"

echo "GOOGLE_PROJECT=${GOOGLE_PROJECT}" > ${SERVICE_OUTPUT_LOCATION}/sqlproxy.env
echo "CLOUDSQL_ZONE=${CLOUDSQL_ZONE}" >> ${SERVICE_OUTPUT_LOCATION}/sqlproxy.env
echo "CLOUDSQL_INSTANCE=${CLOUDSQL_INSTANCE}" >> ${SERVICE_OUTPUT_LOCATION}/sqlproxy.env

kubectl -n terra-dev get secret leonardo-application-secret -o 'go-template={{index .data "jupyter-server.crt"}}' | base64 --decode > ${SERVICE_OUTPUT_LOCATION}/jupyter-server.crt
kubectl -n terra-dev get secret leonardo-application-secret -o 'go-template={{index .data "jupyter-server.key"}}' | base64 --decode > ${SERVICE_OUTPUT_LOCATION}/jupyter-server.key
kubectl -n terra-dev get secret leonardo-application-secret -o 'go-template={{index .data "leo-client.p12"}}' | base64 --decode > ${SERVICE_OUTPUT_LOCATION}/leo-client.p12
kubectl -n terra-dev get secret leonardo-application-secret -o 'go-template={{index .data "rootCA.key"}}' | base64 --decode > ${SERVICE_OUTPUT_LOCATION}/rootCA.key
kubectl -n terra-dev get secret leonardo-application-secret -o 'go-template={{index .data "rootCA.pem"}}' | base64 --decode > ${SERVICE_OUTPUT_LOCATION}/rootCA.pem

kubectl -n terra-dev get configmap leonardo-oauth2-configmap -o 'go-template={{index .data "oauth2.conf"}}' > ${SERVICE_OUTPUT_LOCATION}/oauth2.conf
# Local dev uses a macOS-specific docker replacement hostname for locahost, so replace all instances in the proxy config.
kubectl -n terra-dev get configmap leonardo-site-configmap -o 'go-template={{index .data "site.conf"}}' | sed 's/localhost/host\.docker\.internal/g' > ${SERVICE_OUTPUT_LOCATION}/site.conf

kubectl -n local-dev get secrets local-dev-cert -o 'go-template={{index .data "tls.crt"}}' | base64 --decode > ${SERVICE_OUTPUT_LOCATION}/server.crt
kubectl -n local-dev get secrets local-dev-cert -o 'go-template={{index .data "tls.key"}}' | base64 --decode > ${SERVICE_OUTPUT_LOCATION}/server.key

{
echo B2C_APPLICATION_ID=$(kubectl -n terra-dev get secret leonardo-proxy-b2c-secrets -o 'go-template={{ index .data "application-id" }}' | base64 --decode)
} > ${SERVICE_OUTPUT_LOCATION}/proxy.env
