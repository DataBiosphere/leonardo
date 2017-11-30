#!/bin/bash

# Generates Jupyter SSL keys and echoes the vault write command.
# Run like `ENV=dev ./generate-jupyter-secrets.sh`.
# It's best to run this in an empty directory.

if [ -z "${ENV}" ]; then
    echo "FATAL ERROR: ENV undefined."
    exit 1
fi

echo "Generating root CA"
openssl genrsa -out rootCA.key 2048 -des3
openssl req -x509 -new -nodes -key rootCA.key -days 1024 -out rootCA.pem -sha256 -subj "/C=US/ST=Massachusetts/L=Cambridge/O=Broad Institute/OU=DSP/CN=leonardo"

echo "Generating server key"
openssl genrsa -out jupyter-server.key 2048
openssl req -new -key jupyter-server.key -out jupyter-server.csr -subj "/C=US/ST=Massachusetts/L=Cambridge/O=Broad Institute/OU=DSP/CN=*.jupyter-${ENV}.firecloud.org" -sha256
openssl x509 -req -in jupyter-server.csr -CA rootCA.pem -CAkey rootCA.key -CAcreateserial -out jupyter-server.crt -days 1500

echo "Generating client key"
openssl genrsa -out leo-client.key 2048
openssl req -new -key leo-client.key -out leo-client.csr -subj "/C=US/ST=Massachusetts/L=Cambridge/O=Broad Institute/OU=DSP/CN=leonardo-client" -sha256
openssl x509 -req -in leo-client.csr -CA rootCA.pem -CAkey rootCA.key -CAcreateserial -out leo-client.crt -days 1500

echo "Generating .p12"
openssl pkcs12 -export -inkey leo-client.key -in leo-client.crt -out leo-client.p12
openssl base64 -in leo-client.p12 -out leo-client.p12.b64

echo "vault write secret/dsde/firecloud/${ENV}/leonardo/jupyter jupyter-server.crt=@jupyter-server.crt jupyter-server.key=@jupyter-server.key leo-client.p12.b64=@leo-client.p12.b64 rootCA.key=@rootCA.key rootCA.pem=@rootCA.pem"
