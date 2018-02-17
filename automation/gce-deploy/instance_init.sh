# This script is used to initialize a GCE instance. The shebang
# line is omitted intentionally since it is expected that this
# script will be prefixed with several lines of code defining
# the following variables:
#   - SQL_PROXY_PATH
#   - REMOTE_USER
#   - SERVER_HOST
#   - LEONARDO_SERVER_IMAGE
#
# The script optionally consumes a number of variables used to
# securely fetch remote data. These variables must be defined but
# they may be defined as an empty string.
#   - SERVER_SSL_KEY   # GCS path to encrypted SSL key.
#   - SERVER_SSL_CERT  # GCS path to encrypted SSL cert.
#   - SERVER_CA_BUNDLE # GCS path to encrypted CA Bundle.
#   - INTERNAL_ROOT_CA # GCS path to internal root CA key.
#   - KMS_KEY
#   - KMS_KEYRING
#   - KMS_PROJECT
#   - KMS_LOCATION


KEYROOT=/app
mkdir $KEYROOT


####
#### Install and configure docker and docker-compose.
####

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
add-apt-repository \
        "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
apt-get update
apt-get install -y docker-ce mysql-client

curl -L https://github.com/docker/compose/releases/download/1.19.0/docker-compose-`uname -s`-`uname -m` \
        -o /usr/bin/docker-compose
chmod 755 /usr/bin/docker-compose

usermod -aG docker ubuntu
# If the remote user is configured on this machine, add to docker.
if id -u $REMOTE_USER; then
        usermod -aG docker $REMOTE_USER
fi



####
#### Generate or fetch SSL keys for application.
####

function decrypt_remote_key() {
        local GCS_PATH="${1}"
        local DEST="${2}"
        local INTERMEDIATE="/tmp/$(basename "$DEST").ciphertext"
        gsutil cp "${GCS_PATH}" "${INTERMEDIATE}"
        gcloud kms decrypt \
                --plaintext-file "${DEST}" \
                --ciphertext-file  "${INTERMEDIATE}" \
                --keyring $KMS_KEYRING \
                --key $KMS_KEY \
                --location $KMS_LOCATION \
                --project $KMS_PROJECT
}

if [ -z "${INTERNAL_ROOT_CA}" ]; then
        openssl genrsa -out $KEYROOT/rootCA.key 2048 -des3
else
        decrypt_remote_key "${INTERNAL_ROOT_CA}" $KEYROOT/rootCA.key
fi
openssl req -x509 -new -nodes \
        -subj "/C=US/CN=${SERVER_HOST}" \
        -key $KEYROOT/rootCA.key -days 1024 -out $KEYROOT/rootCA.pem -sha256

openssl genrsa -out $KEYROOT/jupyter-server.key 2048
openssl req -new -key $KEYROOT/jupyter-server.key \
        -subj "/C=US/CN=${SERVER_HOST}" \
        -out $KEYROOT/jupyter-server.csr -sha256
openssl x509 -req -in $KEYROOT/jupyter-server.csr \
        -CA $KEYROOT/rootCA.pem \
        -CAkey $KEYROOT/rootCA.key \
        -CAcreateserial -out $KEYROOT/jupyter-server.crt -days 1500

openssl genrsa -out $KEYROOT/leo-client.key 2048
openssl req -new -key $KEYROOT/leo-client.key \
        -subj "/C=US/CN=${SERVER_HOST}" \
        -out $KEYROOT/leo-client.csr -sha256
openssl x509 -req -in $KEYROOT/leo-client.csr -CA $KEYROOT/rootCA.pem \
        -CAkey $KEYROOT/rootCA.key \
        -CAcreateserial \
        -out $KEYROOT/leo-client.crt \
        -days 1500

# Generate keystore without password.
openssl pkcs12 -export -passout pass:leokeystorepassword \
        -inkey $KEYROOT/leo-client.key \
        -in $KEYROOT/leo-client.crt \
        -out $KEYROOT/leo-client.p12


# Generate front-end self-signed key, delete any key provided
# as a remote key.
openssl req -new -newkey rsa:2048 \
    -days 365 -nodes -x509 \
    -subj "/C=US/CN=${SERVER_HOST}" \
    -keyout $KEYROOT/server.key -out $KEYROOT/server.crt
cp $KEYROOT/server.crt $KEYROOT/ca-bundle.crt

if [ ! -z "${SERVER_SSL_KEY}" ]; then
        rm $KEYROOT/server.key
        decrypt_remote_key "${SERVER_SSL_KEY}" $KEYROOT/server.key
fi
if [ ! -z "${SERVER_SSL_CERT}" ]; then
        rm $KEYROOT/server.crt
        decrypt_remote_key "${SERVER_SSL_CERT}" $KEYROOT/server.crt
fi
if [ ! -z "${SERVER_CA_BUNDLE}" ]; then
        rm $KEYROOT/ca-bundle.crt
        decrypt_remote_key "${SERVER_CA_BUNDLE}" $KEYROOT/ca-bundle.crt
fi

if [ ! -z "${SSL_TEST_FILE}" ]; then
        decrypt_remote_key "${SSL_TEST_FILE}" $KEYROOT/test-file.txt
fi


# Ensure that docker-users can read/write from /app.
chmod 664 /app/*
chown root:docker /app
chown root:docker /app/*



####
#### Localize docker images.
####
docker pull broadinstitute/openidc-proxy:dev
gcloud docker -- pull $LEONARDO_SERVER_IMAGE