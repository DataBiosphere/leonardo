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


CONFIGROOT=/home/ubuntu/app
mkdir $CONFIGROOT
chmod 777 $CONFIGROOT

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

# With install finished (all further operations can be done as
# the primary non-root user).
sudo -u ubuntu bash << EEOOFF

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
        openssl genrsa -out $CONFIGROOT/rootCA.key 2048 -des3
else
        decrypt_remote_key "${INTERNAL_ROOT_CA}" $CONFIGROOT/rootCA.key
fi
openssl req -x509 -new -nodes \
        -subj "/C=US/CN=*.${SERVER_HOST}" \
        -key $CONFIGROOT/rootCA.key -days 1024 -out $CONFIGROOT/rootCA.pem -sha256

openssl genrsa -out $CONFIGROOT/jupyter-server.key 2048
openssl req -new -key $CONFIGROOT/jupyter-server.key \
        -subj "/C=US/CN=*.${SERVER_HOST}" \
        -out $CONFIGROOT/jupyter-server.csr -sha256
openssl x509 -req -in $CONFIGROOT/jupyter-server.csr \
        -CA $CONFIGROOT/rootCA.pem \
        -CAkey $CONFIGROOT/rootCA.key \
        -CAcreateserial -out $CONFIGROOT/jupyter-server.crt -days 1500

# Generate leo client certificate. Common name must be different
# than the leo server cert.
openssl genrsa -out $CONFIGROOT/leo-client.key 2048
openssl req -new -key $CONFIGROOT/leo-client.key \
        -subj "/C=US/CN=leonardo" \
        -out $CONFIGROOT/leo-client.csr -sha256
openssl x509 -req -in $CONFIGROOT/leo-client.csr -CA $CONFIGROOT/rootCA.pem \
        -CAkey $CONFIGROOT/rootCA.key \
        -CAcreateserial \
        -out $CONFIGROOT/leo-client.crt \
        -days 1500

# Generate keystore without password.
openssl pkcs12 -export -passout pass:leokeystorepassword \
        -inkey $CONFIGROOT/leo-client.key \
        -in $CONFIGROOT/leo-client.crt \
        -out $CONFIGROOT/leo-client.p12

# Generate front-end self-signed key, delete any key provided
# as a remote key.
openssl req -new -newkey rsa:2048 \
    -days 365 -nodes -x509 \
    -subj "/C=US/CN=*.${SERVER_HOST}" \
    -keyout $CONFIGROOT/server.key -out $CONFIGROOT/server.crt
cp $CONFIGROOT/server.crt $CONFIGROOT/ca-bundle.crt

if [ ! -z "${SERVER_SSL_KEY}" ]; then
        rm $CONFIGROOT/server.key
        decrypt_remote_key "${SERVER_SSL_KEY}" $CONFIGROOT/server.key
fi
if [ ! -z "${SERVER_SSL_CERT}" ]; then
        rm $CONFIGROOT/server.crt
        decrypt_remote_key "${SERVER_SSL_CERT}" $CONFIGROOT/server.crt
fi
if [ ! -z "${SERVER_CA_BUNDLE}" ]; then
        rm $CONFIGROOT/ca-bundle.crt
        decrypt_remote_key "${SERVER_CA_BUNDLE}" $CONFIGROOT/ca-bundle.crt
fi

if [ ! -z "${SSL_TEST_FILE}" ]; then
        decrypt_remote_key "${SSL_TEST_FILE}" $CONFIGROOT/test-file.txt
fi


####
#### Localize docker images.
####
docker pull broadinstitute/openidc-proxy:dev
gcloud docker -- pull $LEONARDO_SERVER_IMAGE

# End "ubuntu" user part of init script.
EEOOFF
