#!/usr/bin/env bash

set -e -x

# adapted from https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/blob/master/datalab/datalab.sh

# Initialize the dataproc cluster with Jupyter and apache proxy docker images
# Uses cluster-docker-compose.yaml

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)

# If a Google credentials file was specified, grab the service account json file and set the GOOGLE_APPLICATION_CREDENTIALS EV.
# This overrides the credentials on the metadata server.
# This needs to happen on master and worker nodes.
JUPYTER_SERVICE_ACCOUNT_CREDENTIALS=$(jupyterServiceAccountCredentials)
if [ ! -z ${JUPYTER_SERVICE_ACCOUNT_CREDENTIALS} ] ; then
  gsutil cp ${JUPYTER_SERVICE_ACCOUNT_CREDENTIALS} /etc
  JUPYTER_SERVICE_ACCOUNT_CREDENTIALS=`basename ${JUPYTER_SERVICE_ACCOUNT_CREDENTIALS}`
  export GOOGLE_APPLICATION_CREDENTIALS=/etc/${JUPYTER_SERVICE_ACCOUNT_CREDENTIALS}
fi

# Only initialize Jupyter docker containers on the master
if [[ "${ROLE}" == 'Master' ]]; then
    JUPYTER_HOME=/etc/jupyter
    JUPYTER_USER_HOME=/home/jupyter-user

    # The following values are populated by Leo when a cluster is created.
    export CLUSTER_NAME=$(clusterName)
    export GOOGLE_PROJECT=$(googleProject)
    export JUPYTER_SERVER_NAME=$(jupyterServerName)
    export PROXY_SERVER_NAME=$(proxyServerName)
    export JUPYTER_DOCKER_IMAGE=$(jupyterDockerImage)
    export PROXY_DOCKER_IMAGE=$(proxyDockerImage)

    JUPYTER_SERVER_CRT=$(jupyterServerCrt)
    JUPYTER_SERVER_KEY=$(jupyterServerKey)
    JUPYTER_ROOT_CA=$(rootCaPem)
    JUPYTER_DOCKER_COMPOSE=$(jupyterDockerCompose)
    JUPYTER_PROXY_SITE_CONF=$(jupyterProxySiteConf)
    JUPYTER_EXTENSION_URI=$(jupyterExtensionUri)
    JUPYTER_CUSTOM_JS_URI=$(jupyterCustomJsUri)
    JUPYTER_GOOGLE_SIGN_IN_JS_URI=$(jupyterGoogleSignInJsUri)
    JUPYTER_USER_SCRIPT_URI=$(jupyterUserScriptUri)

    # install Docker
    export DOCKER_CE_VERSION="17.12.0~ce-0~debian"
    apt-get update
    apt-get install -y \
     apt-transport-https \
     ca-certificates \
     curl \
     gnupg2 \
     software-properties-common

    curl -fsSL https://download.docker.com/linux/$(. /etc/os-release; echo "$ID")/gpg | apt-key add -

    add-apt-repository \
     "deb [arch=amd64] https://download.docker.com/linux/$(. /etc/os-release; echo "$ID") \
     $(lsb_release -cs) \
     stable"

    apt-get update
    apt-get install -y docker-ce=$DOCKER_CE_VERSION

    # Install docker-compose
    curl -L https://github.com/docker/compose/releases/download/1.18.0/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose

    mkdir /work
    mkdir /certs
    chmod a+wx /work

    # Add the certificates from the bucket to the VM. They are used by the docker-compose file
    gsutil cp ${JUPYTER_SERVER_CRT} /certs
    gsutil cp ${JUPYTER_SERVER_KEY} /certs
    gsutil cp ${JUPYTER_ROOT_CA} /certs
    gsutil cp ${JUPYTER_PROXY_SITE_CONF} /etc
    gsutil cp ${JUPYTER_DOCKER_COMPOSE} /etc

    # Needed because docker-compose can't handle symlinks
    touch /hadoop_gcs_connector_metadata_cache
    touch auth_openidc.conf

    # If we have a service account JSON file, create an .env file to set GOOGLE_APPLICATION_CREDENTIALS
    # in the docker container. Otherwise, we should _not_ set this environment variable so it uses the
    # credentials on the metadata server.
    if [ ! -z ${JUPYTER_SERVICE_ACCOUNT_CREDENTIALS} ] ; then
      echo "GOOGLE_APPLICATION_CREDENTIALS=/etc/${JUPYTER_SERVICE_ACCOUNT_CREDENTIALS}" > /etc/google_application_credentials.env
    else
      echo "" > /etc/google_application_credentials.env
    fi

    # Run docker-compose. This mounts Hadoop, Spark, and other resources inside the docker container.
    docker-compose -f /etc/cluster-docker-compose.yaml up -d

    # Install the Hail additions to Spark conf.
    # OK to do this after pyspark runs; it needs to happen before the Jupyter kernel starts.
    docker exec -u root -d ${JUPYTER_SERVER_NAME} /etc/hail/spark_install_hail.sh

    # Copy the actual service account JSON file into the Jupyter docker container.
    if [ ! -z ${JUPYTER_SERVICE_ACCOUNT_CREDENTIALS} ] ; then
      docker cp /etc/${JUPYTER_SERVICE_ACCOUNT_CREDENTIALS} ${JUPYTER_SERVER_NAME}:/etc/${JUPYTER_SERVICE_ACCOUNT_CREDENTIALS}
    fi

    # If a Jupyter extension was specified, copy it into the jupyter docker container.
    if [ ! -z ${JUPYTER_EXTENSION_URI} ] ; then
      gsutil cp ${JUPYTER_EXTENSION_URI} /etc
      JUPYTER_EXTENSION_ARCHIVE=`basename ${JUPYTER_EXTENSION_URI}`
      docker cp /etc/${JUPYTER_EXTENSION_ARCHIVE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
      docker exec -d ${JUPYTER_SERVER_NAME} ${JUPYTER_HOME}/install-jupyter-extension.sh ${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
    fi

    # If a custom.js was specified, copy it into the jupyter docker container.
    if [ ! -z ${JUPYTER_CUSTOM_JS_URI} ] ; then
      gsutil cp ${JUPYTER_CUSTOM_JS_URI} /etc
      JUPYTER_CUSTOM_JS=`basename ${JUPYTER_CUSTOM_JS_URI}`
      docker exec -d ${JUPYTER_SERVER_NAME} mkdir -p ${JUPYTER_USER_HOME}/.jupyter/custom
      docker cp /etc/${JUPYTER_CUSTOM_JS} ${JUPYTER_SERVER_NAME}:${JUPYTER_USER_HOME}/.jupyter/custom/
    fi

    # If a google_sign_in.js was specified, copy it into the jupyter docker container.
    if [ ! -z ${JUPYTER_GOOGLE_SIGN_IN_JS_URI} ] ; then
      gsutil cp ${JUPYTER_GOOGLE_SIGN_IN_JS_URI} /etc
      JUPYTER_GOOGLE_SIGN_IN_JS=`basename ${JUPYTER_GOOGLE_SIGN_IN_JS_URI}`
      docker exec -d ${JUPYTER_SERVER_NAME} mkdir -p ${JUPYTER_USER_HOME}/.jupyter/custom
      docker cp /etc/${JUPYTER_GOOGLE_SIGN_IN_JS} ${JUPYTER_SERVER_NAME}:${JUPYTER_USER_HOME}/.jupyter/custom/
    fi

    # If a Jupyter user script was specified, copy it into the jupyter docker container.
    if [ ! -z ${JUPYTER_USER_SCRIPT_URI} ] ; then
      gsutil cp ${JUPYTER_USER_SCRIPT_URI} /etc
      JUPYTER_USER_SCRIPT_ARCHIVE=`basename ${JUPYTER_USER_SCRIPT_URI}`
      docker cp /etc/${JUPYTER_USER_SCRIPT_ARCHIVE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_USER_SCRIPT_ARCHIVE}
      docker exec -u root -d ${JUPYTER_SERVER_NAME} chmod +x ${JUPYTER_HOME}/${JUPYTER_USER_SCRIPT_ARCHIVE}
      docker exec -u root -d ${JUPYTER_SERVER_NAME} ${JUPYTER_HOME}/${JUPYTER_USER_SCRIPT_ARCHIVE}
    fi

    docker exec ${JUPYTER_SERVER_NAME} /usr/bin/pyspark
fi




