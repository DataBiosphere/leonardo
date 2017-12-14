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
    # The following values are populated by Leo when a cluster is created.
    export CLUSTER_NAME=$(clusterName)
    export GOOGLE_PROJECT=$(googleProject)
    export JUPYTER_SERVER_NAME=$(jupyterServerName)
    export PROXY_SERVER_NAME=$(proxyServerName)
    export JUPYTER_DOCKER_IMAGE=$(jupyterDockerImage)
    export PROXY_DOCKER_IMAGE=$(proxyDockerImage)
    export COMPOSE_API_VERSION=1.18   # This is set because docker and docker-compose don't work together otherwise

    JUPYTER_SERVER_CRT=$(jupyterServerCrt)
    JUPYTER_SERVER_KEY=$(jupyterServerKey)
    JUPYTER_ROOT_CA=$(rootCaPem)
    JUPYTER_DOCKER_COMPOSE=$(jupyterDockerCompose)
    JUPYTER_PROXY_SITE_CONF=$(jupyterProxySiteConf)
    JUPYTER_INSTALL_EXTENSION_SCRIPT=$(jupyterInstallExtensionScript)
    JUPYTER_EXTENSION_URI=$(jupyterExtensionUri)
    JUPYTER_CUSTOM_JS_URI=$(jupyterCustomJsUri)
    JUPYTER_GOOGLE_SIGN_IN_JS_URI=$(jupyterGoogleSignInJsUri)

    apt-get update
    apt-get install -y -q docker.io
    mkdir /work
    mkdir /certs
    chmod a+wx /work

    # Add the certificates from the bucket to the VM. They are used by the docker-compose file
    gsutil cp ${JUPYTER_SERVER_CRT} /certs
    gsutil cp ${JUPYTER_SERVER_KEY} /certs
    gsutil cp ${JUPYTER_ROOT_CA} /certs
    gsutil cp ${JUPYTER_PROXY_SITE_CONF} /etc
    gsutil cp ${JUPYTER_DOCKER_COMPOSE} /etc
    gsutil cp ${JUPYTER_INSTALL_EXTENSION_SCRIPT} /etc
    gsutil cp ${JUPYTER_CUSTOM_JS_URI} /etc
    gsutil cp ${JUPYTER_GOOGLE_SIGN_IN_JS_URI} /etc

    # Make sure the install-jupyter-extension.sh script is executable
    chmod +x /etc/install-jupyter-extension.sh

    # Install docker-compose
    curl -L https://github.com/docker/compose/releases/download/1.15.0/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose

    # Needed because docker-compose can't handle symlinks
    touch /hadoop_gcs_connector_metadata_cache
    touch auth_openidc.conf

    # If we have a service account JSON file, create an .env file to set GOOGLE_APPLICATION_CREDENTIALS
    # in the docker container. Otherwise, we should _not_ set this environment variable so it uses the
    # credentials on the metadata server.
    if [ ! -z ${JUPYTER_SERVICE_ACCOUNT_CREDENTIALS} ] ; then
      echo "GOOGLE_APPLICATION_CREDENTIALS=/etc/${JUPYTER_SERVICE_ACCOUNT_CREDENTIALS}" > /etc/google_application_credentials.env
    fi

    # Run docker-compose
    docker-compose -f /etc/cluster-docker-compose.yaml up -d

    # Copy the actual service account JSON file into the Jupyter docker container.
    if [ ! -z ${JUPYTER_SERVICE_ACCOUNT_CREDENTIALS} ] ; then
      docker cp /etc/${JUPYTER_SERVICE_ACCOUNT_CREDENTIALS} ${JUPYTER_SERVER_NAME}:/etc/${JUPYTER_SERVICE_ACCOUNT_CREDENTIALS}
    fi

    # If a Jupyter extension was specified, copy it into the jupyter docker container.
    # Note: docker-compose doesn't appear to have the ability to execute a command after run, so we do this explicitly with docker exec commands.
    # See https://github.com/docker/compose/issues/1809
    if [ ! -z ${JUPYTER_EXTENSION_URI} ] ; then
      gsutil cp ${JUPYTER_EXTENSION_URI} /etc
      JUPYTER_EXTENSION_ARCHIVE=`basename ${JUPYTER_EXTENSION_URI}`
      docker cp /etc/${JUPYTER_EXTENSION_ARCHIVE} ${JUPYTER_SERVER_NAME}:/etc/${JUPYTER_EXTENSION_ARCHIVE}
      docker exec -d ${JUPYTER_SERVER_NAME} /etc/install-jupyter-extension.sh /etc/${JUPYTER_EXTENSION_ARCHIVE}
    fi
fi




