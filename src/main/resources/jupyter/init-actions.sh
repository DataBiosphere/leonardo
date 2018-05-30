#!/usr/bin/env bash

set -e -x

# adapted from https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/blob/master/datalab/datalab.sh

function update_apt_get() {
  for ((i = 0; i < 10; i++)); do
    if apt-get update; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function log() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@"
}

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
    JUPYTER_NOTEBOOK_CMD="/usr/local/bin/jupyter notebook > ${JUPYTER_USER_HOME}/jupyter.log 2>&1"
    KERNELSPEC_HOME=/usr/local/share/jupyter/kernels

    # The following values are populated by Leo when a cluster is created.
    export CLUSTER_NAME=$(clusterName)
    export GOOGLE_PROJECT=$(googleProject)
    export OWNER_EMAIL=$(userEmailLoginHint)
    export JUPYTER_SERVER_NAME=$(jupyterServerName)
    export PROXY_SERVER_NAME=$(proxyServerName)
    export JUPYTER_DOCKER_IMAGE=$(jupyterDockerImage)
    export PROXY_DOCKER_IMAGE=$(proxyDockerImage)

    JUPYTER_SERVER_CRT=$(jupyterServerCrt)
    JUPYTER_SERVER_KEY=$(jupyterServerKey)
    JUPYTER_ROOT_CA=$(rootCaPem)
    JUPYTER_DOCKER_COMPOSE=$(jupyterDockerCompose)
    JUPYTER_PROXY_SITE_CONF=$(jupyterProxySiteConf)
    JUPYTER_SERVER_EXTENSIONS=$(jupyterServerExtensions)
    JUPYTER_NB_EXTENSIONS=$(jupyterNbExtensions)
    JUPYTER_COMBINED_EXTENSIONS=$(jupyterCombinedExtensions)
    JUPYTER_CUSTOM_JS_URI=$(jupyterCustomJsUri)
    JUPYTER_GOOGLE_SIGN_IN_JS_URI=$(jupyterGoogleSignInJsUri)
    JUPYTER_USER_SCRIPT_URI=$(jupyterUserScriptUri)

    log 'Installing prerequisites...'

    # install Docker
    export DOCKER_CE_VERSION="17.12.0~ce-0~debian"
    update_apt_get
    apt-get install -y -q \
     apt-transport-https \
     ca-certificates \
     curl \
     gnupg2 \
     software-properties-common

    log 'Adding Docker package sources...'

    curl -fsSL https://download.docker.com/linux/$(. /etc/os-release; echo "$ID")/gpg | apt-key add -

    add-apt-repository \
     "deb [arch=amd64] https://download.docker.com/linux/$(. /etc/os-release; echo "$ID") \
     $(lsb_release -cs) \
     stable"

    log 'Installing Docker...'

    update_apt_get
    apt-get install -y -q docker-ce=$DOCKER_CE_VERSION

    log 'Installing Docker Compose...'

    # Install docker-compose
    curl -L https://github.com/docker/compose/releases/download/1.18.0/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose

    log 'Copying secrets from GCS...'

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

    # If either image is hosted in a GCR registry (detected by regex) then
    # authorize docker to interact with gcr.io.
    if grep -qF "gcr.io" <<< "${JUPYTER_DOCKER_IMAGE}${PROXY_DOCKER_IMAGE}" ; then
      log 'Authorizing GCR...'
      gcloud docker --authorize-only
    fi

    log 'Starting up the Jupydocker...'

    # Run docker-compose. This mounts Hadoop, Spark, and other resources inside the docker container.
    docker-compose -f /etc/cluster-docker-compose.yaml up -d

    log 'Installing Jupydocker kernelspecs...'

    # Change Python and PySpark 2 and 3 kernel specs to allow each to have its own spark
    docker exec -u root ${JUPYTER_SERVER_NAME} ${JUPYTER_HOME}/kernelspec.sh ${JUPYTER_HOME} ${KERNELSPEC_HOME}

    log 'Installing Hail additions to Jupydocker spark.conf...'

    # Install the Hail additions to Spark conf.
    docker exec -u root ${JUPYTER_SERVER_NAME} /etc/hail/spark_install_hail.sh

    # Copy the actual service account JSON file into the Jupyter docker container.
    if [ ! -z ${JUPYTER_SERVICE_ACCOUNT_CREDENTIALS} ] ; then
      log 'Copying SA into Docker...'
      docker cp /etc/${JUPYTER_SERVICE_ACCOUNT_CREDENTIALS} ${JUPYTER_SERVER_NAME}:/etc/${JUPYTER_SERVICE_ACCOUNT_CREDENTIALS}
    fi

    #Install NbExtensions
    if [ ! -z "${JUPYTER_NB_EXTENSIONS}" ] ; then
      for ext in ${JUPYTER_NB_EXTENSIONS}
      do
        log 'Installing Jupyter NB extension [$ext]...'
        if [[ $ext == 'gs://'* ]]; then
          gsutil cp $ext /etc
          JUPYTER_EXTENSION_ARCHIVE=`basename $ext`
          docker cp /etc/${JUPYTER_EXTENSION_ARCHIVE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
          docker exec ${JUPYTER_SERVER_NAME} ${JUPYTER_HOME}/jupyter_install_notebook_extension.sh ${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
        else
          docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_HOME}/jupyter_pip_install_notebook_extension.sh $ext
        fi
      done
    fi

    #Install serverExtensions
    if [ ! -z "${JUPYTER_SERVER_EXTENSIONS}" ] ; then
      for ext in ${JUPYTER_SERVER_EXTENSIONS}
      do
        log 'Installing Jupyter server extension [$ext]...'
        if [[ $ext == 'gs://'* ]]; then
          gsutil cp $ext /etc
          JUPYTER_EXTENSION_ARCHIVE=`basename $ext`
          docker cp /etc/${JUPYTER_EXTENSION_ARCHIVE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
          docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_HOME}/jupyter_install_server_extension.sh ${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
        else
          docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_HOME}/jupyter_pip_install_server_extension.sh $ext
        fi
      done
    fi

    #Install combined extensions
    if [ ! -z "${JUPYTER_COMBINED_EXTENSIONS}"  ] ; then
      for ext in ${JUPYTER_COMBINED_EXTENSIONS}
      do
        log 'Installing Jupyter combined extension [$ext]...'
        log $ext
        if [[ $ext == 'gs://'* ]]; then
          gsutil cp $ext /etc
          JUPYTER_EXTENSION_ARCHIVE=`basename $ext`
          docker cp /etc/${JUPYTER_EXTENSION_ARCHIVE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
          docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_HOME}/jupyter_install_combined_extension.sh ${JUPYTER_EXTENSION_ARCHIVE}
        else
          docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_HOME}/jupyter_pip_install_combined_extension.sh $ext
        fi
      done
    fi

    # If a custom.js was specified, copy it into the jupyter docker container.
    if [ ! -z ${JUPYTER_CUSTOM_JS_URI} ] ; then
      log 'Installing Jupyter custom.js...'
      gsutil cp ${JUPYTER_CUSTOM_JS_URI} /etc
      JUPYTER_CUSTOM_JS=`basename ${JUPYTER_CUSTOM_JS_URI}`
      docker exec ${JUPYTER_SERVER_NAME} mkdir -p ${JUPYTER_USER_HOME}/.jupyter/custom
      docker cp /etc/${JUPYTER_CUSTOM_JS} ${JUPYTER_SERVER_NAME}:${JUPYTER_USER_HOME}/.jupyter/custom/
    fi

    # If a google_sign_in.js was specified, copy it into the jupyter docker container.
    if [ ! -z ${JUPYTER_GOOGLE_SIGN_IN_JS_URI} ] ; then
      log 'Installing Google sign in extension...'
      gsutil cp ${JUPYTER_GOOGLE_SIGN_IN_JS_URI} /etc
      JUPYTER_GOOGLE_SIGN_IN_JS=`basename ${JUPYTER_GOOGLE_SIGN_IN_JS_URI}`
      docker exec ${JUPYTER_SERVER_NAME} mkdir -p ${JUPYTER_USER_HOME}/.jupyter/custom
      docker cp /etc/${JUPYTER_GOOGLE_SIGN_IN_JS} ${JUPYTER_SERVER_NAME}:${JUPYTER_USER_HOME}/.jupyter/custom/
    fi

    # If a Jupyter user script was specified, copy it into the jupyter docker container.
    if [ ! -z ${JUPYTER_USER_SCRIPT_URI} ] ; then
      log 'Installing Jupyter user extension [$JUPYTER_USER_SCRIPT_URI]...'
      gsutil cp ${JUPYTER_USER_SCRIPT_URI} /etc
      JUPYTER_USER_SCRIPT=`basename ${JUPYTER_USER_SCRIPT_URI}`
      docker cp /etc/${JUPYTER_USER_SCRIPT} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_USER_SCRIPT}
      docker exec -u root ${JUPYTER_SERVER_NAME} chmod +x ${JUPYTER_HOME}/${JUPYTER_USER_SCRIPT}
      docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_HOME}/${JUPYTER_USER_SCRIPT}
    fi


    log 'Starting Jupyter Notebook...'
    docker exec -d ${JUPYTER_SERVER_NAME} /bin/sh -c "${JUPYTER_NOTEBOOK_CMD}"
    log 'All done!'
fi




