#!/usr/bin/env bash

set -e -x

# adapted from https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/blob/master/datalab/datalab.sh

# Retry a command up to a specific number of times until it exits successfully,
# with exponential back off.
#
# $ retry 5 echo "Hello"
# Hello
#
# $ retry 5 false
# Retry 1/5 exited 1, retrying in 2 seconds...
# Retry 2/5 exited 1, retrying in 4 seconds...
# Retry 3/5 exited 1, retrying in 8 seconds...
# Retry 4/5 exited 1, retrying in 16 seconds...
# Retry 5/5 exited 1, no more retries left.
#
function retry {
  local retries=$1
  shift

  for ((i = 1; i <= $retries; i++)); do
    # run with an 'or' so set -e doesn't abort the bash script on errors
    exit=0
    "$@" || exit=$?
    if [ $exit -eq 0 ]; then
      return 0
    fi
    wait=$((2 ** $i))
    if [ $i -eq $retries ]; then
      log "Retry $i/$retries exited $exit, no more retries left."
      break
    fi
    log "Retry $i/$retries exited $exit, retrying in $wait seconds..."
    sleep $wait
  done
  return 1
}

function log() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@"
}

function betterAptGet() {
  if ! { apt-get update 2>&1 || echo E: update failed; } | grep -q '^[WE]:'; then
    return 0
  else
    return 1
  fi
}

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)

# If a Google credentials file was specified, grab the service account json file and set the GOOGLE_APPLICATION_CREDENTIALS EV.
# This overrides the credentials on the metadata server.
# This needs to happen on master and worker nodes.
SERVICE_ACCOUNT_CREDENTIALS=$(jupyterServiceAccountCredentials)
if [ ! -z ${SERVICE_ACCOUNT_CREDENTIALS} ] ; then
  gsutil cp ${SERVICE_ACCOUNT_CREDENTIALS} /etc
  SERVICE_ACCOUNT_CREDENTIALS=`basename ${SERVICE_ACCOUNT_CREDENTIALS}`
  export GOOGLE_APPLICATION_CREDENTIALS=/etc/${SERVICE_ACCOUNT_CREDENTIALS}
fi

# Only initialize Jupyter docker containers on the master
if [[ "${ROLE}" == 'Master' ]]; then
    JUPYTER_HOME=/etc/jupyter
    JUPYTER_SCRIPTS=${JUPYTER_HOME}/scripts
    JUPYTER_USER_HOME=/home/jupyter-user
    KERNELSPEC_HOME=/usr/local/share/jupyter/kernels

    # The following values are populated by Leo when a cluster is created.
    export CLUSTER_NAME=$(clusterName)
    export GOOGLE_PROJECT=$(googleProject)
    export OWNER_EMAIL=$(userEmailLoginHint)
    export JUPYTER_SERVER_NAME=$(jupyterServerName)
    export RSTUDIO_SERVER_NAME=$(rstudioServerName)
    export PROXY_SERVER_NAME=$(proxyServerName)
    export JUPYTER_DOCKER_IMAGE=$(jupyterDockerImage)
    export RSTUDIO_DOCKER_IMAGE=$(rstudioDockerImage)
    export PROXY_DOCKER_IMAGE=$(proxyDockerImage)

    SERVER_CRT=$(jupyterServerCrt)
    SERVER_KEY=$(jupyterServerKey)
    ROOT_CA=$(rootCaPem)
    JUPYTER_DOCKER_COMPOSE=$(jupyterDockerCompose)
    RSTUDIO_DOCKER_COMPOSE=$(rstudioDockerCompose)
    PROXY_DOCKER_COMPOSE=$(proxyDockerCompose)
    PROXY_SITE_CONF=$(proxySiteConf)
    JUPYTER_SERVER_EXTENSIONS=$(jupyterServerExtensions)
    JUPYTER_NB_EXTENSIONS=$(jupyterNbExtensions)
    JUPYTER_COMBINED_EXTENSIONS=$(jupyterCombinedExtensions)
    JUPYTER_LAB_EXTENSIONS=$(jupyterLabExtensions)
    GOOGLE_SIGN_IN_JS_URI=$(googleSignInJsUri)
    JUPYTER_GOOGLE_PLUGIN_URI=$(jupyterGooglePluginUri)
    JUPYTER_LAB_GOOGLE_PLUGIN_URI=$(jupyterLabGooglePluginUri)
    JUPYTER_USER_SCRIPT_URI=$(jupyterUserScriptUri)
    JUPYTER_NOTEBOOK_CONFIG_URI=$(jupyterNotebookConfigUri)

    log 'Installing prerequisites...'

    # Obtain the latest valid apt-key.gpg key file from https://packages.cloud.google.com to work
    # around intermittent apt authentication errors. See:
    # https://cloud.google.com/compute/docs/troubleshooting/known-issues
    retry 5 curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
    retry 5 apt-key update

    # install Docker
    # https://docs.docker.com/install/linux/docker-ce/debian/
    export DOCKER_CE_VERSION="18.06.2~ce~3-0~debian"
    retry 5 betterAptGet
    retry 5 apt-get install -y -q \
     apt-transport-https \
     ca-certificates \
     curl \
     gnupg2 \
     software-properties-common

    log 'Adding Docker package sources...'

    retry 5 curl -fsSL https://download.docker.com/linux/$(. /etc/os-release; echo "$ID")/gpg | apt-key add -

    add-apt-repository \
     "deb [arch=amd64] https://download.docker.com/linux/$(. /etc/os-release; echo "$ID") \
     $(lsb_release -cs) \
     stable"

    log 'Installing Docker...'

    retry 5 betterAptGet
    retry 5 apt-get install -y -q docker-ce=$DOCKER_CE_VERSION

    log 'Installing Docker Compose...'

    # Install docker-compose
    # https://docs.docker.com/compose/install/#install-compose
    retry 5 curl -L "https://github.com/docker/compose/releases/download/1.22.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose

    log 'Copying secrets from GCS...'

    mkdir /work
    mkdir /certs
    chmod a+wx /work

    # Add the certificates from the bucket to the VM. They are used by the docker-compose file
    gsutil cp ${SERVER_CRT} /certs
    gsutil cp ${SERVER_KEY} /certs
    gsutil cp ${ROOT_CA} /certs
    gsutil cp ${PROXY_SITE_CONF} /etc
    gsutil cp ${JUPYTER_DOCKER_COMPOSE} /etc
    gsutil cp ${RSTUDIO_DOCKER_COMPOSE} /etc
    gsutil cp ${PROXY_DOCKER_COMPOSE} /etc

    # Needed because docker-compose can't handle symlinks
    touch /hadoop_gcs_connector_metadata_cache
    touch auth_openidc.conf

    # If we have a service account JSON file, create an .env file to set GOOGLE_APPLICATION_CREDENTIALS
    # in the docker container. Otherwise, we should _not_ set this environment variable so it uses the
    # credentials on the metadata server.
    if [ ! -z ${SERVICE_ACCOUNT_CREDENTIALS} ] ; then
      echo "GOOGLE_APPLICATION_CREDENTIALS=/etc/${SERVICE_ACCOUNT_CREDENTIALS}" > /etc/google_application_credentials.env
      if [ ! -z ${JUPYTER_DOCKER_IMAGE} ] ; then
        log 'Copying SA into Jupyter Docker...'
        docker cp /etc/${SERVICE_ACCOUNT_CREDENTIALS} ${JUPYTER_SERVER_NAME}:/etc/${SERVICE_ACCOUNT_CREDENTIALS}
      fi
      if [ ! -z ${RSTUDIO_DOCKER_IMAGE} ] ; then
        log 'Copying SA into RStudio Docker...'
        docker cp /etc/${SERVICE_ACCOUNT_CREDENTIALS} ${RSTUDIO_SERVER_NAME}:/etc/${SERVICE_ACCOUNT_CREDENTIALS}
      fi
    else
      echo "" > /etc/google_application_credentials.env
    fi

    # If any image is hosted in a GCR registry (detected by regex) then
    # authorize docker to interact with gcr.io.
    if grep -qF "gcr.io" <<< "${JUPYTER_DOCKER_IMAGE}${RSTUDIO_DOCKER_IMAGE}${PROXY_DOCKER_IMAGE}" ; then
      log 'Authorizing GCR...'
      gcloud docker --authorize-only
    fi

    log 'Starting up the Jupydocker...'

    # Run docker-compose for each specified compose file.
    # Note the `docker-compose pull` is retried to avoid intermittent network errors, but
    # `docker-compose up` is not retried.
    COMPOSE_FILES=(-f /etc/`basename ${PROXY_DOCKER_COMPOSE}`)
    if [ ! -z ${JUPYTER_DOCKER_IMAGE} ] ; then
      COMPOSE_FILES+=(-f /etc/`basename ${JUPYTER_DOCKER_COMPOSE}`)
    fi
    if [ ! -z ${RSTUDIO_DOCKER_IMAGE} ] ; then
      COMPOSE_FILES+=(-f /etc/`basename ${RSTUDIO_DOCKER_COMPOSE}`)
    fi

    docker-compose "${COMPOSE_FILES[@]}" config
    retry 5 docker-compose "${COMPOSE_FILES[@]}" pull
    docker-compose "${COMPOSE_FILES[@]}" up -d

    # Jupyter-specific setup, only do if Jupyter is installed
    if [ ! -z ${JUPYTER_DOCKER_IMAGE} ] ; then
      log 'Installing Jupydocker kernelspecs...'

      # Change Python and PySpark 2 and 3 kernel specs to allow each to have its own spark
      retry 3 docker exec -u root ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/kernel/kernelspec.sh ${JUPYTER_SCRIPTS}/kernel ${KERNELSPEC_HOME}

      log 'Installing Hail additions to Jupydocker spark.conf...'

      # Install the Hail additions to Spark conf.
      retry 3 docker exec -u root ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/hail/spark_install_hail.sh

      #Install NbExtensions
      if [ ! -z "${JUPYTER_NB_EXTENSIONS}" ] ; then
        for ext in ${JUPYTER_NB_EXTENSIONS}
        do
          log 'Installing Jupyter NB extension [$ext]...'
          if [[ $ext == 'gs://'* ]]; then
            gsutil cp $ext /etc
            JUPYTER_EXTENSION_ARCHIVE=`basename $ext`
            docker cp /etc/${JUPYTER_EXTENSION_ARCHIVE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
            retry 3 docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_notebook_extension.sh ${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
          elif [[ $ext == 'http://'* || $ext == 'https://'* ]]; then
            JUPYTER_EXTENSION_FILE=`basename $ext`
            curl $ext -o /etc/${JUPYTER_EXTENSION_FILE}
            docker cp /etc/${JUPYTER_EXTENSION_FILE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_EXTENSION_FILE}
            retry 3 docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_notebook_extension.sh ${JUPYTER_HOME}/${JUPYTER_EXTENSION_FILE}
          else
            retry 3 docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_pip_install_notebook_extension.sh $ext
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
            retry 3 docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_server_extension.sh ${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
          else
            retry 3 docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_pip_install_server_extension.sh $ext
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
            retry 3 docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_combined_extension.sh ${JUPYTER_EXTENSION_ARCHIVE}
          else
            retry 3 docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_pip_install_combined_extension.sh $ext
          fi
        done
      fi
      
      #Install lab extensions
      #Note: lab extensions need to installed as jupyter user, not root
      if [ ! -z "${JUPYTER_LAB_EXTENSIONS}" ] ; then
        for ext in ${JUPYTER_LAB_EXTENSIONS}
        do
          log 'Installing JupyterLab extension [$ext]...'
          pwd
          if [[ $ext == 'gs://'* ]]; then
            gsutil cp -r $ext /etc
            JUPYTER_EXTENSION_ARCHIVE=`basename $ext`
            docker cp /etc/${JUPYTER_EXTENSION_ARCHIVE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
            retry 3 docker exec ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_lab_extension.sh ${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
          elif [[ $ext == 'http://'* || $ext == 'https://'* ]]; then
            JUPYTER_EXTENSION_FILE=`basename $ext`
            curl $ext -o /etc/${JUPYTER_EXTENSION_FILE}
            docker cp /etc/${JUPYTER_EXTENSION_FILE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_EXTENSION_FILE}
            retry 3 docker exec ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_lab_extension.sh ${JUPYTER_HOME}/${JUPYTER_EXTENSION_FILE}

          else
            retry 3 docker exec ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_lab_extension.sh $ext
          fi
        done
      fi

      retry 3 docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/install_jupyter_contrib_nbextensions.sh

      # If a google_sign_in.js was specified, copy it into the jupyter docker container.
      if [ ! -z ${GOOGLE_SIGN_IN_JS_URI} ] ; then
        log 'Installing Google sign in extension...'
        gsutil cp ${GOOGLE_SIGN_IN_JS_URI} /etc
        GOOGLE_SIGN_IN_JS=`basename ${GOOGLE_SIGN_IN_JS_URI}`
        retry 3 docker exec ${JUPYTER_SERVER_NAME} mkdir -p ${JUPYTER_USER_HOME}/.jupyter/custom
        docker cp /etc/${GOOGLE_SIGN_IN_JS} ${JUPYTER_SERVER_NAME}:${JUPYTER_USER_HOME}/.jupyter/custom/
      fi

      # If a jupyter_google_plugin.js was specified, copy it into the jupyter docker container.
      if [ ! -z ${JUPYTER_GOOGLE_PLUGIN_URI} ] ; then
        log 'Installing jupyter_google_plugin.js extension...'
        gsutil cp ${JUPYTER_GOOGLE_PLUGIN_URI} /etc
        JUPYTER_GOOGLE_PLUGIN=`basename ${JUPYTER_GOOGLE_PLUGIN_URI}`
        retry 3 docker exec ${JUPYTER_SERVER_NAME} mkdir -p ${JUPYTER_USER_HOME}/.jupyter/custom
        # note this needs to be named custom.js in the container
        docker cp /etc/${JUPYTER_GOOGLE_PLUGIN} ${JUPYTER_SERVER_NAME}:${JUPYTER_USER_HOME}/.jupyter/custom/custom.js
      fi

      # If a jupyterlab_google_plugin.js was specified, install it as a Lab extension
      if [ ! -z ${JUPYTER_LAB_GOOGLE_PLUGIN_URI} ] ; then
        log 'Installing jupyterlab_google_plugin.js extension...'
        gsutil cp ${JUPYTER_LAB_GOOGLE_PLUGIN_URI} /etc
        JUPYTER_LAB_GOOGLE_PLUGIN=`basename ${JUPYTER_LAB_GOOGLE_PLUGIN_URI}`
        docker cp /etc/${JUPYTER_LAB_GOOGLE_PLUGIN} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_LAB_GOOGLE_PLUGIN}
        retry 3 docker exec -u root ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_lab_extension.sh ${JUPYTER_HOME}/${JUPYTER_LAB_GOOGLE_PLUGIN}
      fi

      if [ ! -z ${JUPYTER_NOTEBOOK_CONFIG_URI} ] ; then
        log 'Copy Jupyter notebook config...'
        gsutil cp ${JUPYTER_NOTEBOOK_CONFIG_URI} /etc
        JUPYTER_NOTEBOOK_CONFIG=`basename ${JUPYTER_NOTEBOOK_CONFIG_URI}`
        docker cp /etc/${JUPYTER_NOTEBOOK_CONFIG} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/
      fi

      # If a Jupyter user script was specified, copy it into the jupyter docker container.
      if [ ! -z ${JUPYTER_USER_SCRIPT_URI} ] ; then
        log 'Installing Jupyter user extension [$JUPYTER_USER_SCRIPT_URI]...'
        gsutil cp ${JUPYTER_USER_SCRIPT_URI} /etc
        JUPYTER_USER_SCRIPT=`basename ${JUPYTER_USER_SCRIPT_URI}`
        docker cp /etc/${JUPYTER_USER_SCRIPT} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_USER_SCRIPT}
        retry 3 docker exec -u root ${JUPYTER_SERVER_NAME} chmod +x ${JUPYTER_HOME}/${JUPYTER_USER_SCRIPT}
        retry 3 docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_HOME}/${JUPYTER_USER_SCRIPT}
      fi

      log 'Starting Jupyter Notebook...'
      retry 3 docker exec -d ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/run-jupyter.sh
      log 'All done!'
    fi
fi

# Install Python 3.6 on the master and worker VMs
export PYTHON_VERSION=3.6.8
log "Installing Python $PYTHON_VERSION on the VM..."
wget -O python.tar.xz "https://www.python.org/ftp/python/${PYTHON_VERSION%%[a-z]*}/Python-$PYTHON_VERSION.tar.xz"
mkdir -p /usr/src/python
tar -xJC /usr/src/python --strip-components=1 -f python.tar.xz
rm python.tar.xz
cd /usr/src/python
gnuArch="$(dpkg-architecture --query DEB_BUILD_GNU_TYPE)"
./configure \
  --build="$gnuArch" \
  --enable-loadable-sqlite-extensions \
  --enable-shared \
  --with-system-expat \
  --with-system-ffi \
  --without-ensurepip
make -j "$(nproc)"
make install
ldconfig
python3 --version

log "Finished installing Python $PYTHON_VERSION"
