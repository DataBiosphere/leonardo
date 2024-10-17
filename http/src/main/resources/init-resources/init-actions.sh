#!/usr/bin/env bash

set -e -x

#
# This init script instantiates the tool (e.g. Jupyter) docker images on the Dataproc cluster master node.
# Adapted from https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/blob/master/datalab/datalab.sh
#

#
# Functions
#

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

function apply_user_script() {
  local CONTAINER_NAME=$1
  local TARGET_DIR=$2

  log "Running user script $USER_SCRIPT_URI in $CONTAINER_NAME container..."
  USER_SCRIPT=`basename ${USER_SCRIPT_URI}`
  if [[ "$USER_SCRIPT_URI" == 'gs://'* ]]; then
    gsutil cp ${USER_SCRIPT_URI} /etc
  else
    curl $USER_SCRIPT_URI -o /etc/${USER_SCRIPT}
  fi
  docker cp /etc/${USER_SCRIPT} ${CONTAINER_NAME}:${TARGET_DIR}/${USER_SCRIPT}
  retry 3 docker exec -u root ${CONTAINER_NAME} chmod +x ${TARGET_DIR}/${USER_SCRIPT}

  # Execute the user script as privileged to allow for deeper customization of VM behavior, e.g. installing
  # network egress throttling. As docker is not a security layer, it is assumed that a determined attacker
  # can gain full access to the VM already, so using this flag is not a significant escalation.
  EXIT_CODE=0
  docker exec --privileged -u root -e PIP_USER=false ${CONTAINER_NAME} ${TARGET_DIR}/${USER_SCRIPT} &> us_output.txt || EXIT_CODE=$?

  if [ $EXIT_CODE -ne 0 ]; then
    log "User script failed with exit code $EXIT_CODE. Output is saved to $USER_SCRIPT_OUTPUT_URI."
    retry 3 gsutil -h "x-goog-meta-passed":"false" cp us_output.txt ${USER_SCRIPT_OUTPUT_URI}
    exit $EXIT_CODE
  else
    retry 3 gsutil -h "x-goog-meta-passed":"true" cp us_output.txt ${USER_SCRIPT_OUTPUT_URI}
  fi
}

function apply_start_user_script() {
  local CONTAINER_NAME=$1
  local TARGET_DIR=$2

  log "Running start user script $START_USER_SCRIPT_URI in $CONTAINER_NAME container..."
  START_USER_SCRIPT=`basename ${START_USER_SCRIPT_URI}`
  if [[ "$START_USER_SCRIPT_URI" == 'gs://'* ]]; then
    gsutil cp ${START_USER_SCRIPT_URI} /etc
  else
    curl $START_USER_SCRIPT_URI -o /etc/${START_USER_SCRIPT}
  fi
  docker cp /etc/${START_USER_SCRIPT} ${CONTAINER_NAME}:${TARGET_DIR}/${START_USER_SCRIPT}
  retry 3 docker exec -u root ${CONTAINER_NAME} chmod +x ${TARGET_DIR}/${START_USER_SCRIPT}

  # Keep in sync with startup.sh
  EXIT_CODE=0
  docker exec --privileged -u root -e PIP_USER=false ${CONTAINER_NAME} ${TARGET_DIR}/${START_USER_SCRIPT} &> start_output.txt || EXIT_CODE=$?
  if [ $EXIT_CODE -ne 0 ]; then
    echo "User start script failed with exit code ${EXIT_CODE}. Output is saved to ${START_USER_SCRIPT_OUTPUT_URI}"
    retry 3 gsutil -h "x-goog-meta-passed":"false" cp start_output.txt ${START_USER_SCRIPT_OUTPUT_URI}
    exit $EXIT_CODE
  else
    retry 3 gsutil -h "x-goog-meta-passed":"true" cp start_output.txt ${START_USER_SCRIPT_OUTPUT_URI}
  fi
}

#
# Main
#

#
# Array for instrumentation
# UPDATE THIS IF YOU ADD MORE STEPS:
# currently the steps are:
# START init,
# .. after env setup
# .. after copying files from google and into docker
# .. after docker compose
# .. after welder start
# .. after hail and spark
# .. after nbextension install
# .. after server extension install
# .. after combined extension install
# .. after user script
# .. after lab extension install
# .. after jupyter notebook start
# END
STEP_TIMINGS=($(date +%s))
# temp workaround for https://github.com/docker/compose/issues/5930
export CLOUDSDK_PYTHON=python3

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)

# If a Google credentials file was specified, grab the service account json file and set the GOOGLE_APPLICATION_CREDENTIALS env var.
# This overrides the credentials on the metadata server.
# This needs to happen on master and worker nodes.
SERVICE_ACCOUNT_CREDENTIALS=$(jupyterServiceAccountCredentials)
if [ ! -z "$SERVICE_ACCOUNT_CREDENTIALS" ] ; then
  gsutil cp ${SERVICE_ACCOUNT_CREDENTIALS} /etc
  SERVICE_ACCOUNT_CREDENTIALS=`basename ${SERVICE_ACCOUNT_CREDENTIALS}`
  export GOOGLE_APPLICATION_CREDENTIALS=/etc/${SERVICE_ACCOUNT_CREDENTIALS}
fi

# Only initialize tool and proxy docker containers on the master
if [[ "${ROLE}" == 'Master' ]]; then
    JUPYTER_HOME=/etc/jupyter
    JUPYTER_SCRIPTS=${JUPYTER_HOME}/scripts
    KERNELSPEC_HOME=/usr/local/share/jupyter/kernels

    # The following values are populated by Leo when a cluster is created.
    export JUPYTER_USER_HOME=$(jupyterHomeDirectory)
    export CLUSTER_NAME=$(clusterName)
    export RUNTIME_NAME=$(clusterName)
    export GOOGLE_PROJECT=$(googleProject)
    export STAGING_BUCKET=$(stagingBucketName)
    export OWNER_EMAIL=$(loginHint)
    export PET_SA_EMAIL=$(petSaEmail)
    export JUPYTER_SERVER_NAME=$(jupyterServerName)
    export RSTUDIO_SERVER_NAME=$(rstudioServerName)
    export PROXY_SERVER_NAME=$(proxyServerName)
    export WELDER_SERVER_NAME=$(welderServerName)
    export CRYPTO_DETECTOR_SERVER_NAME=$(cryptoDetectorServerName)
    export JUPYTER_DOCKER_IMAGE=$(jupyterDockerImage)
    export RSTUDIO_DOCKER_IMAGE=$(rstudioDockerImage)
    export PROXY_DOCKER_IMAGE=$(proxyDockerImage)
    export WELDER_DOCKER_IMAGE=$(welderDockerImage)
    export CRYPTO_DETECTOR_DOCKER_IMAGE=$(cryptoDetectorDockerImage)
    export WELDER_ENABLED=$(welderEnabled)
    export NOTEBOOKS_DIR=$(notebooksDir)
    export MEM_LIMIT=$(memLimit)
    export SHM_SIZE=$(shmSize)
    export WELDER_MEM_LIMIT=$(welderMemLimit)
    export PROXY_SERVER_HOST_NAME=$(proxyServerHostName)
    export CERT_DIRECTORY='/certs'
    export WORK_DIRECTORY='/work'
    export DOCKER_COMPOSE_FILES_DIRECTORY='/etc'
    PROXY_SITE_CONF=$(proxySiteConf)
    export HOST_PROXY_SITE_CONF_FILE_PATH=${DOCKER_COMPOSE_FILES_DIRECTORY}/`basename ${PROXY_SITE_CONF}`
    if [ ! -z "$RSTUDIO_DOCKER_IMAGE" ] ; then
      export SHOULD_BACKGROUND_SYNC="true"
    else
      export SHOULD_BACKGROUND_SYNC="false"
    fi

    SERVER_CRT=$(proxyServerCrt)
    SERVER_KEY=$(proxyServerKey)
    ROOT_CA=$(rootCaPem)
    JUPYTER_DOCKER_COMPOSE=$(jupyterDockerCompose)
    RSTUDIO_DOCKER_COMPOSE=$(rstudioDockerCompose)
    PROXY_DOCKER_COMPOSE=$(proxyDockerCompose)
    WELDER_DOCKER_COMPOSE=$(welderDockerCompose)
    PROXY_SITE_CONF=$(proxySiteConf)
    JUPYTER_SERVER_EXTENSIONS=$(jupyterServerExtensions)
    JUPYTER_NB_EXTENSIONS=$(jupyterNbExtensions)
    JUPYTER_COMBINED_EXTENSIONS=$(jupyterCombinedExtensions)
    JUPYTER_LAB_EXTENSIONS=$(jupyterLabExtensions)
    USER_SCRIPT_URI=$(userScriptUri)
    USER_SCRIPT_OUTPUT_URI=$(userScriptOutputUri)
    START_USER_SCRIPT_URI=$(startUserScriptUri)
    # Include a timestamp suffix to differentiate different startup logs across restarts.
    START_USER_SCRIPT_OUTPUT_URI="$(startUserScriptOutputUri)"
    JUPYTER_NOTEBOOK_FRONTEND_CONFIG_URI=$(jupyterNotebookFrontendConfigUri)
    CUSTOM_ENV_VARS_CONFIG_URI=$(customEnvVarsConfigUri)
    RSTUDIO_SCRIPTS=/etc/rstudio/scripts
    RSTUDIO_USER_HOME=/home/rstudio
    INIT_BUCKET_NAME=$(initBucketName)

    STEP_TIMINGS+=($(date +%s))

    log 'Copying secrets from GCS...'

    mkdir /work
    mkdir /certs
    chmod a+rwx /work

    # Add the certificates from the bucket to the VM. They are used by the docker-compose file
    gsutil cp ${SERVER_CRT} /certs
    gsutil cp ${SERVER_KEY} /certs
    gsutil cp ${ROOT_CA} /certs
    gsutil cp gs://${INIT_BUCKET_NAME}/* ${DOCKER_COMPOSE_FILES_DIRECTORY}


    # Needed because docker-compose can't handle symlinks
    touch /hadoop_gcs_connector_metadata_cache
    touch auth_openidc.conf

    # Add stack driver configuration for welder
    tee /etc/google-fluentd/config.d/welder.conf << END
<source>
 @type tail
 format json
 path /work/welder.log
 pos_file /var/tmp/fluentd.welder.pos
 read_from_head true
 tag welder
</source>
END

    # Add stack driver configuration for jupyter
    tee /etc/google-fluentd/config.d/jupyter.conf << END
<source>
 @type tail
 format none
 path /work/jupyter.log
 pos_file /var/tmp/fluentd.jupyter.pos
 read_from_head true
 tag jupyter
</source>
END

    # Add stack driver configuration for user startup and shutdown scripts
    tee /etc/google-fluentd/config.d/daemon.conf << END
<source>
 @type tail
 format none
 path /var/log/daemon.log
 pos_file /var/tmp/fluentd.google.user.daemon.pos
 read_from_head true
 tag daemon
</source>
END

    service google-fluentd reload

    # Install env var config
    if [ ! -z ${CUSTOM_ENV_VARS_CONFIG_URI} ] ; then
      log 'Copy custom env vars config...'
      gsutil cp ${CUSTOM_ENV_VARS_CONFIG_URI} /var
    fi

    if [ ! -z ${SERVICE_ACCOUNT_CREDENTIALS} ] ; then
      echo "GOOGLE_APPLICATION_CREDENTIALS=/var/${SERVICE_ACCOUNT_CREDENTIALS}" > /var/google_application_credentials.env
    else
      echo "" > /var/google_application_credentials.env
    fi

    # Install RStudio license file, if specified
    if [ ! -z "$RSTUDIO_DOCKER_IMAGE" ] ; then
      STAT_EXIT_CODE=0
      gsutil -q stat ${RSTUDIO_LICENSE_FILE} || STAT_EXIT_CODE=$?
      if [ $STAT_EXIT_CODE -eq 0 ] ; then
        echo "Using RStudio license file $RSTUDIO_LICENSE_FILE"
        gsutil cp ${RSTUDIO_LICENSE_FILE} /etc/rstudio-license-file.lic
      else
        echo "" > /etc/rstudio-license-file.lic
      fi
    fi

    # If any image is hosted in a GAR registry (detected by regex) then
    # authorize docker to interact with gcr.io.
    # NOTE: GCR images are now hosted on GAR, but the file paths haven't changed, they automatically redirect.
    if grep -qF "gcr.io" <<< "${JUPYTER_DOCKER_IMAGE}${RSTUDIO_DOCKER_IMAGE}${PROXY_DOCKER_IMAGE}${WELDER_DOCKER_IMAGE}" ; then
      log 'Authorizing GCR/GAR...'
      gcloud auth configure-docker
    fi

    STEP_TIMINGS+=($(date +%s))

    log 'Starting up the Jupydocker...'

    # Run docker-compose for each specified compose file.
    # Note the `docker-compose pull` is retried to avoid intermittent network errors, but
    # `docker-compose up` is not retried.
    COMPOSE_FILES=(-f /etc/`basename ${PROXY_DOCKER_COMPOSE}`)

    cat /etc/`basename ${PROXY_DOCKER_COMPOSE}`

    if [ ! -z ${WELDER_DOCKER_IMAGE} ] && [ "${WELDER_ENABLED}" == "true" ] ; then
      COMPOSE_FILES+=(-f /etc/`basename ${WELDER_DOCKER_COMPOSE}`)
      cat /etc/`basename ${WELDER_DOCKER_COMPOSE}`
    fi

    if [ ! -z ${JUPYTER_DOCKER_IMAGE} ] ; then
      TOOL_SERVER_NAME=${JUPYTER_SERVER_NAME}
      COMPOSE_FILES+=(-f /etc/`basename ${JUPYTER_DOCKER_COMPOSE}`)
      cat /etc/`basename ${JUPYTER_DOCKER_COMPOSE}`
    fi

    if [ ! -z ${RSTUDIO_DOCKER_IMAGE} ] ; then
      TOOL_SERVER_NAME=${RSTUDIO_SERVER_NAME}
      COMPOSE_FILES+=(-f /etc/`basename ${RSTUDIO_DOCKER_COMPOSE}`)
      cat /etc/`basename ${RSTUDIO_DOCKER_COMPOSE}`
    fi

    retry 5 docker-compose "${COMPOSE_FILES[@]}" config
    retry 5 docker-compose "${COMPOSE_FILES[@]}" pull
    retry 5 docker-compose "${COMPOSE_FILES[@]}" up -d

    # Start up crypto detector, if enabled.
    # This should be started after other containers.
    # Use `docker run` instead of docker-compose so we can link it to the Jupyter/RStudio container's network.
    # See https://github.com/broadinstitute/terra-cryptomining-security-alerts/tree/master/v2
    if [ ! -z "$CRYPTO_DETECTOR_DOCKER_IMAGE" ] ; then
      docker run --name=${CRYPTO_DETECTOR_SERVER_NAME} --rm -d \
        --net=container:${TOOL_SERVER_NAME} ${CRYPTO_DETECTOR_DOCKER_IMAGE}
    fi

    STEP_TIMINGS+=($(date +%s))

    # If we have a service account JSON file, create an .env file to set GOOGLE_APPLICATION_CREDENTIALS
    # in the docker container. Otherwise, we should _not_ set this environment variable so it uses the
    # credentials on the metadata server.
    if [ ! -z ${SERVICE_ACCOUNT_CREDENTIALS} ] ; then
      if [ ! -z ${JUPYTER_DOCKER_IMAGE} ] ; then
        log 'Copying SA into Jupyter Docker...'
        docker cp /etc/${SERVICE_ACCOUNT_CREDENTIALS} ${JUPYTER_SERVER_NAME}:/etc/${SERVICE_ACCOUNT_CREDENTIALS}
      fi
      if [ ! -z ${RSTUDIO_DOCKER_IMAGE} ] ; then
        log 'Copying SA into RStudio Docker...'
        docker cp /etc/${SERVICE_ACCOUNT_CREDENTIALS} ${RSTUDIO_SERVER_NAME}:/etc/${SERVICE_ACCOUNT_CREDENTIALS}
      fi
      if [ ! -z ${WELDER_DOCKER_IMAGE} ] && [ "${WELDER_ENABLED}" == "true" ] ; then
        log 'Copying SA into Welder Docker...'
        docker cp /etc/${SERVICE_ACCOUNT_CREDENTIALS} ${WELDER_SERVER_NAME}:/etc/${SERVICE_ACCOUNT_CREDENTIALS}
      fi
    fi

    STEP_TIMINGS+=($(date +%s))

    # Jupyter-specific setup, only do if Jupyter is installed
    if [ ! -z ${JUPYTER_DOCKER_IMAGE} ] ; then
      log 'Installing Jupydocker kernelspecs...'

      # Install hail addition if the image is old leonardo jupyter image or it's a hail specific image
      if [[ ${JUPYTER_DOCKER_IMAGE} == *"leonardo-jupyter"* ]] ; then
        log 'Installing Hail additions to Jupydocker spark.conf...'

        # Install the Hail additions to Spark conf.
        retry 3 docker exec -u root ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/hail/spark_install_hail.sh
      fi

      # Install notebook.json
      if [ ! -z ${JUPYTER_NOTEBOOK_FRONTEND_CONFIG_URI} ] ; then
        log 'Copy Jupyter frontend notebook config...'
        gsutil cp ${JUPYTER_NOTEBOOK_FRONTEND_CONFIG_URI} /etc
        JUPYTER_NOTEBOOK_FRONTEND_CONFIG=`basename ${JUPYTER_NOTEBOOK_FRONTEND_CONFIG_URI}`
        retry 3 docker exec -u root ${JUPYTER_SERVER_NAME} /bin/bash -c "mkdir -p $JUPYTER_HOME/nbconfig"
        docker cp /etc/${JUPYTER_NOTEBOOK_FRONTEND_CONFIG} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/nbconfig/
      fi

      STEP_TIMINGS+=($(date +%s))

      # Install NbExtensions
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

      STEP_TIMINGS+=($(date +%s))

      # Install serverExtensions
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

      STEP_TIMINGS+=($(date +%s))

      # Install combined extensions
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

      STEP_TIMINGS+=($(date +%s))

      # If a user script was specified, copy it into the docker container and execute it.
      if [ ! -z "$USER_SCRIPT_URI" ] ; then
        apply_user_script $JUPYTER_SERVER_NAME $JUPYTER_HOME
      fi

      # done user script
      STEP_TIMINGS+=($(date +%s))

      # If a start user script was specified, copy it into the docker container for consumption during startups.
      if [ ! -z "$START_USER_SCRIPT_URI" ] ; then
        apply_start_user_script $JUPYTER_SERVER_NAME $JUPYTER_HOME
      fi

      # done start user script
      STEP_TIMINGS+=($(date +%s))

      # Install lab extensions
      # Note: lab extensions need to installed as jupyter user, not root
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

      STEP_TIMINGS+=($(date +%s))

      # See IA-1901: Jupyter UI stalls indefinitely on initial R kernel connection after cluster create/resume
      # The intent of this is to "warm up" R at VM creation time to hopefully prevent issues when the Jupyter
      # kernel tries to connect to it.
      docker exec $JUPYTER_SERVER_NAME /bin/bash -c "R -e '1+1'" || true

      # jupyter_delocalize.py now assumes welder's url is `http://welder:8080`, but on dataproc, we're still using host network
      # A better to do this might be to take welder host as an argument to the script
      docker exec $JUPYTER_SERVER_NAME /bin/bash -c "sed -i 's/http:\/\/welder/http:\/\/127.0.0.1/g' /etc/jupyter/custom/jupyter_delocalize.py"

      # In new jupyter images, we should update jupyter_notebook_config.py in terra-docker.
      # This is to make it so that older images will still work after we change notebooks location to home dir
      docker exec ${JUPYTER_SERVER_NAME} sed -i '/^# to mount there as it effectively deletes existing files on the image/,+5d' ${JUPYTER_HOME}/jupyter_notebook_config.py

      # Copy gitignore into jupyter container

      docker exec $JUPYTER_SERVER_NAME /bin/bash -c "wget https://raw.githubusercontent.com/DataBiosphere/terra-docker/045a139dbac19fbf2b8c4080b8bc7fff7fc8b177/terra-jupyter-aou/gitignore_global"

      # Install nbstripout and set gitignore in Git Config

      docker exec $JUPYTER_SERVER_NAME /bin/bash -c "pip install nbstripout \
            && python -m nbstripout --install --global \
            && git config --global core.excludesfile $JUPYTER_USER_HOME/gitignore_global"


      docker exec -u 0 $JUPYTER_SERVER_NAME /bin/bash -c "$JUPYTER_HOME/scripts/extension/install_jupyter_contrib_nbextensions.sh \
           && mkdir -p $JUPYTER_USER_HOME/.jupyter/custom/ \
           && cp $JUPYTER_HOME/custom/google_sign_in.js $JUPYTER_USER_HOME/.jupyter/custom/ \
           && ls -la $JUPYTER_HOME/custom/extension_entry_jupyter.js \
           && cp $JUPYTER_HOME/custom/extension_entry_jupyter.js $JUPYTER_USER_HOME/.jupyter/custom/custom.js \
           && cp $JUPYTER_HOME/custom/safe-mode.js $JUPYTER_USER_HOME/.jupyter/custom/ \
           && cp $JUPYTER_HOME/custom/edit-mode.js $JUPYTER_USER_HOME/.jupyter/custom/ \
           && mkdir -p $JUPYTER_HOME/nbconfig"

      log 'Starting Jupyter Notebook...'
      retry 3 docker exec -d ${JUPYTER_SERVER_NAME} /bin/bash -c "${JUPYTER_SCRIPTS}/run-jupyter.sh ${NOTEBOOKS_DIR}"

      STEP_TIMINGS+=($(date +%s))
    fi

    # RStudio specific setup; only do if RStudio is installed
    if [ ! -z "$RSTUDIO_DOCKER_IMAGE" ] ; then
      EXIT_CODE=0
      retry 3 docker exec ${RSTUDIO_SERVER_NAME} ${RSTUDIO_SCRIPTS}/set_up_package_dir.sh || EXIT_CODE=$?
      if [ $EXIT_CODE -ne 0 ]; then
        echo "RStudio user package installation directory creation failed, creating /packages directory"
        docker exec ${RSTUDIO_SERVER_NAME} /bin/bash -c "mkdir -p ${RSTUDIO_USER_HOME}/packages && chmod a+rwx ${RSTUDIO_USER_HOME}/packages"
      fi

      # Add the EVs specified in rstudio-docker-compose.yaml to Renviron.site
      retry 3 docker exec ${RSTUDIO_SERVER_NAME} /bin/bash -c 'echo "GOOGLE_PROJECT=$GOOGLE_PROJECT
CLUSTER_NAME=$CLUSTER_NAME
RUNTIME_NAME=$RUNTIME_NAME
OWNER_EMAIL=$OWNER_EMAIL
SHOULD_BACKGROUND_SYNC=$SHOULD_BACKGROUND_SYNC" >> /usr/local/lib/R/etc/Renviron.site'

      # Add custom_env_vars.env to Renviron.site
      CUSTOM_ENV_VARS_FILE=/var/custom_env_vars.env
      if [ -f "$CUSTOM_ENV_VARS_FILE" ]; then
        retry 3 docker cp ${CUSTOM_ENV_VARS_FILE} ${RSTUDIO_SERVER_NAME}:/usr/local/lib/R/var/custom_env_vars.env
        retry 3 docker exec ${RSTUDIO_SERVER_NAME} /bin/bash -c 'cat /usr/local/lib/R/var/custom_env_vars.env >> /usr/local/lib/R/etc/Renviron.site'
      fi

      # If a user script was specified, copy it into the docker container and execute it.
      if [ ! -z "$USER_SCRIPT_URI" ] ; then
        apply_user_script $RSTUDIO_SERVER_NAME $RSTUDIO_SCRIPTS
      fi

      # If a start user script was specified, copy it into the docker container for consumption during startups.
      if [ ! -z "$START_USER_SCRIPT_URI" ] ; then
        apply_start_user_script $RSTUDIO_SERVER_NAME $RSTUDIO_SCRIPTS
      fi

      # Start RStudio server
      retry 3 docker exec -d ${RSTUDIO_SERVER_NAME} /init
    fi

    # Remove any unneeded cached images to save disk space.
    # Do this asynchronously so it doesn't hold up cluster creation
    log 'Pruning docker images...'
    docker image prune -a -f &
fi

log 'All done!'
log "Timings: ${STEP_TIMINGS[@]}"
