#!/usr/bin/env bash

# This init script instantiates the tool (e.g. Jupyter) docker images on Google Compute Engine instances created by Leo.

set -e -x

#####################################################################################################
# Functions
#####################################################################################################

# Retry a command up to a specific number of times until it exits successfully,
# with exponential back off. For example:
#
#   $ retry 5 echo "Hello"
#     Hello
#
#   $ retry 5 false
#     Retry 1/5 exited 1, retrying in 2 seconds...
#     Retry 2/5 exited 1, retrying in 4 seconds...
#     Retry 3/5 exited 1, retrying in 8 seconds...
#     Retry 4/5 exited 1, retrying in 16 seconds...
#     Retry 5/5 exited 1, no more retries left.
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

display_time() {
  local T=$1
  local D=$((T/60/60/24))
  local H=$((T/60/60%24))
  local M=$((T/60%60))
  local S=$((T%60))
  (( $D > 0 )) && printf '%d days ' $D
  (( $H > 0 )) && printf '%d hours ' $H
  (( $M > 0 )) && printf '%d minutes ' $M
  (( $D > 0 || $H > 0 || $M > 0 )) && printf 'and '
  printf '%d seconds\n' $S
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
  docker exec --privileged -u root -e PIP_TARGET=${ROOT_USER_PIP_DIR} ${CONTAINER_NAME} ${TARGET_DIR}/${USER_SCRIPT} &> us_output.txt || EXIT_CODE=$?

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
  docker exec --privileged -u root -e PIP_TARGET=${ROOT_USER_PIP_DIR} ${CONTAINER_NAME} ${TARGET_DIR}/${START_USER_SCRIPT} &> start_output.txt || EXIT_CODE=$?
  if [ $EXIT_CODE -ne 0 ]; then
    echo "User start script failed with exit code ${EXIT_CODE}. Output is saved to ${START_USER_SCRIPT_OUTPUT_URI}"
    retry 3 gsutil -h "x-goog-meta-passed":"false" cp start_output.txt ${START_USER_SCRIPT_OUTPUT_URI}
    exit $EXIT_CODE
  else
    retry 3 gsutil -h "x-goog-meta-passed":"true" cp start_output.txt ${START_USER_SCRIPT_OUTPUT_URI}
  fi
}

#####################################################################################################
# Main starts here.
#####################################################################################################

log "Running GCE VM init script..."

# Array for instrumentation
# UPDATE THIS IF YOU ADD MORE STEPS:
# currently the steps are:
# START init,
# .. after persistent disk setup
# .. after copying files from the GCS init bucket
# .. after starting google-fluentd
# .. after docker compose
# .. after welder start
# .. after extension install
# .. after user script
# .. after start user script
# .. after start Jupyter
# END
START_TIME=$(date +%s)
STEP_TIMINGS=($(date +%s))

# Set variables
# Values like $(..) are populated by Leo when a cluster is created.
# Avoid exporting variables unless they are needed by external scripts or docker-compose files.
export CLUSTER_NAME=$(clusterName)
export RUNTIME_NAME=$(clusterName)
export GOOGLE_PROJECT=$(googleProject)
export STAGING_BUCKET=$(stagingBucketName)
export OWNER_EMAIL=$(loginHint)
export JUPYTER_SERVER_NAME=$(jupyterServerName)
export JUPYTER_DOCKER_IMAGE=$(jupyterDockerImage)
export NOTEBOOKS_DIR=$(notebooksDir)
export WELDER_SERVER_NAME=$(welderServerName)
export WELDER_DOCKER_IMAGE=$(welderDockerImage)
export RSTUDIO_SERVER_NAME=$(rstudioServerName)
export RSTUDIO_DOCKER_IMAGE=$(rstudioDockerImage)
export PROXY_SERVER_NAME=$(proxyServerName)
export PROXY_DOCKER_IMAGE=$(proxyDockerImage)
export CRYPTO_DETECTOR_SERVER_NAME=$(cryptoDetectorServerName)
export CRYPTO_DETECTOR_DOCKER_IMAGE=$(cryptoDetectorDockerImage)
export MEM_LIMIT=$(memLimit)
export WELDER_MEM_LIMIT=$(welderMemLimit)
export PROXY_SERVER_HOST_NAME=$(proxyServerHostName)
export WELDER_ENABLED=$(welderEnabled)

START_USER_SCRIPT_URI=$(startUserScriptUri)
# Include a timestamp suffix to differentiate different startup logs across restarts.
START_USER_SCRIPT_OUTPUT_URI=$(startUserScriptOutputUri)
IS_GCE_FORMATTED=$(isGceFormatted)
JUPYTER_HOME=/etc/jupyter
JUPYTER_SCRIPTS=$JUPYTER_HOME/scripts
JUPYTER_USER_HOME=$(jupyterHomeDirectory)
RSTUDIO_SCRIPTS=/etc/rstudio/scripts
RSTUDIO_USER_HOME=/home/rstudio
SERVER_CRT=$(proxyServerCrt)
SERVER_KEY=$(proxyServerKey)
ROOT_CA=$(rootCaPem)
JUPYTER_DOCKER_COMPOSE_GCE=$(jupyterDockerComposeGce)
RSTUDIO_DOCKER_COMPOSE=$(rstudioDockerCompose)
PROXY_DOCKER_COMPOSE=$(proxyDockerCompose)
WELDER_DOCKER_COMPOSE=$(welderDockerCompose)
CRYPTO_DETECTOR_DOCKER_COMPOSE=$(cryptoDetectorDockerCompose)
PROXY_SITE_CONF=$(proxySiteConf)
JUPYTER_SERVER_EXTENSIONS=$(jupyterServerExtensions)
JUPYTER_NB_EXTENSIONS=$(jupyterNbExtensions)
JUPYTER_COMBINED_EXTENSIONS=$(jupyterCombinedExtensions)
JUPYTER_LAB_EXTENSIONS=$(jupyterLabExtensions)
USER_SCRIPT_URI=$(userScriptUri)
USER_SCRIPT_OUTPUT_URI=$(userScriptOutputUri)
JUPYTER_NOTEBOOK_FRONTEND_CONFIG_URI=$(jupyterNotebookFrontendConfigUri)
CUSTOM_ENV_VARS_CONFIG_URI=$(customEnvVarsConfigUri)
RSTUDIO_LICENSE_FILE=$(rstudioLicenseFile)

mkdir -p /work
mkdir -p /certs

log 'Formatting and mounting persistent disk...'

# Format and mount persistent disk
export DISK_DEVICE_ID=$(lsblk -o name,serial | grep 'user-disk' | awk '{print $1}')
# Only format disk is it hasn't already been formatted
if [ "$IS_GCE_FORMATTED" == "false" ] ; then
  mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/${DISK_DEVICE_ID}
fi
mount -o discard,defaults /dev/${DISK_DEVICE_ID} /work
# Ensure persistent disk re-mounts if runtime stops and restarts
cp /etc/fstab /etc/fstab.backup
echo UUID=`blkid -s UUID -o value /dev/${DISK_DEVICE_ID}` /work ext4 discard,defaults,nofail 0 2 | tee -a /etc/fstab
chmod a+rwx /work

# done persistent disk setup
STEP_TIMINGS+=($(date +%s))

log 'Copying secrets from GCS...'

# Add the certificates from the bucket to the VM. They are used by the docker-compose file
gsutil cp ${SERVER_CRT} /certs
gsutil cp ${SERVER_KEY} /certs
gsutil cp ${ROOT_CA} /certs
gsutil cp ${PROXY_SITE_CONF} /etc
gsutil cp ${JUPYTER_DOCKER_COMPOSE_GCE} /etc
gsutil cp ${RSTUDIO_DOCKER_COMPOSE} /etc
gsutil cp ${PROXY_DOCKER_COMPOSE} /etc
gsutil cp ${WELDER_DOCKER_COMPOSE} /etc
gsutil cp ${CRYPTO_DETECTOR_DOCKER_COMPOSE} /etc

echo "" > /etc/google_application_credentials.env

# Install env var config
if [ ! -z "$CUSTOM_ENV_VARS_CONFIG_URI" ] ; then
  log 'Copy custom env vars config...'
  gsutil cp ${CUSTOM_ENV_VARS_CONFIG_URI} /etc
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

# done GCS copy
STEP_TIMINGS+=($(date +%s))

log 'Setting up google-fluentd...'

# Not all images have the directory used for Stackdriver configs. If so, create it
mkdir -p /etc/google-fluentd/config.d

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

# restarting instead of `service google-fluentd-reload` because of bug:
# https://github.com/GoogleCloudPlatform/google-fluentd/issues/232
service google-fluentd restart

# done google-fluentd setup
STEP_TIMINGS+=($(date +%s))

# If any image is hosted in a GCR registry (detected by regex) then
# authorize docker to interact with gcr.io.
if grep -qF "gcr.io" <<< "${JUPYTER_DOCKER_IMAGE}${RSTUDIO_DOCKER_IMAGE}${PROXY_DOCKER_IMAGE}${WELDER_DOCKER_IMAGE}" ; then
  log 'Authorizing GCR...'
  gcloud --quiet auth configure-docker
fi

log 'Starting up the Jupyter...'

# Run docker-compose for each specified compose file.
# Note the `docker-compose pull` is retried to avoid intermittent network errors, but
# `docker-compose up` is not retried since if that fails, something is probably broken
# and wouldn't be remedied by retrying
COMPOSE_FILES=(-f /etc/`basename ${PROXY_DOCKER_COMPOSE}`)
cat /etc/`basename ${PROXY_DOCKER_COMPOSE}`
if [ ! -z "$JUPYTER_DOCKER_IMAGE" ] ; then
  COMPOSE_FILES+=(-f /etc/`basename ${JUPYTER_DOCKER_COMPOSE_GCE}`)
  cat /etc/`basename ${JUPYTER_DOCKER_COMPOSE_GCE}`
fi
if [ ! -z "$RSTUDIO_DOCKER_IMAGE" ] ; then
  COMPOSE_FILES+=(-f /etc/`basename ${RSTUDIO_DOCKER_COMPOSE}`)
  cat /etc/`basename ${RSTUDIO_DOCKER_COMPOSE}`
fi
if [ ! -z "$WELDER_DOCKER_IMAGE" ] && [ "$WELDER_ENABLED" == "true" ] ; then
  COMPOSE_FILES+=(-f /etc/`basename ${WELDER_DOCKER_COMPOSE}`)
  cat /etc/`basename ${WELDER_DOCKER_COMPOSE}`
fi
# Note: crypto-detector should be started after user containers
if [ ! -z "$CRYPTO_DETECTOR_DOCKER_IMAGE" ] ; then
  COMPOSE_FILES+=(-f /etc/`basename ${CRYPTO_DETECTOR_DOCKER_COMPOSE}`)
  cat /etc/`basename ${CRYPTO_DETECTOR_DOCKER_COMPOSE}`
fi

docker-compose "${COMPOSE_FILES[@]}" config
retry 5 docker-compose "${COMPOSE_FILES[@]}" pull
docker-compose "${COMPOSE_FILES[@]}" up -d

# done docker-compose
STEP_TIMINGS+=($(date +%s))

# If Welder is installed, start the service.
# See https://broadworkbench.atlassian.net/browse/IA-1026
if [ ! -z "$WELDER_DOCKER_IMAGE" ] && [ "$WELDER_ENABLED" == "true" ] ; then
  log 'Starting Welder (file synchronization service)...'
  retry 3 docker exec -d ${WELDER_SERVER_NAME} /opt/docker/bin/entrypoint.sh
fi

# done welder start
STEP_TIMINGS+=($(date +%s))

# Jupyter-specific setup, only do if Jupyter is installed
if [ ! -z "$JUPYTER_DOCKER_IMAGE" ] ; then
  # user package installation directory
  mkdir -p /work/packages
  chmod a+rwx /work/packages

  # Used to pip install packacges
  JUPYTER_USER_PIP_DIR=$JUPYTER_USER_HOME/.local/lib/python3.7/site-packages

  # TODO: update this if we upgrade python version
  if [ "$JUPYTER_USER_HOME" = "/home/jupyter" ]
  then
     ROOT_USER_PIP_DIR=/opt/conda/lib/python3.7/site-packages
  else
     ROOT_USER_PIP_DIR=/usr/local/lib/python3.7/dist-packages


    # TODO: Remove once we stop supporting non AI notebooks based images
    log 'Installing Jupyter kernelspecs...(Remove once we stop supporting non AI notebooks based images)'
    KERNELSPEC_HOME=/usr/local/share/jupyter/kernels

    # Install kernelspecs inside the Jupyter container
    retry 3 docker exec -u root ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/kernel/kernelspec.sh ${JUPYTER_SCRIPTS}/kernel ${KERNELSPEC_HOME}
  fi


  # Install notebook.json
  if [ ! -z "$JUPYTER_NOTEBOOK_FRONTEND_CONFIG_URI" ] ; then
    log 'Copy Jupyter frontend notebook config...'
    gsutil cp ${JUPYTER_NOTEBOOK_FRONTEND_CONFIG_URI} /etc
    JUPYTER_NOTEBOOK_FRONTEND_CONFIG=`basename ${JUPYTER_NOTEBOOK_FRONTEND_CONFIG_URI}`
    docker cp /etc/${JUPYTER_NOTEBOOK_FRONTEND_CONFIG} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/nbconfig/
  fi

  # Install NbExtensions
  if [ ! -z "$JUPYTER_NB_EXTENSIONS" ] ; then
    for ext in ${JUPYTER_NB_EXTENSIONS}
    do
      log "Installing Jupyter NB extension [$ext]..."
      if [[ $ext == 'gs://'* ]]; then
        gsutil cp $ext /etc
        JUPYTER_EXTENSION_ARCHIVE=`basename $ext`
        docker cp /etc/${JUPYTER_EXTENSION_ARCHIVE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
        retry 3 docker exec -u root -e PIP_TARGET=${ROOT_USER_PIP_DIR} ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_notebook_extension.sh ${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
      elif [[ $ext == 'http://'* || $ext == 'https://'* ]]; then
        JUPYTER_EXTENSION_FILE=`basename $ext`
        curl $ext -o /etc/${JUPYTER_EXTENSION_FILE}
        docker cp /etc/${JUPYTER_EXTENSION_FILE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_EXTENSION_FILE}
        retry 3 docker exec -u root -e PIP_TARGET=${ROOT_USER_PIP_DIR} ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_notebook_extension.sh ${JUPYTER_HOME}/${JUPYTER_EXTENSION_FILE}
      else
        retry 3 docker exec -u root -e PIP_TARGET=${ROOT_USER_PIP_DIR} ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_pip_install_notebook_extension.sh $ext
      fi
    done
  fi

  # Install serverExtensions
  if [ ! -z "$JUPYTER_SERVER_EXTENSIONS" ] ; then
    for ext in ${JUPYTER_SERVER_EXTENSIONS}
    do
      log "Installing Jupyter server extension [$ext]..."
      if [[ $ext == 'gs://'* ]]; then
        gsutil cp $ext /etc
        JUPYTER_EXTENSION_ARCHIVE=`basename $ext`
        docker cp /etc/${JUPYTER_EXTENSION_ARCHIVE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
        retry 3 docker exec -u root -e PIP_TARGET=${ROOT_USER_PIP_DIR} ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_server_extension.sh ${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
      else
        retry 3 docker exec -u root -e PIP_TARGET=${ROOT_USER_PIP_DIR} ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_pip_install_server_extension.sh $ext
      fi
    done
  fi

  # Install combined extensions
  if [ ! -z "$JUPYTER_COMBINED_EXTENSIONS"  ] ; then
    for ext in ${JUPYTER_COMBINED_EXTENSIONS}
    do
      log "Installing Jupyter combined extension [$ext]..."
      log $ext
      if [[ $ext == 'gs://'* ]]; then
        gsutil cp $ext /etc
        JUPYTER_EXTENSION_ARCHIVE=`basename $ext`
        docker cp /etc/${JUPYTER_EXTENSION_ARCHIVE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
        retry 3 docker exec -u root -e PIP_TARGET=${ROOT_USER_PIP_DIR} ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_combined_extension.sh ${JUPYTER_EXTENSION_ARCHIVE}
      else
        retry 3 docker exec -u root -e PIP_TARGET=${ROOT_USER_PIP_DIR} ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_pip_install_combined_extension.sh $ext
      fi
    done
  fi

  # Install lab extensions
  # Note: lab extensions need to installed as jupyter user, not root
  if [ ! -z "$JUPYTER_LAB_EXTENSIONS" ] ; then
    for ext in ${JUPYTER_LAB_EXTENSIONS}
    do
      log "Installing JupyterLab extension [$ext]..."
      pwd
      if [[ $ext == 'gs://'* ]]; then
        gsutil cp -r $ext /etc
        JUPYTER_EXTENSION_ARCHIVE=`basename $ext`
        docker cp /etc/${JUPYTER_EXTENSION_ARCHIVE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
        retry 3 docker exec -e PIP_TARGET=${JUPYTER_USER_PIP_DIR} ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_lab_extension.sh ${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
      elif [[ $ext == 'http://'* || $ext == 'https://'* ]]; then
        JUPYTER_EXTENSION_FILE=`basename $ext`
        curl $ext -o /etc/${JUPYTER_EXTENSION_FILE}
        docker cp /etc/${JUPYTER_EXTENSION_FILE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_EXTENSION_FILE}
        retry 3 docker exec -e PIP_TARGET=${JUPYTER_USER_PIP_DIR} ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_lab_extension.sh ${JUPYTER_HOME}/${JUPYTER_EXTENSION_FILE}
      else
        retry 3 docker exec -e PIP_TARGET=${JUPYTER_USER_PIP_DIR} ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_lab_extension.sh $ext
      fi
    done
  fi

  # done extension setup
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

  # See IA-1901: Jupyter UI stalls indefinitely on initial R kernel connection after cluster create/resume
  # The intent of this is to "warm up" R at VM creation time to hopefully prevent issues when the Jupyter
  # kernel tries to connect to it.
  docker exec $JUPYTER_SERVER_NAME /bin/bash -c "R -e '1+1'" || true

  log 'Starting Jupyter Notebook...'
  retry 3 docker exec -d ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/run-jupyter.sh ${NOTEBOOKS_DIR}

  # done start Jupyter
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
WORKSPACE_NAMESPACE=$WORKSPACE_NAMESPACE
CLUSTER_NAME=$CLUSTER_NAME
RUNTIME_NAME=$RUNTIME_NAME
OWNER_EMAIL=$OWNER_EMAIL" >> /usr/local/lib/R/etc/Renviron.site'

  # Add custom_env_vars.env to Renviron.site
  CUSTOM_ENV_VARS_FILE=/etc/custom_env_vars.env
  if [ -f "$CUSTOM_ENV_VARS_FILE" ]; then
    retry 3 docker cp /etc/custom_env_vars.env ${RSTUDIO_SERVER_NAME}:/usr/local/lib/R/etc/custom_env_vars.env
    retry 3 docker exec ${RSTUDIO_SERVER_NAME} /bin/bash -c 'cat /usr/local/lib/R/etc/custom_env_vars.env >> /usr/local/lib/R/etc/Renviron.site'
  fi

  # If a user script was specified, copy it into the docker container and execute it.
  if [ ! -z "$USER_SCRIPT_URI" ] ; then
    apply_user_script $RSTUDIO_SERVER_NAME $JUPYTER_HOME
  fi

  # If a start user script was specified, copy it into the docker container for consumption during startups.
  if [ ! -z "$START_USER_SCRIPT_URI" ] ; then
    apply_start_user_script $RSTUDIO_SERVER_NAME $RS#!/usr/bin/env bash
  fi

  # Start RStudio server
  retry 3 docker exec -d ${RSTUDIO_SERVER_NAME} /init
fi

# Remove any unneeded cached images to save disk space.
# Do this asynchronously so it doesn't hold up cluster creation
log 'Pruning docker images...'
docker image prune -a -f &

log 'All done!'

ELAPSED_TIME=$(($END_TIME - $START_TIME))
log "gce-init.sh took $(display_time $ELAPSED_TIME)"
log "Step timings: ${STEP_TIMINGS[@]}"