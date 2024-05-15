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
export PET_SA_EMAIL=$(petSaEmail)
export JUPYTER_SERVER_NAME=$(jupyterServerName)
export JUPYTER_DOCKER_IMAGE=$(jupyterDockerImage)
export WELDER_SERVER_NAME=$(welderServerName)
export WELDER_DOCKER_IMAGE=$(welderDockerImage)
export RSTUDIO_SERVER_NAME=$(rstudioServerName)
export RSTUDIO_DOCKER_IMAGE=$(rstudioDockerImage)
export RSTUDIO_USER_HOME=/home/rstudio
export PROXY_SERVER_NAME=$(proxyServerName)
export PROXY_DOCKER_IMAGE=$(proxyDockerImage)
export CRYPTO_DETECTOR_SERVER_NAME=$(cryptoDetectorServerName)
export CRYPTO_DETECTOR_DOCKER_IMAGE=$(cryptoDetectorDockerImage)
export MEM_LIMIT=$(memLimit)
export WELDER_MEM_LIMIT=$(welderMemLimit)
export PROXY_SERVER_HOST_NAME=$(proxyServerHostName)
export WELDER_ENABLED=$(welderEnabled)
export NOTEBOOKS_DIR=$(notebooksDir)

START_USER_SCRIPT_URI=$(startUserScriptUri)
# Include a timestamp suffix to differentiate different startup logs across restarts.
START_USER_SCRIPT_OUTPUT_URI=$(startUserScriptOutputUri)
IS_GCE_FORMATTED=$(isGceFormatted)
JUPYTER_HOME=/etc/jupyter
JUPYTER_SCRIPTS=$JUPYTER_HOME/scripts
JUPYTER_USER_HOME=$(jupyterHomeDirectory)
RSTUDIO_SCRIPTS=/etc/rstudio/scripts
SERVER_CRT=$(proxyServerCrt)
SERVER_KEY=$(proxyServerKey)
ROOT_CA=$(rootCaPem)
JUPYTER_DOCKER_COMPOSE_GCE=$(jupyterDockerCompose)
RSTUDIO_DOCKER_COMPOSE=$(rstudioDockerCompose)
PROXY_DOCKER_COMPOSE=$(proxyDockerCompose)
WELDER_DOCKER_COMPOSE=$(welderDockerCompose)
GPU_DOCKER_COMPOSE=$(gpuDockerCompose)
PROXY_SITE_CONF=$(proxySiteConf)
JUPYTER_SERVER_EXTENSIONS=$(jupyterServerExtensions)
JUPYTER_NB_EXTENSIONS=$(jupyterNbExtensions)
JUPYTER_COMBINED_EXTENSIONS=$(jupyterCombinedExtensions)
JUPYTER_LAB_EXTENSIONS=$(jupyterLabExtensions)
USER_SCRIPT_URI=$(userScriptUri)
USER_SCRIPT_OUTPUT_URI=$(userScriptOutputUri)
JUPYTER_NOTEBOOK_FRONTEND_CONFIG_URI=$(jupyterNotebookFrontendConfigUri)
CUSTOM_ENV_VARS_CONFIG_URI=$(customEnvVarsConfigUri)
GPU_ENABLED=$(gpuEnabled)
INIT_BUCKET_NAME=$(initBucketName)

CERT_DIRECTORY='/var/certs'
DOCKER_COMPOSE_FILES_DIRECTORY='/var/docker-compose-files'
WORK_DIRECTORY='/mnt/disks/work'
GSUTIL_CMD='docker run --rm -v /var:/var us.gcr.io/cos-cloud/toolbox:v20230714 gsutil'
GCLOUD_CMD='docker run --rm -v /var:/var us.gcr.io/cos-cloud/toolbox:v20230714 gcloud'

if [ ! -z "$RSTUDIO_DOCKER_IMAGE" ] ; then
  export SHOULD_BACKGROUND_SYNC="true"
else
  export SHOULD_BACKGROUND_SYNC="false"
fi

if grep -qF "gcr.io" <<< "${JUPYTER_DOCKER_IMAGE}${RSTUDIO_DOCKER_IMAGE}${PROXY_DOCKER_IMAGE}${WELDER_DOCKER_IMAGE}" ; then
  log 'Authorizing GCR...'
  DOCKER_COMPOSE="docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v /var:/var -w=/var cryptopants/docker-compose-gcr"
else
  DOCKER_COMPOSE="docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v /var:/var docker/compose:1.29.2"
fi

function apply_user_script() {
  local CONTAINER_NAME=$1
  local TARGET_DIR=$2

  log "Running user script $USER_SCRIPT_URI in $CONTAINER_NAME container..."
  USER_SCRIPT=`basename ${USER_SCRIPT_URI}`
  if [[ "$USER_SCRIPT_URI" == 'gs://'* ]]; then
    $GSUTIL_CMD cp ${USER_SCRIPT_URI} /var &> /var/user_script_copy_output.txt
  else
    curl "${USER_SCRIPT_URI}" -o /var/"${USER_SCRIPT}"
  fi
  docker cp /var/"${USER_SCRIPT}" ${CONTAINER_NAME}:${TARGET_DIR}/"${USER_SCRIPT}"
  retry 3 docker exec -u root ${CONTAINER_NAME} chmod +x ${TARGET_DIR}/"${USER_SCRIPT}"

  # Execute the user script as privileged to allow for deeper customization of VM behavior, e.g. installing
  # network egress throttling. As docker is not a security layer, it is assumed that a determined attacker
  # can gain full access to the VM already, so using this flag is not a significant escalation.
  EXIT_CODE=0
  docker exec --privileged -u root -e PIP_USER=false ${CONTAINER_NAME} ${TARGET_DIR}/"${USER_SCRIPT}" &> /var/us_output.txt || EXIT_CODE=$?

  if [ $EXIT_CODE -ne 0 ]; then
    log "User script failed with exit code $EXIT_CODE. Output is saved to $USER_SCRIPT_OUTPUT_URI."
    retry 3 $GSUTIL_CMD -h "x-goog-meta-passed":"false" cp /var/us_output.txt ${USER_SCRIPT_OUTPUT_URI}
    exit $EXIT_CODE
  else
    retry 3 $GSUTIL_CMD -h "x-goog-meta-passed":"true" cp /var/us_output.txt ${USER_SCRIPT_OUTPUT_URI}
  fi
}

function apply_start_user_script() {
  local CONTAINER_NAME=$1
  local TARGET_DIR=$2

  log "Running start user script $START_USER_SCRIPT_URI in $CONTAINER_NAME container..."
  START_USER_SCRIPT=`basename ${START_USER_SCRIPT_URI}`
  if [[ "$START_USER_SCRIPT_URI" == 'gs://'* ]]; then
    $GSUTIL_CMD cp ${START_USER_SCRIPT_URI} /var
  else
    curl $START_USER_SCRIPT_URI -o /var/${START_USER_SCRIPT}
  fi
  docker cp /var/${START_USER_SCRIPT} ${CONTAINER_NAME}:${TARGET_DIR}/${START_USER_SCRIPT}
  retry 3 docker exec -u root ${CONTAINER_NAME} chmod +x ${TARGET_DIR}/${START_USER_SCRIPT}

  # Keep in sync with startup.sh
  EXIT_CODE=0
  docker exec --privileged -u root -e PIP_USER=false ${CONTAINER_NAME} ${TARGET_DIR}/${START_USER_SCRIPT} &> /var/start_output.txt || EXIT_CODE=$?
  if [ $EXIT_CODE -ne 0 ]; then
    echo "User start script failed with exit code ${EXIT_CODE}. Output is saved to ${START_USER_SCRIPT_OUTPUT_URI}"
    retry 3 $GSUTIL_CMD -h "x-goog-meta-passed":"false" cp /var/start_output.txt ${START_USER_SCRIPT_OUTPUT_URI}
    exit $EXIT_CODE
  else
    retry 3 $GSUTIL_CMD -h "x-goog-meta-passed":"true" cp /var/start_output.txt ${START_USER_SCRIPT_OUTPUT_URI}
  fi
}

mkdir -p ${WORK_DIRECTORY}
mkdir -p ${CERT_DIRECTORY}
mkdir -p ${DOCKER_COMPOSE_FILES_DIRECTORY}

log 'Formatting and mounting persistent disk...'

# Format and mount persistent disk
# Fix this to `sdb`. We've never seen a device name that's not `sdb`,
# Altho you some images, this cmd $(lsblk -o name,serial | grep 'user-disk' | awk '{print $1}')
# can be used to find device name, this doesn't work for COS images
USER_DISK_DEVICE_ID=$(lsblk -o name,serial | grep 'user-disk' | awk '{print $1}')
DISK_DEVICE_ID=${USER_DISK_DEVICE_ID:-sdb}

## Only format disk is it hasn't already been formatted
if [ "$IS_GCE_FORMATTED" == "false" ] ; then
  # It's likely that the persistent disk was previously mounted on another VM and wasn't properly unmounted
  # either because the VM was terminated or there is no unmount in the shutdown sequence and occasionally
  # fs is getting marked as not clean.
  # Passing -F -F to mkfs.ext4 should force the tool to ignore the state of the partition.
  # Note that there should be two instances command-line switch (-F -F) to override this check

  mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/${DISK_DEVICE_ID} -F -F
fi

mount -t ext4 -O discard,defaults /dev/${DISK_DEVICE_ID} ${WORK_DIRECTORY}

# done persistent disk setup
STEP_TIMINGS+=($(date +%s))

if [ "${GPU_ENABLED}" == "true" ] ; then
  log 'Installing GPU driver...'
  version="535.154.05"
  isAvailable=$(cos-extensions list|grep $version)
  if [[ -z "$isAvailable" ]]; then
      # Install default version on the COS image
      cos-extensions install gpu
  else
      cos-extensions install gpu -- --version $version
  fi
  mount --bind /var/lib/nvidia /var/lib/nvidia
  mount -o remount,exec /var/lib/nvidia

  $GSUTIL_CMD cp ${GPU_DOCKER_COMPOSE} ${DOCKER_COMPOSE_FILES_DIRECTORY}
fi

log 'Copying secrets from GCS...'

# Add the certificates from the bucket to the VM. They are used by the docker-compose file
$GSUTIL_CMD cp ${SERVER_CRT} ${CERT_DIRECTORY}
$GSUTIL_CMD cp ${SERVER_KEY} ${CERT_DIRECTORY}
$GSUTIL_CMD cp ${ROOT_CA} ${CERT_DIRECTORY}
$GSUTIL_CMD cp gs://${INIT_BUCKET_NAME}/* ${DOCKER_COMPOSE_FILES_DIRECTORY}

echo "" > /var/google_application_credentials.env

# Install env var config
if [ ! -z "$CUSTOM_ENV_VARS_CONFIG_URI" ] ; then
  log 'Copy custom env vars config...'
  $GSUTIL_CMD cp ${CUSTOM_ENV_VARS_CONFIG_URI} /var
fi

# done GCS copy
STEP_TIMINGS+=($(date +%s))

log 'Starting up the Jupyter...'

# Run docker-compose for each specified compose file.
# Note the `docker-compose pull` is retried to avoid intermittent network errors, but
# `docker-compose up` is not retried since if that fails, something is probably broken
# and wouldn't be remedied by retrying
COMPOSE_FILES=(-f ${DOCKER_COMPOSE_FILES_DIRECTORY}/`basename ${PROXY_DOCKER_COMPOSE}`)
cat ${DOCKER_COMPOSE_FILES_DIRECTORY}/`basename ${PROXY_DOCKER_COMPOSE}`
if [ ! -z "$WELDER_DOCKER_IMAGE" ] && [ "$WELDER_ENABLED" == "true" ] ; then
  COMPOSE_FILES+=(-f ${DOCKER_COMPOSE_FILES_DIRECTORY}/`basename ${WELDER_DOCKER_COMPOSE}`)
  cat ${DOCKER_COMPOSE_FILES_DIRECTORY}/`basename ${WELDER_DOCKER_COMPOSE}`
fi

if [ "${GPU_ENABLED}" == "true" ] ; then
  COMPOSE_FILES+=(-f ${DOCKER_COMPOSE_FILES_DIRECTORY}/`basename ${GPU_DOCKER_COMPOSE}`)
  if [ ! -z "$RSTUDIO_DOCKER_IMAGE" ] ; then
    sed -i 's/jupyter/rstudio/g' ${DOCKER_COMPOSE_FILES_DIRECTORY}/`basename ${GPU_DOCKER_COMPOSE}`
    sed -i 's#${NOTEBOOKS_DIR}#/home/rstudio#g' ${DOCKER_COMPOSE_FILES_DIRECTORY}/`basename ${GPU_DOCKER_COMPOSE}`
  fi
  cat ${DOCKER_COMPOSE_FILES_DIRECTORY}/`basename ${GPU_DOCKER_COMPOSE}`
fi

if [ ! -z "$JUPYTER_DOCKER_IMAGE" ] ; then
  TOOL_SERVER_NAME=${JUPYTER_SERVER_NAME}
  COMPOSE_FILES+=(-f ${DOCKER_COMPOSE_FILES_DIRECTORY}/`basename ${JUPYTER_DOCKER_COMPOSE_GCE}`)
  cat ${DOCKER_COMPOSE_FILES_DIRECTORY}/`basename ${JUPYTER_DOCKER_COMPOSE_GCE}`
fi

if [ ! -z "$RSTUDIO_DOCKER_IMAGE" ] ; then
  TOOL_SERVER_NAME=${RSTUDIO_SERVER_NAME}
  COMPOSE_FILES+=(-f ${DOCKER_COMPOSE_FILES_DIRECTORY}/`basename ${RSTUDIO_DOCKER_COMPOSE}`)
  cat ${DOCKER_COMPOSE_FILES_DIRECTORY}/`basename ${RSTUDIO_DOCKER_COMPOSE}`
fi

tee /var/variables.env << END
CERT_DIRECTORY=${CERT_DIRECTORY}
WORK_DIRECTORY=${WORK_DIRECTORY}
PROXY_SERVER_NAME=${PROXY_SERVER_NAME}
PROXY_DOCKER_IMAGE=${PROXY_DOCKER_IMAGE}
GOOGLE_PROJECT=${GOOGLE_PROJECT}
RUNTIME_NAME=${RUNTIME_NAME}
PROXY_SERVER_HOST_NAME=${PROXY_SERVER_HOST_NAME}
JUPYTER_SERVER_NAME=${JUPYTER_SERVER_NAME}
JUPYTER_DOCKER_IMAGE=${JUPYTER_DOCKER_IMAGE}
NOTEBOOKS_DIR=${NOTEBOOKS_DIR}
OWNER_EMAIL=${OWNER_EMAIL}
PET_SA_EMAIL=${PET_SA_EMAIL}
WELDER_ENABLED=${WELDER_ENABLED}
MEM_LIMIT=${MEM_LIMIT}
WELDER_SERVER_NAME=${WELDER_SERVER_NAME}
WELDER_DOCKER_IMAGE=${WELDER_DOCKER_IMAGE}
STAGING_BUCKET=${STAGING_BUCKET}
WELDER_MEM_LIMIT=${WELDER_MEM_LIMIT}
JUPYTER_SCRIPTS=${JUPYTER_SCRIPTS}
HOST_PROXY_SITE_CONF_FILE_PATH=${DOCKER_COMPOSE_FILES_DIRECTORY}/`basename ${PROXY_SITE_CONF}`
DOCKER_COMPOSE_FILES_DIRECTORY=${DOCKER_COMPOSE_FILES_DIRECTORY}
RSTUDIO_SERVER_NAME=${RSTUDIO_SERVER_NAME}
RSTUDIO_DOCKER_IMAGE=${RSTUDIO_DOCKER_IMAGE}
SHOULD_BACKGROUND_SYNC=${SHOULD_BACKGROUND_SYNC}
RSTUDIO_USER_HOME=${RSTUDIO_USER_HOME}
END

# Create a network that allows containers to talk to each other via exposed ports
docker network create -d bridge app_network

${DOCKER_COMPOSE} --env-file=/var/variables.env "${COMPOSE_FILES[@]}" config

retry 5 ${DOCKER_COMPOSE} --env-file=/var/variables.env "${COMPOSE_FILES[@]}" pull &> /var/docker_pull_output.txt

# This needs to happen before we start up containers
chmod a+rwx ${WORK_DIRECTORY}

${DOCKER_COMPOSE} --env-file=/var/variables.env "${COMPOSE_FILES[@]}" up -d

# Start up crypto detector, if enabled.
# This should be started after other containers.
# Use `docker run` instead of docker-compose so we can link it to the Jupyter/RStudio container's network.
# See https://github.com/broadinstitute/terra-cryptomining-security-alerts/tree/master/v2
if [ ! -z "$CRYPTO_DETECTOR_DOCKER_IMAGE" ] ; then
  docker run --name=${CRYPTO_DETECTOR_SERVER_NAME} --rm -d \
    --net=container:${TOOL_SERVER_NAME} ${CRYPTO_DETECTOR_DOCKER_IMAGE}
fi

# done welder start
STEP_TIMINGS+=($(date +%s))

# Jupyter-specific setup, only do if Jupyter is installed
if [ ! -z "$JUPYTER_DOCKER_IMAGE" ] ; then
  # user package installation directory
  mkdir -p ${WORK_DIRECTORY}/packages
  chmod a+rwx ${WORK_DIRECTORY}/packages

  # TODO: update this if we upgrade python version
  if [ ! "$JUPYTER_USER_HOME" = "/home/jupyter" ] ; then
    # TODO: Remove once we stop supporting non AI notebooks based images
    log 'Installing Jupyter kernelspecs...(Remove once we stop supporting non AI notebooks based images)'
    KERNELSPEC_HOME=/usr/local/share/jupyter/kernels

    # Install kernelspecs inside the Jupyter container
    retry 3 docker exec -u root ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/kernel/kernelspec.sh ${JUPYTER_SCRIPTS}/kernel ${KERNELSPEC_HOME}
  fi

  # Install notebook.json
  if [ ! -z "$JUPYTER_NOTEBOOK_FRONTEND_CONFIG_URI" ] ; then
    log 'Copy Jupyter frontend notebook config...'
    $GSUTIL_CMD cp ${JUPYTER_NOTEBOOK_FRONTEND_CONFIG_URI} /var
    JUPYTER_NOTEBOOK_FRONTEND_CONFIG=`basename ${JUPYTER_NOTEBOOK_FRONTEND_CONFIG_URI}`
    retry 3 docker exec -u root ${JUPYTER_SERVER_NAME} /bin/bash -c "mkdir -p $JUPYTER_HOME/nbconfig"
    docker cp /var/${JUPYTER_NOTEBOOK_FRONTEND_CONFIG} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/nbconfig/
  fi

  # Install NbExtensions
  if [ ! -z "$JUPYTER_NB_EXTENSIONS" ] ; then
    for ext in ${JUPYTER_NB_EXTENSIONS}
    do
      log "Installing Jupyter NB extension [$ext]..."
      if [[ $ext == 'gs://'* ]]; then
        $GSUTIL_CMD cp $ext /var
        JUPYTER_EXTENSION_ARCHIVE=`basename $ext`
        docker cp /var/${JUPYTER_EXTENSION_ARCHIVE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
        retry 3 docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_notebook_extension.sh ${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
      elif [[ $ext == 'http://'* || $ext == 'https://'* ]]; then
        JUPYTER_EXTENSION_FILE=`basename $ext`
        curl $ext -o /var/${JUPYTER_EXTENSION_FILE}
        docker cp /var/${JUPYTER_EXTENSION_FILE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_EXTENSION_FILE}
        retry 3 docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_notebook_extension.sh ${JUPYTER_HOME}/${JUPYTER_EXTENSION_FILE}
      else
        retry 3 docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_pip_install_notebook_extension.sh $ext
      fi
    done
  fi

  # Install serverExtensions
  if [ ! -z "$JUPYTER_SERVER_EXTENSIONS" ] ; then
    for ext in ${JUPYTER_SERVER_EXTENSIONS}
    do
      log "Installing Jupyter server extension [$ext]..."
      if [[ $ext == 'gs://'* ]]; then
        $GSUTIL_CMD cp $ext /var
        JUPYTER_EXTENSION_ARCHIVE=`basename $ext`
        docker cp /var/${JUPYTER_EXTENSION_ARCHIVE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
        retry 3 docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_server_extension.sh ${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
      else
        retry 3 docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_pip_install_server_extension.sh $ext
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
        $GSUTIL_CMD cp $ext /var
        JUPYTER_EXTENSION_ARCHIVE=`basename $ext`
        docker cp /var/${JUPYTER_EXTENSION_ARCHIVE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
        retry 3 docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_combined_extension.sh ${JUPYTER_EXTENSION_ARCHIVE}
      else
        retry 3 docker exec -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_pip_install_combined_extension.sh $ext
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
        $GSUTIL_CMD cp -r $ext /var
        JUPYTER_EXTENSION_ARCHIVE=`basename $ext`
        docker cp /var/${JUPYTER_EXTENSION_ARCHIVE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
        retry 3 docker exec -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_lab_extension.sh ${JUPYTER_HOME}/${JUPYTER_EXTENSION_ARCHIVE}
      elif [[ $ext == 'http://'* || $ext == 'https://'* ]]; then
        JUPYTER_EXTENSION_FILE=`basename $ext`
        curl $ext -o /var/${JUPYTER_EXTENSION_FILE}
        docker cp /var/${JUPYTER_EXTENSION_FILE} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${JUPYTER_EXTENSION_FILE}
        retry 3 docker exec -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_lab_extension.sh ${JUPYTER_HOME}/${JUPYTER_EXTENSION_FILE}
      else
        retry 3 docker exec -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_SCRIPTS}/extension/jupyter_install_lab_extension.sh $ext
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

  # For older jupyter images, jupyter_delocalize.py is using 127.0.0.1 as welder's url, which won't work now that we're no longer using `network_mode: host` for GCE VMs
  docker exec $JUPYTER_SERVER_NAME /bin/bash -c "sed -i 's/127.0.0.1/welder/g' /etc/jupyter/custom/jupyter_delocalize.py"

  # Copy gitignore into jupyter container

  docker exec $JUPYTER_SERVER_NAME /bin/bash -c "wget -N https://raw.githubusercontent.com/DataBiosphere/terra-docker/045a139dbac19fbf2b8c4080b8bc7fff7fc8b177/terra-jupyter-aou/gitignore_global"

  # Install nbstripout and set gitignore in Git Config

  docker exec $JUPYTER_SERVER_NAME /bin/bash -c "pip install nbstripout \
        && nbstripout --install --global \
        && git config --global core.excludesfile $JUPYTER_USER_HOME/gitignore_global"

  docker exec -u 0 $JUPYTER_SERVER_NAME /bin/bash -c "$JUPYTER_HOME/scripts/extension/install_jupyter_contrib_nbextensions.sh \
       && mkdir -p $JUPYTER_USER_HOME/.jupyter/custom/ \
       && cp $JUPYTER_HOME/custom/google_sign_in.js $JUPYTER_USER_HOME/.jupyter/custom/ \
       && ls -la $JUPYTER_HOME/custom/extension_entry_jupyter.js \
       && cp $JUPYTER_HOME/custom/extension_entry_jupyter.js $JUPYTER_USER_HOME/.jupyter/custom/custom.js \
       && cp $JUPYTER_HOME/custom/safe-mode.js $JUPYTER_USER_HOME/.jupyter/custom/ \
       && cp $JUPYTER_HOME/custom/edit-mode.js $JUPYTER_USER_HOME/.jupyter/custom/ \
       && mkdir -p $JUPYTER_HOME/nbconfig"

  # In new jupyter images, we should update jupyter_notebook_config.py in terra-docker.
  # This is to make it so that older images will still work after we change notebooks location to home dir
  docker exec ${JUPYTER_SERVER_NAME} sed -i '/^# to mount there as it effectively deletes existing files on the image/,+5d' ${JUPYTER_HOME}/jupyter_notebook_config.py

  log 'Starting Jupyter Notebook...'
  retry 3 docker exec -d $JUPYTER_SERVER_NAME /bin/bash -c "${JUPYTER_SCRIPTS}/run-jupyter.sh ${NOTEBOOKS_DIR}"

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
CLUSTER_NAME=$CLUSTER_NAME
RUNTIME_NAME=$RUNTIME_NAME
OWNER_EMAIL=$OWNER_EMAIL
SHOULD_BACKGROUND_SYNC=$SHOULD_BACKGROUND_SYNC
RSTUDIO_USER_HOME=$RSTUDIO_USER_HOME" >> /usr/local/lib/R/etc/Renviron.site'

  # Add custom_env_vars.env to Renviron.site
  CUSTOM_ENV_VARS_FILE=/var/custom_env_vars.env
  if [ -f "$CUSTOM_ENV_VARS_FILE" ]; then
    retry 3 docker cp /var/custom_env_vars.env ${RSTUDIO_SERVER_NAME}:/usr/local/lib/R/etc/custom_env_vars.env
    retry 3 docker exec ${RSTUDIO_SERVER_NAME} /bin/bash -c 'cat /usr/local/lib/R/etc/custom_env_vars.env >> /usr/local/lib/R/etc/Renviron.site'
  fi

  # For older rstudio images, /etc/rstudio/rserver.conf is using 127.0.0.1 as www-address, which won't work now that we're no longer using `network_mode: host` for GCE VMs
  docker exec ${RSTUDIO_SERVER_NAME} sed -i  "s/127.0.0.1/0.0.0.0/g" /etc/rstudio/rserver.conf

    # If a user script was specified, copy it into the docker container and execute it.
  if [ ! -z "$USER_SCRIPT_URI" ] ; then
    apply_user_script $RSTUDIO_SERVER_NAME $RSTUDIO_SCRIPTS
  fi

  # If a start user script was specified, copy it into the docker container for consumption during startups.
  if [ ! -z "$START_USER_SCRIPT_URI" ] ; then
    apply_start_user_script $RSTUDIO_SERVER_NAME $RSTUDIO_SCRIPTS
  fi

  # default autosave to 10 seconds
  docker exec ${RSTUDIO_SERVER_NAME} /bin/bash -c 'mkdir -p $RSTUDIO_USER_HOME/.config/rstudio \
    && echo "{
\"initial_working_directory\": \"~\",
\"auto_save_on_blur\": true,
\"auto_save_on_idle\": \"commit\",
\"posix_terminal_shell\": \"bash\",
\"auto_save_idle_ms\": 10000
}" > $RSTUDIO_USER_HOME/.config/rstudio/rstudio-prefs-temp.json \
    && mv $RSTUDIO_USER_HOME/.config/rstudio/rstudio-prefs-temp.json $RSTUDIO_USER_HOME/.config/rstudio/rstudio-prefs.json \
    && chown -R rstudio:users $RSTUDIO_USER_HOME/.config'

  # Start RStudio server
  retry 3 docker exec -d ${RSTUDIO_SERVER_NAME} /init
fi

# Resize persistent disk if needed.
# This condition assumes Dataproc's cert directory is different from GCE's cert directory, a better condition would be
# a dedicated flag that distinguishes gce and dataproc. But this will do for now
# If it's GCE, we resize the PD. Dataproc doesn't have PD
if [ -f "/var/certs/jupyter-server.crt" ]; then
  echo "Resizing persistent disk attached to runtime $GOOGLE_PROJECT / $CLUSTER_NAME if disk size changed..."
  resize2fs /dev/${DISK_DEVICE_ID}
fi

# Remove any unneeded cached images to save disk space.
# Do this asynchronously so it doesn't hold up cluster creation
log 'Pruning docker images...'
docker image prune -a -f &

log 'All done!'

ELAPSED_TIME=$(($END_TIME - $START_TIME))
log "gce-init.sh took $(display_time $ELAPSED_TIME)"
log "Step timings: ${STEP_TIMINGS[@]}"
