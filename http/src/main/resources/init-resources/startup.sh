#!/usr/bin/env bash

set -e -x

##
# This is a startup script designed to run on Leo-created Dataproc clusters and GCE VMs.
#
# It starts up Jupyter and Welder processes. It also optionally deploys welder on a
# cluster if not already installed.
##

#
# Functions
# (copied from init-actions.sh and gce-init.sh, see documentation there)
#
EXIT_CODE=0

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
#
# Main
#

# Templated values
export JUPYTER_USER_HOME=$(jupyterHomeDirectory)
export GOOGLE_PROJECT=$(googleProject)
export CLUSTER_NAME=$(clusterName)
export RUNTIME_NAME=$(clusterName)
export OWNER_EMAIL=$(loginHint)
export PET_SA_EMAIL=$(petSaEmail)
export JUPYTER_SERVER_NAME=$(jupyterServerName)
export RSTUDIO_SERVER_NAME=$(rstudioServerName)
export WELDER_SERVER_NAME=$(welderServerName)
export CRYPTO_DETECTOR_SERVER_NAME=$(cryptoDetectorServerName)
export NOTEBOOKS_DIR=$(notebooksDir)
export JUPYTER_DOCKER_IMAGE=$(jupyterDockerImage)
export RSTUDIO_DOCKER_IMAGE=$(rstudioDockerImage)
export CRYPTO_DETECTOR_DOCKER_IMAGE=$(cryptoDetectorDockerImage)
export WELDER_ENABLED=$(welderEnabled)
export UPDATE_WELDER=$(updateWelder)
export WELDER_DOCKER_IMAGE=$(welderDockerImage)
export DISABLE_DELOCALIZATION=$(disableDelocalization)
export STAGING_BUCKET=$(stagingBucketName)
export START_USER_SCRIPT_URI=$(startUserScriptUri)
export START_USER_SCRIPT_OUTPUT_URI=$(startUserScriptOutputUri)
export WELDER_MEM_LIMIT=$(welderMemLimit)
export MEM_LIMIT=$(memLimit)
export INIT_BUCKET_NAME=$(initBucketName)
export USE_GCE_STARTUP_SCRIPT=$(useGceStartupScript)
export PROXY_DOCKER_COMPOSE=$(proxyDockerCompose)
JUPYTER_NOTEBOOK_FRONTEND_CONFIG_URI=$(jupyterNotebookFrontendConfigUri)
GPU_ENABLED=$(gpuEnabled)
if [ ! -z "$RSTUDIO_DOCKER_IMAGE" ] ; then
  export SHOULD_BACKGROUND_SYNC="true"
else
  export SHOULD_BACKGROUND_SYNC="false"
fi

# Overwrite old cert on restart
SERVER_CRT=$(proxyServerCrt)
SERVER_KEY=$(proxyServerKey)
ROOT_CA=$(rootCaPem)
FILE=/var/certs/jupyter-server.crt
USER_DISK_DEVICE_ID=$(lsblk -o name,serial | grep 'user-disk' | awk '{print $1}')
DISK_DEVICE_ID=${USER_DISK_DEVICE_ID:-sdb}
JUPYTER_HOME=/etc/jupyter
RSTUDIO_SCRIPTS=/etc/rstudio/scripts

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
fi

# https://broadworkbench.atlassian.net/browse/IA-3186
# This condition assumes Dataproc's cert directory is different from GCE's cert directory, a better condition would be
# a dedicated flag that distinguishes gce and dataproc. But this will do for now
if [ -f "$FILE" ]
then
    CERT_DIRECTORY='/var/certs'
    DOCKER_COMPOSE_FILES_DIRECTORY='/var/docker-compose-files'
    GSUTIL_CMD='docker run --rm -v /var:/var us.gcr.io/cos-cloud/toolbox:v20230714 gsutil'
    GCLOUD_CMD='docker run --rm -v /var:/var us.gcr.io/cos-cloud/toolbox:v20230714 gcloud'
    DOCKER_COMPOSE='docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v /var:/var docker/compose:1.29.2'
    WELDER_DOCKER_COMPOSE=$(ls ${DOCKER_COMPOSE_FILES_DIRECTORY}/welder*)
    JUPYTER_DOCKER_COMPOSE=$(ls ${DOCKER_COMPOSE_FILES_DIRECTORY}/jupyter-docker*)
    export WORK_DIRECTORY='/mnt/disks/work'

    fsck.ext4 -tvy /dev/${DISK_DEVICE_ID}
    mkdir -p /mnt/disks/work
    mount -t ext4 -O discard,defaults /dev/${DISK_DEVICE_ID} ${WORK_DIRECTORY}
    chmod a+rwx /mnt/disks/work

    # (1/6/22) Restart Jupyter Container to reset `NOTEBOOKS_DIR` for existing runtimes. This code can probably be removed after a year
    if [ ! -z "$JUPYTER_DOCKER_IMAGE" ] ; then
        echo "Restarting Jupyter Container $GOOGLE_PROJECT / $CLUSTER_NAME..."

        # the docker containers need to be restarted or the jupyter container
        # will fail to start until the appropriate volume/device exists
        docker restart jupyter-server
        docker restart welder-server

        # This line is only for migration (1/26/2022). Say you have an existing runtime where jupyter container's PD is mapped at $HOME/notebooks,
        # then all jupyter related files (.jupyter, .local) and things like bash history etc all lives under $HOME. The home diretory change will
        # make it so that next time this runtime starts up, PD will be mapped to $HOME, but this means that the previous files under $HOME (.jupyter, .local etc)
        # will be lost....So this one line is to before we restart jupyter container with updated home directory mapping,
        # we will copy all files under $HOME to $HOME/notebooks first, which will live on PD...So when it starts up,
        # what was previously under $HOME will now appear in new $HOME as well
        NEED_MIGRATE=$(docker exec $JUPYTER_SERVER_NAME /bin/bash -c "[ -d $JUPYTER_USER_HOME/notebooks ] && echo 'true' || echo 'false'")

        if [ "$NEED_MIGRATE" == "true" ] ; then
          docker exec $JUPYTER_SERVER_NAME /bin/bash -c "[ ! -d $JUPYTER_USER_HOME/notebooks/.jupyter ] && rsync -av --progress --exclude notebooks . $JUPYTER_USER_HOME/notebooks || true"

          # Make sure when runtimes restarts, they'll get a new version of jupyter docker compose file
          $GSUTIL_CMD cp gs://${INIT_BUCKET_NAME}/`basename ${JUPYTER_DOCKER_COMPOSE}` $JUPYTER_DOCKER_COMPOSE

          tee /var/variables.env << END
JUPYTER_SERVER_NAME=${JUPYTER_SERVER_NAME}
JUPYTER_DOCKER_IMAGE=${JUPYTER_DOCKER_IMAGE}
NOTEBOOKS_DIR=${NOTEBOOKS_DIR}
GOOGLE_PROJECT=${GOOGLE_PROJECT}
RUNTIME_NAME=${RUNTIME_NAME}
OWNER_EMAIL=${OWNER_EMAIL}
PET_SA_EMAIL=${PET_SA_EMAIL}
WELDER_ENABLED=${WELDER_ENABLED}
MEM_LIMIT=${MEM_LIMIT}
END

          ${DOCKER_COMPOSE} -f ${JUPYTER_DOCKER_COMPOSE} stop
          ${DOCKER_COMPOSE} -f ${JUPYTER_DOCKER_COMPOSE} rm -f
          ${DOCKER_COMPOSE} --env-file=/var/variables.env -f ${JUPYTER_DOCKER_COMPOSE} up -d

          log 'Copy Jupyter frontend notebook config...'
          $GSUTIL_CMD cp ${JUPYTER_NOTEBOOK_FRONTEND_CONFIG_URI} /var
          JUPYTER_NOTEBOOK_FRONTEND_CONFIG=`basename ${JUPYTER_NOTEBOOK_FRONTEND_CONFIG_URI}`
          retry 3 docker exec -u root ${JUPYTER_SERVER_NAME} /bin/bash -c "mkdir -p $JUPYTER_HOME/nbconfig"
          docker cp /var/${JUPYTER_NOTEBOOK_FRONTEND_CONFIG} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/nbconfig/
        fi
    fi

    if [ ! -z "$RSTUDIO_DOCKER_IMAGE" ] ; then
        echo "Restarting Rstudio Container $GOOGLE_PROJECT / $CLUSTER_NAME..."

        # the docker containers need to be restarted or the jupyter container
        # will fail to start until the appropriate volume/device exists
        docker restart $RSTUDIO_SERVER_NAME

    fi

    if [ "$UPDATE_WELDER" == "true" ] ; then
        echo "Upgrading welder..."

        # Make sure when runtimes restarts, they'll get a new version of jupyter docker compose file
        $GSUTIL_CMD cp gs://${INIT_BUCKET_NAME}/`basename ${WELDER_DOCKER_COMPOSE}` $WELDER_DOCKER_COMPOSE

tee /var/welder-variables.env << END
WORK_DIRECTORY=${WORK_DIRECTORY}
GOOGLE_PROJECT=${GOOGLE_PROJECT}
RUNTIME_NAME=${RUNTIME_NAME}
OWNER_EMAIL=${OWNER_EMAIL}
PET_SA_EMAIL=${PET_SA_EMAIL}
WELDER_ENABLED=${WELDER_ENABLED}
WELDER_SERVER_NAME=${WELDER_SERVER_NAME}
WELDER_DOCKER_IMAGE=${WELDER_DOCKER_IMAGE}
STAGING_BUCKET=${STAGING_BUCKET}
WELDER_MEM_LIMIT=${WELDER_MEM_LIMIT}
SHOULD_BACKGROUND_SYNC=${SHOULD_BACKGROUND_SYNC}
END

        ${DOCKER_COMPOSE} -f ${WELDER_DOCKER_COMPOSE} stop
        ${DOCKER_COMPOSE} -f ${WELDER_DOCKER_COMPOSE} rm -f
        ${DOCKER_COMPOSE} --env-file=/var/welder-variables.env -f ${WELDER_DOCKER_COMPOSE} up -d &> /var/start_output.txt || EXIT_CODE=$?
    fi
else
    CERT_DIRECTORY='/certs'
    DOCKER_COMPOSE_FILES_DIRECTORY='/etc'
    GSUTIL_CMD='gsutil'
    GCLOUD_CMD='gcloud'
    DOCKER_COMPOSE='docker-compose'
    WELDER_DOCKER_COMPOSE=$(ls ${DOCKER_COMPOSE_FILES_DIRECTORY}/welder*)
    JUPYTER_DOCKER_COMPOSE=$(ls ${DOCKER_COMPOSE_FILES_DIRECTORY}/jupyter-docker*)
    export WORK_DIRECTORY=/work

    if [ ! -z "$JUPYTER_DOCKER_IMAGE" ] ; then
        echo "Restarting Jupyter Container $GOOGLE_PROJECT / $CLUSTER_NAME..."

        # This line is only for migration (1/26/2022). Say you have an existing runtime where jupyter container's PD is mapped at $HOME/notebooks,
        # then all jupyter related files (.jupyter, .local) and things like bash history etc all lives under $HOME. The home diretory change will
        # make it so that next time this runtime starts up, PD will be mapped to $HOME, but this means that the previous files under $HOME (.jupyter, .local etc)
        # will be lost....So this one line is to before we restart jupyter container with updated home directory mapping,
        # we will copy all files under $HOME to $HOME/notebooks first, which will live on PD...So when it starts up,
        # what was previously under $HOME will now appear in new $HOME as well
        NEED_MIGRATE=$(docker exec $JUPYTER_SERVER_NAME /bin/bash -c "[ -d $JUPYTER_USER_HOME/notebooks ] && echo 'true' || echo 'false'")

        if [ "$NEED_MIGRATE" == "true" ] ; then
          docker exec $JUPYTER_SERVER_NAME /bin/bash -c "[ ! -d $JUPYTER_USER_HOME/notebooks/.jupyter ] && rsync -avr --progress --exclude notebooks . $JUPYTER_USER_HOME/notebooks || true"

          # Make sure when runtimes restarts, they'll get a new version of jupyter docker compose file
          $GSUTIL_CMD cp gs://${INIT_BUCKET_NAME}/`basename ${JUPYTER_DOCKER_COMPOSE}` $JUPYTER_DOCKER_COMPOSE

          ${DOCKER_COMPOSE} -f ${JUPYTER_DOCKER_COMPOSE} stop
          ${DOCKER_COMPOSE} -f ${JUPYTER_DOCKER_COMPOSE} rm -f
          ${DOCKER_COMPOSE} -f ${JUPYTER_DOCKER_COMPOSE} up -d

          log 'Copy Jupyter frontend notebook config...'
          $GSUTIL_CMD cp ${JUPYTER_NOTEBOOK_FRONTEND_CONFIG_URI} /var
          JUPYTER_NOTEBOOK_FRONTEND_CONFIG=`basename ${JUPYTER_NOTEBOOK_FRONTEND_CONFIG_URI}`
          retry 3 docker exec -u root ${JUPYTER_SERVER_NAME} /bin/bash -c "mkdir -p $JUPYTER_HOME/nbconfig"
          docker cp /var/${JUPYTER_NOTEBOOK_FRONTEND_CONFIG} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/nbconfig/
        fi
        # jupyter_delocalize.py now assumes welder's url is `http://welder:8080`, but on dataproc, we're still using host network
        # A better to do this might be to take welder host as an argument to the script
        docker exec $JUPYTER_SERVER_NAME /bin/bash -c "sed -i 's/http:\/\/welder/http:\/\/127.0.0.1/g' /etc/jupyter/custom/jupyter_delocalize.py"
    fi

    if [ "$WELDER_ENABLED" == "true" ] ; then
      # Update old welder docker-compose file's entrypoint
      sed -i  "s/tail -f \/dev\/null/\/opt\/docker\/bin\/entrypoint.sh/g" /etc/welder-docker-compose.yaml

      ${DOCKER_COMPOSE} -f ${WELDER_DOCKER_COMPOSE} stop
      ${DOCKER_COMPOSE} -f ${WELDER_DOCKER_COMPOSE} rm -f
      ${DOCKER_COMPOSE} -f ${WELDER_DOCKER_COMPOSE} up -d &> /var/start_output.txt || EXIT_CODE=$?
    fi
fi

function failScriptIfError() {
  if [ $EXIT_CODE -ne 0 ]; then
    echo "Fail to docker-compose start container ${EXIT_CODE}. Output is saved to ${START_USER_SCRIPT_OUTPUT_URI}"
    retry 3 ${GSUTIL_CMD} -h "x-goog-meta-passed":"false" cp /var/start_output.txt ${START_USER_SCRIPT_OUTPUT_URI}
    exit $EXIT_CODE
  else
    retry 3 ${GSUTIL_CMD} -h "x-goog-meta-passed":"true" cp /var/start_output.txt ${START_USER_SCRIPT_OUTPUT_URI}
  fi
}

function validateCert() {
  certFileDirectory=$1
  ## This helps when we need to rotate certs.
  notAfter=`openssl x509 -enddate -noout -in ${certFileDirectory}/jupyter-server.crt` # output should be something like `notAfter=Jul  4 20:31:52 2026 GMT`

  ## If cert is old, then pull latest certs. Update date if we need to rotate cert again
  if [[ "$notAfter" != *"notAfter=Jul  4"* ]] ; then
    ${GSUTIL_CMD} cp ${SERVER_CRT} ${certFileDirectory}
    ${GSUTIL_CMD} cp ${SERVER_KEY} ${certFileDirectory}
    ${GSUTIL_CMD} cp ${ROOT_CA} ${certFileDirectory}

    IMAGES_TO_RESTART=(-f /var/docker-compose-files/proxy-docker-compose-gce.yaml)
    DATAPROC_IMAGES_TO_RESTART=(-f /etc/proxy-docker-compose.yaml)
    if [ ! -z ${WELDER_DOCKER_IMAGE} ] && [ "${WELDER_ENABLED}" == "true" ]; then
      IMAGES_TO_RESTART+=(-f /var/docker-compose-files/welder-docker-compose-gce.yaml)
      DATAPROC_IMAGES_TO_RESTART+=(-f /etc/welder-docker-compose.yaml)
    fi
    if [[ ! -z "$RSTUDIO_DOCKER_IMAGE" ]] ; then
      IMAGES_TO_RESTART+=(-f /var/docker-compose-files/rstudio-docker-compose-gce.yaml)
    fi
    if [[ ! -z "$JUPYTER_DOCKER_IMAGE" ]] ; then
      IMAGES_TO_RESTART+=(-f /var/docker-compose-files/jupyter-docker-compose-gce.yaml)
      DATAPROC_IMAGES_TO_RESTART+=(-f /etc/jupyter-docker-compose.yaml )
    fi

    if [ "$certFileDirectory" = "/certs" ] #if its dataproc the cert directory is '/certs', and we can assume the docker images present
    then
      ${DOCKER_COMPOSE} "${DATAPROC_IMAGES_TO_RESTART[@]}" restart &> /var/start_output.txt || EXIT_CODE=$?
    else
      ${DOCKER_COMPOSE} --env-file=/var/variables.env "${IMAGES_TO_RESTART[@]}" restart &> /var/start_output.txt || EXIT_CODE=$?
    fi

    failScriptIfError ${GSUTIL_CMD}
    retry 3 ${GSUTIL_CMD} -h "x-goog-meta-passed":"true" cp /var/start_output.txt ${START_USER_SCRIPT_OUTPUT_URI}
  fi
}

validateCert ${CERT_DIRECTORY}

# TODO: remove this block once data syncing is rolled out to Terra
if [ "$DISABLE_DELOCALIZATION" == "true" ] ; then
    echo "Disabling localization on cluster $GOOGLE_PROJECT / $CLUSTER_NAME..."
    docker exec -i jupyter-server bash -c "find $JUPYTER_USER_HOME -name .cache -prune -or -name .delocalize.json -exec rm -f {} \;"
fi


# If a start user script was specified, execute it now. It should already be in the docker container
# via initialization in init-actions.sh (we explicitly do not want to recopy it from GCS on every cluster resume).
if [ ! -z ${START_USER_SCRIPT_URI} ] ; then
  START_USER_SCRIPT=`basename ${START_USER_SCRIPT_URI}`
  log "Executing user start script [$START_USER_SCRIPT]..."

  if [ ! -z "$JUPYTER_DOCKER_IMAGE" ] ; then
    if [ "$USE_GCE_STARTUP_SCRIPT" == "true" ] ; then
      docker cp /var/${START_USER_SCRIPT} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${START_USER_SCRIPT}
      retry 3 docker exec -u root ${JUPYTER_SERVER_NAME} chmod +x ${JUPYTER_HOME}/${START_USER_SCRIPT}

      docker exec --privileged -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_HOME}/${START_USER_SCRIPT} &> /var/start_output.txt || EXIT_CODE=$?
    else
      docker cp /etc/${START_USER_SCRIPT} ${JUPYTER_SERVER_NAME}:${JUPYTER_HOME}/${START_USER_SCRIPT}
      retry 3 docker exec -u root ${JUPYTER_SERVER_NAME} chmod +x ${JUPYTER_HOME}/${START_USER_SCRIPT}

      docker exec --privileged -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_HOME}/${START_USER_SCRIPT} &> /var/start_output.txt || EXIT_CODE=$?
    fi
  fi

  if [ ! -z "$RSTUDIO_DOCKER_IMAGE" ] ; then
    docker cp /var/${START_USER_SCRIPT} ${RSTUDIO_SERVER_NAME}:${RSTUDIO_SCRIPTS}/${START_USER_SCRIPT}
    retry 3 docker exec -u root ${RSTUDIO_SERVER_NAME} chmod +x ${RSTUDIO_SCRIPTS}/${START_USER_SCRIPT}

    docker exec --privileged -u root ${RSTUDIO_SERVER_NAME} ${RSTUDIO_SCRIPTS}/${START_USER_SCRIPT} &> /var/start_output.txt || EXIT_CODE=$?
  fi

  failScriptIfError
fi

# By default GCE restarts containers on exit so we're not explicitly starting them below

# Configuring Jupyter
if [ ! -z "$JUPYTER_DOCKER_IMAGE" ] ; then
    echo "Starting Jupyter on cluster $GOOGLE_PROJECT / $CLUSTER_NAME..."
    TOOL_SERVER_NAME=${JUPYTER_SERVER_NAME}

    # update container MEM_LIMIT to reflect VM's MEM_LIMIT
    docker update $JUPYTER_SERVER_NAME --memory $MEM_LIMIT --memory-swap $MEM_LIMIT

    # See IA-1901: Jupyter UI stalls indefinitely on initial R kernel connection after cluster create/resume
    # The intent of this is to "warm up" R at VM creation time to hopefully prevent issues when the Jupyter
    # kernel tries to connect to it.
    docker exec $JUPYTER_SERVER_NAME /bin/bash -c "R -e '1+1'" || true

    # In new jupyter images, we should update jupyter_notebook_config.py in terra-docker.
    # This is to make it so that older images will still work after we change notebooks location to home dir
    docker exec ${JUPYTER_SERVER_NAME} sed -i '/^# to mount there as it effectively deletes existing files on the image/,+5d' ${JUPYTER_HOME}/jupyter_notebook_config.py

    docker exec -d $JUPYTER_SERVER_NAME /bin/bash -c "export WELDER_ENABLED=$WELDER_ENABLED && export NOTEBOOKS_DIR=$NOTEBOOKS_DIR && (/etc/jupyter/scripts/run-jupyter.sh $NOTEBOOKS_DIR || /opt/conda/bin/jupyter notebook)"

    if [ "$WELDER_ENABLED" == "true" ] ; then
        # fix for https://broadworkbench.atlassian.net/browse/IA-1453
        # TODO: remove this when we stop supporting the legacy docker image
        docker exec -u root jupyter-server sed -i -e 's/export WORKSPACE_NAME=.*/export WORKSPACE_NAME="$(basename "$(dirname "$(pwd)")")"/' /etc/jupyter/scripts/kernel/kernel_bootstrap.sh
    fi
fi

# Configuring RStudio, if enabled
if [ ! -z "$RSTUDIO_DOCKER_IMAGE" ] ; then
    echo "Starting RStudio on cluster $GOOGLE_PROJECT / $CLUSTER_NAME..."

    TOOL_SERVER_NAME=${RSTUDIO_SERVER_NAME}

    # update container MEM_LIMIT to reflect VM's MEM_LIMIT
    docker update $RSTUDIO_SERVER_NAME --memory $MEM_LIMIT --memory-swap $MEM_LIMIT

    # Warm up R before starting the RStudio session (see above comment).
    docker exec $RSTUDIO_SERVER_NAME /bin/bash -c "R -e '1+1'" || true

    # Start RStudio server
    docker exec -d $RSTUDIO_SERVER_NAME /init
fi

# Start up crypto detector, if enabled.
# This should be started after other containers.
# Use `docker run` instead of docker-compose so we can link it to the Jupyter/RStudio container's network.
# See https://github.com/broadinstitute/terra-cryptomining-security-alerts/tree/master/v2
if [ ! -z "$CRYPTO_DETECTOR_DOCKER_IMAGE" ] ; then
    docker run --name=${CRYPTO_DETECTOR_SERVER_NAME} --rm -d \
        --net=container:${TOOL_SERVER_NAME} ${CRYPTO_DETECTOR_DOCKER_IMAGE}
fi

# Resize persistent disk if needed.
# This condition assumes Dataproc's cert directory is different from GCE's cert directory, a better condition would be
# a dedicated flag that distinguishes gce and dataproc. But this will do for now
# If it's GCE, we resize the PD. Dataproc doesn't have PD
if [ -f "$FILE" ]; then
  echo "Resizing persistent disk attached to runtime $GOOGLE_PROJECT / $CLUSTER_NAME if disk size changed..."
  resize2fs /dev/${DISK_DEVICE_ID}
fi
