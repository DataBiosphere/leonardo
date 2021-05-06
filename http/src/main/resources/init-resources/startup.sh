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
export JUPYTER_SERVER_NAME=$(jupyterServerName)
export RSTUDIO_SERVER_NAME=$(rstudioServerName)
export WELDER_SERVER_NAME=$(welderServerName)
export NOTEBOOKS_DIR=$(notebooksDir)
export JUPYTER_DOCKER_IMAGE=$(jupyterDockerImage)
export RSTUDIO_DOCKER_IMAGE=$(rstudioDockerImage)
export WELDER_ENABLED=$(welderEnabled)
export UPDATE_WELDER=$(updateWelder)
export WELDER_DOCKER_IMAGE=$(welderDockerImage)
export DISABLE_DELOCALIZATION=$(disableDelocalization)
export STAGING_BUCKET=$(stagingBucketName)
export START_USER_SCRIPT_URI=$(startUserScriptUri)
export START_USER_SCRIPT_OUTPUT_URI=$(startUserScriptOutputUri)
export WELDER_MEM_LIMIT=$(welderMemLimit)
export MEM_LIMIT=$(memLimit)
export USE_GCE_STARTUP_SCRIPT=$(useGceStartupScript)

function failScriptIfError() {
  if [ $EXIT_CODE -ne 0 ]; then
    echo "Fail to docker-compose start welder ${EXIT_CODE}. Output is saved to ${START_USER_SCRIPT_OUTPUT_URI}"
    retry 3 gsutil -h "x-goog-meta-passed":"false" cp start_output.txt ${START_USER_SCRIPT_OUTPUT_URI}
    exit $EXIT_CODE
  else
    retry 3 gsutil -h "x-goog-meta-passed":"true" cp start_output.txt ${START_USER_SCRIPT_OUTPUT_URI}
  fi
}

# Overwrite old cert on restart
SERVER_CRT=$(proxyServerCrt)
SERVER_KEY=$(proxyServerKey)
ROOT_CA=$(rootCaPem)

## This helps when we need to rotate certs.
notAfter=`openssl x509 -enddate -noout -in /certs/jupyter-server.crt` # output should be something like `notAfter=Jul 22 13:09:15 2023 GMT`

## If cert is old, then pull latest certs. Update date if we need to rotate cert again
if [[ "$notAfter" != *"notAfter=Jul 22"* ]] ; then
  gsutil cp ${SERVER_CRT} /certs
  gsutil cp ${SERVER_KEY} /certs
  gsutil cp ${ROOT_CA} /certs
  docker-compose -f /etc/proxy-docker-compose.yaml restart &> start_output.txt || EXIT_CODE=$?

  failScriptIfError
fi

JUPYTER_HOME=/etc/jupyter

# TODO: remove this block once data syncing is rolled out to Terra
if [ "$DISABLE_DELOCALIZATION" == "true" ] ; then
    echo "Disabling localization on cluster $GOOGLE_PROJECT / $CLUSTER_NAME..."
    docker exec -i jupyter-server bash -c "find $JUPYTER_USER_HOME -name .cache -prune -or -name .delocalize.json -exec rm -f {} \;"
fi

if [ "$UPDATE_WELDER" == "true" ] ; then
    # Run welder-docker-compose
    gcloud auth configure-docker
    retry 5 docker-compose -f /etc/welder-docker-compose.yaml pull
    docker-compose -f /etc/welder-docker-compose.yaml stop
    docker-compose -f /etc/welder-docker-compose.yaml rm -f
    docker-compose -f /etc/welder-docker-compose.yaml up -d &> start_output.txt || EXIT_CODE=$?

    failScriptIfError
fi

# If a start user script was specified, execute it now. It should already be in the docker container
# via initialization in init-actions.sh (we explicitly do not want to recopy it from GCS on every cluster resume).
if [ ! -z ${START_USER_SCRIPT_URI} ] ; then
  START_USER_SCRIPT=`basename ${START_USER_SCRIPT_URI}`
  log 'Executing Jupyter user start script [$START_USER_SCRIPT]...'
  if [ "$USE_GCE_STARTUP_SCRIPT" == "true" ] ; then
    docker exec --privileged -u root -e PIP_TARGET=/usr/local/lib/python3.7/dist-packages ${JUPYTER_SERVER_NAME} ${JUPYTER_HOME}/${START_USER_SCRIPT} &> start_output.txt || EXIT_CODE=$?
  else
    docker exec --privileged -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_HOME}/${START_USER_SCRIPT} &> start_output.txt || EXIT_CODE=$?
  fi

  failScriptIfError
fi

# By default GCE restarts containers on exit so we're not explicitly starting them below

# Configuring Jupyter
if [ ! -z "$JUPYTER_DOCKER_IMAGE" ] ; then
    echo "Starting Jupyter on cluster $GOOGLE_PROJECT / $CLUSTER_NAME..."

    # update container MEM_LIMIT to reflect VM's MEM_LIMIT
    docker update $JUPYTER_SERVER_NAME --memory $MEM_LIMIT

    # See IA-1901: Jupyter UI stalls indefinitely on initial R kernel connection after cluster create/resume
    # The intent of this is to "warm up" R at VM creation time to hopefully prevent issues when the Jupyter
    # kernel tries to connect to it.
    docker exec $JUPYTER_SERVER_NAME /bin/bash -c "R -e '1+1'" || true

    docker exec -d $JUPYTER_SERVER_NAME /bin/bash -c "export WELDER_ENABLED=$WELDER_ENABLED && export NOTEBOOKS_DIR=$NOTEBOOKS_DIR && (/etc/jupyter/scripts/run-jupyter.sh $NOTEBOOKS_DIR || /usr/local/bin/jupyter notebook)"

    if [ "$WELDER_ENABLED" == "true" ] ; then
        # fix for https://broadworkbench.atlassian.net/browse/IA-1453
        # TODO: remove this when we stop supporting the legacy docker image
        docker exec -u root jupyter-server sed -i -e 's/export WORKSPACE_NAME=.*/export WORKSPACE_NAME="$(basename "$(dirname "$(pwd)")")"/' /etc/jupyter/scripts/kernel/kernel_bootstrap.sh
    fi
fi

# Configuring RStudio, if enabled
if [ ! -z "$RSTUDIO_DOCKER_IMAGE" ] ; then
    echo "Starting RStudio on cluster $GOOGLE_PROJECT / $CLUSTER_NAME..."

    # update container MEM_LIMIT to reflect VM's MEM_LIMIT
    docker update $RSTUDIO_SERVER_NAME --memory $MEM_LIMIT

    # Warm up R before starting the RStudio session (see above comment).
    docker exec $RSTUDIO_SERVER_NAME /bin/bash -c "R -e '1+1'" || true

    # Start RStudio server
    docker exec -d $RSTUDIO_SERVER_NAME /init
fi

# Configuring Welder, if enabled
if [ "$WELDER_ENABLED" == "true" ] ; then
    echo "Starting Welder on cluster $GOOGLE_PROJECT / $CLUSTER_NAME..."
    docker exec -d $WELDER_SERVER_NAME /bin/bash -c "export STAGING_BUCKET=$STAGING_BUCKET && /opt/docker/bin/entrypoint.sh"
fi

# Resize persistent disk if needed.
echo "Resizing persistent disk attached to runtime $GOOGLE_PROJECT / $CLUSTER_NAME if disk size changed..."
resize2fs /dev/sdb
