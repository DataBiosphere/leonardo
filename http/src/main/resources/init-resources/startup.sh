#!/usr/bin/env bash

set -e -x

##
# This is a startup script designed to run on Leo-created Dataproc clusters.
#
# It starts up Jupyter and Welder processes. It also optionally deploys welder on a
# cluster if not already installed.
##

#
# Functions
# (copied from init-actions.sh, see documentation there)
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

#
# Main
#

# Templated values
export GOOGLE_PROJECT=$(googleProject)
export CLUSTER_NAME=$(clusterName)
export OWNER_EMAIL=$(loginHint)
export JUPYTER_SERVER_NAME=$(jupyterServerName)
export WELDER_SERVER_NAME=$(welderServerName)
export NOTEBOOKS_DIR=$(notebooksDir)
export JUPYTER_DOCKER_IMAGE=$(jupyterDockerImage)
export WELDER_ENABLED=$(welderEnabled)
export DEPLOY_WELDER=$(deployWelder)
export UPDATE_WELDER=$(updateWelder)
export WELDER_DOCKER_IMAGE=$(welderDockerImage)
export DISABLE_DELOCALIZATION=$(disableDelocalization)
export STAGING_BUCKET=$(stagingBucketName)
export JUPYTER_START_USER_SCRIPT_URI=$(jupyterStartUserScriptUri)
# Include a timestamp suffix to differentiate different startup logs across restarts.
export JUPYTER_START_USER_SCRIPT_OUTPUT_URI="$(jupyterStartUserScriptOutputBaseUri)-$(date -u "+%Y.%m.%d-%H.%M.%S").txt"

JUPYTER_HOME=/etc/jupyter

# TODO: remove this block once data syncing is rolled out to Terra
if [ "$DEPLOY_WELDER" == "true" ] ; then
    echo "Deploying Welder on cluster $GOOGLE_PROJECT / $CLUSTER_NAME..."

    # Run welder-docker-compose
    gcloud auth configure-docker
    docker-compose -f /etc/welder-docker-compose.yaml up -d

    # Move existing notebooks to new notebooks dir
    docker exec -i $JUPYTER_SERVER_NAME bash -c "ls -I notebooks -I miniconda /home/jupyter-user | xargs -d '\n'  -I file mv file $NOTEBOOKS_DIR"

    # Enable welder in /etc/jupyter/nbconfig/notebook.json (which powers the front-end extensions like edit.js and safe.js)
    docker exec -u root -i $JUPYTER_SERVER_NAME bash -c \
      "test -f /etc/jupyter/nbconfig/notebook.json && jq '.welderEnabled=\"true\"' /etc/jupyter/nbconfig/notebook.json > /etc/jupyter/nbconfig/notebook.json.tmp && mv /etc/jupyter/nbconfig/notebook.json.tmp /etc/jupyter/nbconfig/notebook.json || true"
fi

# TODO: remove this block once data syncing is rolled out to Terra
if [ "$DISABLE_DELOCALIZATION" == "true" ] ; then
    echo "Disabling localization on cluster $GOOGLE_PROJECT / $CLUSTER_NAME..."
    docker exec -i jupyter-server bash -c "find /home/jupyter-user -name .cache -prune -or -name .delocalize.json -exec rm -f {} \;"
fi

if [ "$UPDATE_WELDER" == "true" ] ; then
    # Run welder-docker-compose
    gcloud auth configure-docker
    docker-compose -f /etc/welder-docker-compose.yaml stop
    docker-compose -f /etc/welder-docker-compose.yaml rm -f
    docker-compose -f /etc/welder-docker-compose.yaml up -d
fi

# If a Jupyter start user script was specified, execute it now. It should already be in the docker container
# via initialization in init-actions.sh (we explicitly do not want to recopy it from GCS on every cluster resume).
if [ ! -z ${JUPYTER_START_USER_SCRIPT_URI} ] ; then
  JUPYTER_START_USER_SCRIPT=`basename ${JUPYTER_START_USER_SCRIPT_URI}`
  log 'Executing Jupyter user start script [$JUPYTER_START_USER_SCRIPT]...'
  EXIT_CODE=0
  docker exec --privileged -u root -e PIP_USER=false ${JUPYTER_SERVER_NAME} ${JUPYTER_HOME}/${JUPYTER_START_USER_SCRIPT} &> start_output.txt || EXIT_CODE=$?
  if [ $EXIT_CODE -ne 0 ]; then
    echo "User start script failed with exit code ${EXIT_CODE}. Output is saved to ${JUPYTER_START_USER_SCRIPT_OUTPUT_URI}"
    retry 3 gsutil -h "x-goog-meta-passed":"false" cp start_output.txt ${JUPYTER_START_USER_SCRIPT_OUTPUT_URI}
    exit $EXIT_CODE
  else
    retry 3 gsutil -h "x-goog-meta-passed":"true" cp start_output.txt ${JUPYTER_START_USER_SCRIPT_OUTPUT_URI}
  fi
fi

# By default GCE restarts containers on exit so we're not explicitly starting them below

# Configuring Jupyter
if [ ! -z "$JUPYTER_DOCKER_IMAGE" ] ; then
    echo "Starting Jupyter on cluster $GOOGLE_PROJECT / $CLUSTER_NAME..."
    docker exec -d $JUPYTER_SERVER_NAME /bin/bash -c "export WELDER_ENABLED=$WELDER_ENABLED && export NOTEBOOKS_DIR=$NOTEBOOKS_DIR && (/etc/jupyter/scripts/run-jupyter.sh $NOTEBOOKS_DIR || /usr/local/bin/jupyter notebook)"

    if [ "$WELDER_ENABLED" == "true" ] ; then
        # fix for https://broadworkbench.atlassian.net/browse/IA-1453
        # TODO: remove this when we stop supporting the legacy docker image
        docker exec -u root jupyter-server sed -i -e 's/export WORKSPACE_NAME=.*/export WORKSPACE_NAME="$(basename "$(dirname "$(pwd)")")"/' /etc/jupyter/scripts/kernel/kernel_bootstrap.sh
    fi
fi

# Configuring Welder, if enabled
if [ "$WELDER_ENABLED" == "true" ] ; then
    echo "Starting Welder on cluster $GOOGLE_PROJECT / $CLUSTER_NAME..."
    docker exec -d $WELDER_SERVER_NAME /bin/bash -c "export STAGING_BUCKET=$STAGING_BUCKET && /opt/docker/bin/entrypoint.sh"
fi
