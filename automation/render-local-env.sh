#!/usr/bin/env bash

WORKING_DIR=${1-$(pwd)}
VAULT_TOKEN=$2
ENV=$3
LOCAL_UI=false
LOCAL_UI_PARAM=$4
if [ "$LOCAL_UI_PARAM" = "local_ui" ]; then
  LOCAL_UI=true 
fi

# render ctmpls
docker pull broadinstitute/dsde-toolbox:dev
docker run --rm -e VAULT_TOKEN=${VAULT_TOKEN} \
    -e ENVIRONMENT=${ENV} -e ROOT_DIR=${WORKING_DIR} -v ${WORKING_DIR}:/working \
    -e OUT_PATH=/working/src/test/resources -e INPUT_PATH=/working/docker -e LOCAL_UI=$LOCAL_UI \
    broadinstitute/dsde-toolbox:dev render-templates.sh
