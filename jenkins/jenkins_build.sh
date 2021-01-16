#!/bin/bash

set -eux

if [ "$skip_docker_build" = false ]; then
  GCR_SVCACCT_VAULT="secret/dsde/dsp-techops/common/dspci-wb-gcr-service-account.json"
  GCR_REPO_PROJ="broad-dsp-gcr-public"

  docker run --rm  -v /etc/vault-token-dsde:/root/.vault-token:ro \
       broadinstitute/dsde-toolbox:latest vault read --format=json ${GCR_SVCACCT_VAULT} \
       | jq .data > dspci-wb-gcr-service-account.json

  ./docker/build.sh jar -d push -gr "gcr.io/broad-dsp-gcr-public" \
      -k "dspci-wb-gcr-service-account.json"

  # clean up
  rm -f dspci-wb-gcr-service-account.json
else
  REMOTE=$(if [[ ${BRANCH} == "update/"* ]]; then echo "scalaSteward"; else echo "origin"; fi)
  GIT_SHA=$(git rev-parse ${REMOTE}/${BRANCH})
  echo GIT_SHA=$GIT_SHA > env.properties
fi

