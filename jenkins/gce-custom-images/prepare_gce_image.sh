#!/usr/bin/env bash

set -e -x

#
# This script is used to cache various container images in a snapshot that is then referenced
# by Leonardo while creating new VMs on behalf of clients so the creation time is shorter.
#
# The service account used to run the parent script must have the following permissions:
# TODO: Verify the list below is correct when the snapshot creation is Jenkinsified
# [  "roles/compute.admin",
#    "roles/iam.serviceAccountUser",
#    "roles/storage.objectViewer",

#
# Constants and Global Vars
#

terra_jupyter_python="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-python:1.1.5"
terra_jupyter_r="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-r:2.2.5"
terra_jupyter_bioconductor="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-bioconductor:2.2.5"
terra_jupyter_gatk="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-gatk:2.3.7"
terra_jupyter_aou="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-aou:2.2.12"
welder_server="us.gcr.io/broad-dsp-gcr-public/welder-server:8667bfe"
openidc_proxy="broadinstitute/openidc-proxy:2.3.1_2"
anvil_rstudio_bioconductor="us.gcr.io/broad-dsp-gcr-public/anvil-rstudio-bioconductor:3.19.0"

# Note that this is the version used currently by AOU in production, the one above can be staged for testing
terra_jupyter_aou_old="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-aou:2.2.7"

cos_gpu_installer="gcr.io/cos-cloud/cos-gpu-installer:v2.1.9"
google_cloud_toolbox="us.gcr.io/cos-cloud/toolbox:v20230714"
docker_composer="docker/compose:1.29.2"
docker_composer_with_auth="cryptopants/docker-compose-gcr"

# If you change this you must also change Leo reference.conf!
cryptomining_detector="us.gcr.io/broad-dsp-gcr-public/cryptomining-detector:0.0.2"

# This array determines which of the above images are baked into the custom image
# the entry must match the var name above, which must correspond to a valid docker URI
docker_image_var_names="welder_server terra_jupyter_python terra_jupyter_r terra_jupyter_bioconductor terra_jupyter_gatk terra_jupyter_aou terra_jupyter_aou_old openidc_proxy anvil_rstudio_bioconductor cryptomining_detector cos_gpu_installer google_cloud_toolbox docker_composer docker_composer_with_auth"

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
#
function retry {
    local retries=$1
    shift

    for ((i = 1; i <= retries; i++))
    do
        # run with an 'or' so set -e doesn't abort the bash script on errors
        exit=0
        "$@" || exit=$?
        if [[ $exit -eq 0 ]]; then
            return 0
        fi
        wait=$((2 ** i))
        if [[ $i -eq $retries ]]; then
            log "Retry $i/$retries exited $exit, no more retries left."
            break
        fi
        log "Retry $i/$retries exited $exit, retrying in $wait seconds..."
        sleep $wait
    done
    return 1
}

function log() {
    printf '[%s]: %s\n' \
        "$(date +'%Y-%m-%dT%H:%M:%S%z')" \
        "$*"
}

#
# Main
#

# Pull the docker images -- this caches them in the GCE snapshot.
if [[ -n ${docker_image_var_names:?} ]]; then
    for _docker_image_var_name in ${docker_image_var_names:?}
    do
        _docker_image="${!_docker_image_var_name:?}"
        retry 5 docker pull "${_docker_image:?}"
    done
else
    log "ERROR-VAR_NULL_OR_UNSET: docker_image_var_names. Will not pull docker images."
fi

log 'Cached docker images:'
docker images

# Shut down the instance after it is done, which is used by the Daisy workflow's wait-for-inst-install
# step to determine when provisioning is done.
shutdown -h now
