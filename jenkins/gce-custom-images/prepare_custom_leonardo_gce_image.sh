#!/usr/bin/env bash

set -e -x

#
# This script sets up a custom Dataproc image for Leonardo clusters.
# See: https://cloud.google.com/dataproc/docs/guides/dataproc-images
# The service account used to run the parent python script must have the following permissions:
# [  "roles/compute.admin",
#    "roles/iam.serviceAccountUser",
#    "roles/storage.objectViewer",
#    "roles/dataproc.editor" ]

#
# Constants and Global Vars
# the image tags are set via jenkins automation
#

# The versions below don't matter; they are replaced by the Jenkins job
terra_jupyter_base="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-base:0.0.6"
terra_jupyter_python="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-python:0.0.6"
terra_jupyter_r="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-r:0.0.7"
terra_jupyter_bioconductor="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-bioconductor:0.0.9"
terra_jupyter_hail="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-hail:0.0.5"
terra_jupyter_gatk="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-gatk:0.0.8"

#leonardo_jupyter will be discontinued soon
leonardo_jupyter="us.gcr.io/broad-dsp-gcr-public/leonardo-jupyter:5c51ce6935da"
welder_server="us.gcr.io/broad-dsp-gcr-public/welder-server:60e28bc"
openidc_proxy="broadinstitute/openidc-proxy:2.3.1_2"
anvil_rstudio_base="us.gcr.io/anvil-gcr-public/anvil-rstudio-base:0.0.2"
anvil_rstudio_bioconductor="us.gcr.io/anvil-gcr-public/anvil-rstudio-bioconductor:0.0.3"

# this array determines which of the above images are baked into the custom image
# the entry must match the var name above, which must correspond to a valid docker URI
docker_image_var_names="welder_server leonardo_jupyter terra_jupyter_base terra_jupyter_python terra_jupyter_r terra_jupyter_bioconductor terra_jupyter_hail terra_jupyter_gatk openidc_proxy anvil_rstudio_base anvil_rstudio_bioconductor"

# The version of python to install
python_version="3.7.4"

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
log 'Installing prerequisites...'

# Obtain the latest valid apt-key.gpg key file from https://packages.cloud.google.com to work
# around intermittent apt authentication errors. See:
# https://cloud.google.com/compute/docs/troubleshooting/known-issues
retry 5 curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
retry 5 apt-key update

# Shut down the instance after it is done, which is used by the Daisy workflow's wait-for-inst-install
# step to determine when provisioning is done.
shutdown -h now
