#!/usr/bin/env bash

set -e -x

#
# This script sets up a custom GCE image for Leonardo clusters.
# See: https://cloud.google.com/compute/docs/images/create-delete-deprecate-private-images
# The service account used to run the parent script must have the following permissions:
# TODO: Verify the list below is correct when the image creation is Jenkinsified
# [  "roles/compute.admin",
#    "roles/iam.serviceAccountUser",
#    "roles/storage.objectViewer",

#
# Constants and Global Vars
# the image tags are set via jenkins automation
#

# The versions below don't matter; they are replaced by the Jenkins job
terra_jupyter_base="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-base:0.0.6"
terra_jupyter_python="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-python:0.0.6"
terra_jupyter_r="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-r:0.0.7"
terra_jupyter_bioconductor="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-bioconductor:0.0.9"
terra_jupyter_gatk="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-gatk:0.0.8"

#leonardo_jupyter will be discontinued soon
leonardo_jupyter="us.gcr.io/broad-dsp-gcr-public/leonardo-jupyter:5c51ce6935da"
welder_server="us.gcr.io/broad-dsp-gcr-public/welder-server:60e28bc"
openidc_proxy="broadinstitute/openidc-proxy:2.3.1_2"
anvil_rstudio_base="us.gcr.io/anvil-gcr-public/anvil-rstudio-base:0.0.2"
anvil_rstudio_bioconductor="us.gcr.io/anvil-gcr-public/anvil-rstudio-bioconductor:0.0.3"

# this array determines which of the above images are baked into the custom image
# the entry must match the var name above, which must correspond to a valid docker URI
docker_image_var_names="welder_server leonardo_jupyter terra_jupyter_base terra_jupyter_python terra_jupyter_r terra_jupyter_bioconductor terra_jupyter_gatk openidc_proxy anvil_rstudio_base anvil_rstudio_bioconductor"

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

# install Docker
# https://docs.docker.com/install/linux/docker-ce/debian/
# export DOCKER_CE_VERSION="19.03.2~ce~3-0~debian"

# retry 5 betterAptGet
retry 5 apt-get install -y -q \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg2 \
    software-properties-common \
    libffi-dev

log 'Adding Docker package sources...'

apt-get remove docker docker-engine

# shellcheck disable=SC1091
os_dist_name="$(. /etc/os-release; echo "$ID")"
os_dist_code_name="$(lsb_release -cs)"
os_dist_release_channel="stable"
os_dist_arch="amd64"

docker_gpg_key_url="https://download.docker.com/linux/${os_dist_name:?}/gpg"
docker_apt_repo_url="https://download.docker.com/linux/${os_dist_name:?}"

retry 5 curl -fsSL "${docker_gpg_key_url:?}" | apt-key add -

add-apt-repository \
  "deb [arch=${os_dist_arch:?}] ${docker_apt_repo_url:?} \
  ${os_dist_code_name:?} \
  ${os_dist_release_channel:?}"

log 'Installing Docker...'

retry 5 apt-get update
retry 5 add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"

dpkg --configure -a
# This line fails consistently, but it does not fail in a fatal way so we add `|| true` to prevent the script from halting execution
# The message that is non-fatal is `Sub-process /usr/bin/dpkg returned an error code (1).`
# NOTE: If it fails with another legitimate error, this `|| true` could mask it. It was used as a last resort after a lot of attempts to fix.
apt-get install -y -q docker-ce || true

log 'Installing Docker Compose...'

# Install docker-compose
# https://docs.docker.com/compose/install/#install-compose
docker_compose_version_number="1.22.0"
docker_compose_kernel_name="$(uname -s)"
docker_compose_machine_hardware_name="$(uname -m)"
docker_compose_binary_download_url="https://github.com/docker/compose/releases/download/${docker_compose_version_number:?}/docker-compose-${docker_compose_kernel_name:?}-${docker_compose_machine_hardware_name:?}"
docker_compose_binary_download_target_filename="/usr/local/bin/docker-compose"

retry 5 curl -L "${docker_compose_binary_download_url:?}" -o "${docker_compose_binary_download_target_filename:?}"
chmod +x "${docker_compose_binary_download_target_filename:?}"

# Pull docker image versions as of the time this script ran; this caches them in the
# GCE custom instance image.
if [[ -n ${docker_image_var_names:?} ]]; then
    for _docker_image_var_name in ${docker_image_var_names:?}
    do
        _docker_image="${!_docker_image_var_name:?}"
        retry 5 docker pull "${_docker_image:?}"
    done
else
    log "ERROR-VAR_NULL_OR_UNSET: docker_image_var_names. Will not pull docker images."
fi

log 'Making systemd additions...'
mkdir -p /etc/systemd/system/google-startup-scripts.service.d
cat > /etc/systemd/system/google-startup-scripts.service.d/override.conf <<EOF
[Unit]
After=docker.service
EOF
mkdir -p /etc/systemd/system/google-shutdown-scripts.service.d
cat > /etc/systemd/system/google-shutdown-scripts.service.d/override.conf <<EOF
[Unit]
After=docker.service
EOF

# Shut down the instance after it is done, which is used by the Daisy workflow's wait-for-inst-install
# step to determine when provisioning is done.
shutdown -h now
