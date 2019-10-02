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

#filled out by jenkins job
terra_jupyter_base="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-base:0.0.1"
terra_jupyter_python="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-python:0.0.1"
terra_jupyter_r="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-r:0.0.3"

#bioconductor and hail currently are not baked into the custom image
terra_jupyter_bioconductor="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-bioconductor:0.0.2"
terra_jupyter_hail="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-hail:0.0.1"

#leonardo_jupyter will be discontinued soon
leonardo_jupyter="us.gcr.io/broad-dsp-gcr-public/leonardo-jupyter:5c51ce6935da"
welder_server="us.gcr.io/broad-dsp-gcr-public/welder-server:latest"
openidc_proxy="broadinstitute/openidc-proxy:2.3.1_2"

# this array determines which of the above images are baked into the custom image
# the entry must match the var name above, which must correspond to a valid docker URI
docker_image_var_names="welder_server leonardo_jupyter terra_jupyter_base terra_jupyter_python openidc_proxy"

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
    software-properties-common

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
# dataproc custom instance image.
if [[ -n ${docker_image_var_names:?} ]]; then
    for _docker_image_var_name in ${docker_image_var_names:?}
    do
        _docker_image="${!_docker_image_var_name:?}"
        retry 5 docker pull "${_docker_image:?}"
    done
else
    log "ERROR-VAR_NULL_OR_UNSET: docker_image_var_names. Will not pull docker images."
fi

# Install Python 3.7
export PYTHON_VERSION="3.7.4"
python_source_archive_name="Python-${PYTHON_VERSION:?}.tar.xz"
python_source_archive_download_url="https://www.python.org/ftp/python/${PYTHON_VERSION%%[a-z]*}/${python_source_archive_name:?}"
python_target_archive_name="python.tar.xz"

log "Installing Python ${PYTHON_VERSION:?} on the dataproc VM..."
retry 5 wget -O "${python_target_archive_name:?}" "${python_source_archive_download_url:?}"

mkdir -p /usr/src/python
tar -xJC /usr/src/python --strip-components=1 -f "${python_target_archive_name:?}"
rm -v "${python_target_archive_name:?}"
cd /usr/src/python
gnuArch="$(dpkg-architecture --query DEB_BUILD_GNU_TYPE)"
./configure \
  --build="$gnuArch" \
  --enable-loadable-sqlite-extensions \
  --enable-shared \
  --with-system-expat \
  --with-system-ffi \
  --without-ensurepip
make -j "$(nproc)"
make install
ldconfig
python3 --version

log "Finished installing Python $PYTHON_VERSION"
