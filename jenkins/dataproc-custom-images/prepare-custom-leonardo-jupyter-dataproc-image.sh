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

terra_jupyter_python="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-python:1.1.5"
terra_jupyter_r="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-r:2.2.5"
terra_jupyter_bioconductor="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-bioconductor:2.2.5"
terra_jupyter_hail="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-hail:1.1.10"
terra_jupyter_gatk="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-gatk:2.3.7"
terra_jupyter_aou="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-aou:2.2.12"
welder_server="us.gcr.io/broad-dsp-gcr-public/welder-server:8667bfe"
openidc_proxy="broadinstitute/openidc-proxy:2.3.1_2"
anvil_rstudio_bioconductor="us.gcr.io/broad-dsp-gcr-public/anvil-rstudio-bioconductor:3.19.0"

# Note that this is the version used currently by AOU in production, the one above can be staged for testing
terra_jupyter_aou_old="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-aou:2.2.7"

# If you change this you must also change Leo reference.conf!
cryptomining_detector="us.gcr.io/broad-dsp-gcr-public/cryptomining-detector:0.0.2"

# this array determines which of the above images are baked into the custom image
# the entry must match the var name above, which must correspond to a valid docker URI
docker_image_var_names="welder_server terra_jupyter_python terra_jupyter_r terra_jupyter_bioconductor terra_jupyter_hail terra_jupyter_gatk terra_jupyter_aou terra_jupyter_aou_old openidc_proxy anvil_rstudio_bioconductor cryptomining_detector"

# NOTE - UNCOMMENT TO REGENERATE THE AOU LEGACY DATAPROC IMAGE
# You would also need to change the debian version, see https://github.com/DataBiosphere/leonardo/pull/3871
#docker_image_var_names="welder_server terra_jupyter_python terra_jupyter_r terra_jupyter_bioconductor terra_jupyter_hail terra_jupyter_gatk terra_jupyter_aou_old openidc_proxy anvil_rstudio_bioconductor cryptomining_detector"

# The version of python to install
# Note: this should match the version of python in the terra-jupyter-hail image.
python_version="3.10.9"

bucket_name="gs://leo-dataproc-image"

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

#TODO: Remove this flag once we migrate to debian11
retry 5 apt-get --allow-releaseinfo-change update

# retry 5 betterAptGet
retry 5 apt-get install -y -q \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg2 \
    software-properties-common \
    libffi-dev \
    libsqlite3-dev

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

retry 5 add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"
retry 5 apt-get update

dpkg --configure -a
# This line fails consistently, but it does not fail in a fatal way so we add `|| true` to prevent the script from halting execution
# The message that is non-fatal is `Sub-process /usr/bin/dpkg returned an error code (1).`
# NOTE: If it fails with another legitimate error, this `|| true` could mask it. It was used as a last resort after a lot of attempts to fix.
# apt-get install -y -q docker-ce || true
log 'Installing Docker Compose...'

# Install docker-compose
# https://docs.docker.com/compose/install/#install-compose
docker_compose_version_number="2.28.1"
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

docker info
log 'Cached docker images:'
docker images

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

# Install Python
python_source_archive_name="Python-${python_version:?}.tar.xz"
python_source_archive_download_url="https://www.python.org/ftp/python/${python_version%%[a-z]*}/${python_source_archive_name:?}"
python_target_archive_name="python.tar.xz"

log "Installing Python ${python_version:?} on the dataproc VM..."
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
log "Finished installing Python $python_version"
