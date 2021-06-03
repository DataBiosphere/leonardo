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

# The version of ansible to install
ansible_version="2.7.0.0"

#
# Constants and Global Vars
# the image tags are set via jenkins automation
#

# The versions below don't matter; they are replaced by the Jenkins job
terra_jupyter_base="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-base:0.0.19"
terra_jupyter_python="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-python:0.1.1"
terra_jupyter_r="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-r:1.0.13"
terra_jupyter_bioconductor="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-bioconductor:1.0.13"
terra_jupyter_gatk="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-gatk:1.1.1"
terra_jupyter_aou_old="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-aou:1.1.2"
terra_jupyter_aou="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-aou:1.1.5"

welder_server="us.gcr.io/broad-dsp-gcr-public/welder-server:6cfad83"
openidc_proxy="broadinstitute/openidc-proxy:2.3.1_2"
anvil_rstudio_bioconductor="us.gcr.io/anvil-gcr-public/anvil-rstudio-bioconductor:3.13.0"

# Not replaced by Jenkins. If you change this you must also change Leo reference.conf!
cryptomining_detector="us.gcr.io/broad-dsp-gcr-public/cryptomining-detector:0.0.1"

# This array determines which of the above images are baked into the custom image
# the entry must match the var name above, which must correspond to a valid docker URI
docker_image_var_names="welder_server terra_jupyter_base terra_jupyter_python terra_jupyter_r terra_jupyter_bioconductor terra_jupyter_gatk terra_jupyter_aou terra_jupyter_aou_old openidc_proxy anvil_rstudio_bioconductor cryptomining_detector"

# Variables for downloading the Ansible playbook files for hardening
cis_hardening_dir="cis-harden-images/debian9"
cis_hardening_playbook_requirements_file="${cis_hardening_dir}/requirements.yml"
cis_hardening_playbook_config_file="${cis_hardening_dir}/deb9-cis-playbook.yml"
image_hardening_script_file_name="harden-images.sh"
image_hardening_script_file="${cis_hardening_dir}/${image_hardening_script_file_name}"
daisy_sources_metadata_url="http://metadata.google.internal/computeMetadata/v1/instance/attributes/daisy-sources-path"
vm_metadata_google_header="Metadata-Flavor: Google"

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

retry 5 apt-get update

retry 5 apt-get install -y -q \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg2 \
    software-properties-common \
    libffi-dev \
    python3-pip

# Install google-fluent-d
# https://cloud.google.com/logging/docs/agent/installation
curl -sSO https://dl.google.com/cloudagents/install-logging-agent.sh
bash install-logging-agent.sh

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

retry 5 apt-get update

# Do some set-up in preparation for image hardening

# Install dpkg-dev so we can use dpkg-architecture down below
apt-get install -y -q dpkg-dev

python_version=$(python3 --version)
log "Using $python_version packaged in the base (Debian 9) image..."

log "Installing python requests module..."
pip3 install requests

# For some reason existing gcloud logs are world writable, which causes the image hardening
# script to fail. This makes them writable only by owner.
chmod -R 644 /root/.config/gcloud/logs

log "Downloading Ansible playbook files and the image hardening script..."
daisy_sources_path=$(curl --silent -H "$vm_metadata_google_header" "$daisy_sources_metadata_url")
gsutil cp "${daisy_sources_path}/${cis_hardening_playbook_requirements_file}" .
gsutil cp "${daisy_sources_path}/${cis_hardening_playbook_config_file}" .
gsutil cp "${daisy_sources_path}/${image_hardening_script_file}" .

# Run CIS hardening
chmod u+x $image_hardening_script_file_name
./$image_hardening_script_file_name

log 'Installing Docker...'

retry 5 add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"
retry 5 apt-get update

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

# Shut down the instance after it is done, which is used by the Daisy workflow's wait-for-inst-install
# step to determine when provisioning is done.
shutdown -h now
