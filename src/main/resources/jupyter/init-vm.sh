#!/usr/bin/env bash

set -e -x

#
# This init script installs VM prerequisites on a Leonardo Dataproc cluster.
# It is only used if we do not specify a custom Dataproc image.
# See: https://cloud.google.com/dataproc/docs/guides/dataproc-images
#

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

function betterAptGet() {
  if ! { apt-get update 2>&1 || echo E: update failed; } | grep -q '^[WE]:'; then
    return 0
  else
    return 1
  fi
}

#
# Main
#

#
# Array for instrumentation
# UPDATE THIS IF YOU ADD MORE STEPS:
# currently the steps are:
# START init,
# .. after prerequisites
# .. after docker install
# .. after docker-compose install
# .. after python install
# END
STEP_TIMINGS=($(date +%s))

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)

# Install docker, docker-compose on the master node.
if [[ "${ROLE}" == 'Master' ]]; then
    log 'Installing prerequisites...'

    # Obtain the latest valid apt-key.gpg key file from https://packages.cloud.google.com to work
    # around intermittent apt authentication errors. See:
    # https://cloud.google.com/compute/docs/troubleshooting/known-issues
    retry 5 curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
    retry 5 apt-key update

    # install Docker
    # https://docs.docker.com/install/linux/docker-ce/debian/
    export DOCKER_CE_VERSION="18.06.2~ce~3-0~debian"

    retry 5 betterAptGet
    retry 5 apt-get install -y -q \
        apt-transport-https \
        ca-certificates \
        curl \
        gnupg2 \
        software-properties-common

    STEP_TIMINGS=($(date +%s))

    log 'Adding Docker package sources...'

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

    retry 5 betterAptGet
    retry 5 apt-get install -y -q docker-ce="${DOCKER_CE_VERSION:?}"

    STEP_TIMINGS=($(date +%s))

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

    STEP_TIMINGS=($(date +%s))
fi

# Install Python 3.6 on the master and worker VMs
export PYTHON_VERSION=3.6.8
log "Installing Python $PYTHON_VERSION on the VM..."
retry 5 wget -O python.tar.xz "https://www.python.org/ftp/python/${PYTHON_VERSION%%[a-z]*}/Python-$PYTHON_VERSION.tar.xz"
mkdir -p /usr/src/python
tar -xJC /usr/src/python --strip-components=1 -f python.tar.xz
rm python.tar.xz
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

STEP_TIMINGS=($(date +%s))

log "Timings: ${STEP_TIMINGS[@]}"
