#!/bin/bash
# Sourced from https://github.com/galaxyproject/galaxy-k8s-boot/blob/anvil/bin/user_data.sh
# When updating this file, sync it manually from that repository and verify the changes.
# Passed to the GCE instance as the "startup-script" metadata key so the Guest Agent
# executes it on every boot (cloud-init user-data is skipped on pre-baked images).

echo "[$(date)] - Starting galaxy_bootstrap script..."

# 1. Setup persistent disk if available
DISK_DEVICE="/dev/disk/by-id/google-galaxy-data"
if [ -b "$DISK_DEVICE" ]; then
  echo "[$(date)] - Found persistent disk at $DISK_DEVICE"

  if ! blkid "$DISK_DEVICE" > /dev/null 2>&1; then
    echo "[$(date)] - Formatting disk $DISK_DEVICE with ext4"
    mkfs -t ext4 "$DISK_DEVICE"
  else
    echo "[$(date)] - Disk $DISK_DEVICE is already formatted"
  fi

  mkdir -p /mnt/block_storage
  mount "$DISK_DEVICE" /mnt/block_storage

  DISK_UUID=$(blkid -s UUID -o value "$DISK_DEVICE")
  if [ -n "$DISK_UUID" ] && ! grep -q "$DISK_UUID" /etc/fstab; then
    echo "UUID=$DISK_UUID /mnt/block_storage ext4 defaults 0 2" >> /etc/fstab
  fi

  chown debian:debian /mnt/block_storage
  echo "[$(date)] - Persistent disk mounted at /mnt/block_storage"
else
  echo "[$(date)] - No persistent disk found at $DISK_DEVICE. Galaxy will use ephemeral storage."
fi

# 2. Setup PostgreSQL disk if available
POSTGRES_DISK_DEVICE="/dev/disk/by-id/google-galaxy-postgres-data"
if [ -b "$POSTGRES_DISK_DEVICE" ]; then
  echo "[$(date)] - Found PostgreSQL disk at $POSTGRES_DISK_DEVICE"

  if ! blkid "$POSTGRES_DISK_DEVICE" > /dev/null 2>&1; then
    echo "[$(date)] - Formatting PostgreSQL disk $POSTGRES_DISK_DEVICE with ext4"
    mkfs -t ext4 "$POSTGRES_DISK_DEVICE"
  else
    echo "[$(date)] - PostgreSQL disk $POSTGRES_DISK_DEVICE is already formatted"
  fi

  mkdir -p /mnt/postgres_storage
  mount "$POSTGRES_DISK_DEVICE" /mnt/postgres_storage

  POSTGRES_DISK_UUID=$(blkid -s UUID -o value "$POSTGRES_DISK_DEVICE")
  if [ -n "$POSTGRES_DISK_UUID" ] && ! grep -q "$POSTGRES_DISK_UUID" /etc/fstab; then
    echo "UUID=$POSTGRES_DISK_UUID /mnt/postgres_storage ext4 defaults 0 2" >> /etc/fstab
  fi

  chown debian:debian /mnt/postgres_storage
  echo "[$(date)] - PostgreSQL disk mounted at /mnt/postgres_storage"
else
  echo "[$(date)] - No PostgreSQL disk found at $POSTGRES_DISK_DEVICE. PostgreSQL will use ephemeral storage."
fi

# 3. Run ansible-pull as the debian user.
# Single-quoted heredoc delimiter means the outer (root) shell does not expand any
# variables — all expansion happens inside the debian bash session.
sudo -u debian bash <<'DEBIAN_EOF'
export HOME=/home/debian

HOST_IP=$(curl -s -f "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip" \
  -H "Metadata-Flavor: Google" 2>/dev/null || curl -s ifconfig.me)

PV_SIZE=$(curl -s -f "http://metadata.google.internal/computeMetadata/v1/instance/attributes/persistent-volume-size" \
  -H "Metadata-Flavor: Google" 2>/dev/null)
if [ -z "$PV_SIZE" ]; then
    echo "[$(date)] - persistent-volume-size metadata not found or empty, using default."
    PV_SIZE="139Gi"
fi
echo "[$(date)] - NFS storage size for Galaxy: ${PV_SIZE}"

RESTORE_GALAXY=$(curl -s -f "http://metadata.google.internal/computeMetadata/v1/instance/attributes/restore_galaxy" \
  -H "Metadata-Flavor: Google" 2>/dev/null || echo "false")

GCP_BATCH_SERVICE_ACCOUNT_EMAIL=$(curl -s -f "http://metadata.google.internal/computeMetadata/v1/instance/attributes/gcp_batch_service_account_email" \
  -H "Metadata-Flavor: Google" 2>/dev/null || echo "galaxy-batch-runner@anvil-and-terra-development.iam.gserviceaccount.com")
echo "[$(date)] - GCP Batch service account email: ${GCP_BATCH_SERVICE_ACCOUNT_EMAIL}"

TERRA_WORKSPACE=$(curl -s -f "http://metadata.google.internal/computeMetadata/v1/instance/attributes/terra-workspace" \
  -H "Metadata-Flavor: Google" 2>/dev/null || echo "")
TERRA_NAMESPACE=$(curl -s -f "http://metadata.google.internal/computeMetadata/v1/instance/attributes/terra-namespace" \
  -H "Metadata-Flavor: Google" 2>/dev/null || echo "")
TERRA_DRS_URL=$(curl -s -f "http://metadata.google.internal/computeMetadata/v1/instance/attributes/terra-drs-url" \
  -H "Metadata-Flavor: Google" 2>/dev/null || echo "")
TERRA_API_URL=$(curl -s -f "http://metadata.google.internal/computeMetadata/v1/instance/attributes/terra-api-url" \
  -H "Metadata-Flavor: Google" 2>/dev/null || echo "")

# Leo proxy path prefix (e.g. /proxy/google/v1/apps/{project}/{appName}/galaxy).
# Passed to ansible-pull as galaxy_prefix so Galaxy nginx ingress is configured at
# the correct subpath. Without this Galaxy generates links rooted at / which the
# browser resolves against Leo host and gets 404s.
GALAXY_URL_PREFIX=$(curl -s -f "http://metadata.google.internal/computeMetadata/v1/instance/attributes/galaxy-url-prefix" \
  -H "Metadata-Flavor: Google" 2>/dev/null || echo "")
echo "[$(date)] - Galaxy URL prefix: ${GALAXY_URL_PREFIX}"

GALAXY_USER=$(curl -s -f "http://metadata.google.internal/computeMetadata/v1/instance/attributes/galaxy-user-email" \
  -H "Metadata-Flavor: Google" 2>/dev/null || echo "")
echo "[$(date)] - Galaxy user email: ${GALAXY_USER}"

GIT_REPO=$(curl -s -f "http://metadata.google.internal/computeMetadata/v1/instance/attributes/git-repo" \
  -H "Metadata-Flavor: Google" 2>/dev/null || echo "https://github.com/galaxyproject/galaxy-k8s-boot.git")
GIT_BRANCH=$(curl -s -f "http://metadata.google.internal/computeMetadata/v1/instance/attributes/git-branch" \
  -H "Metadata-Flavor: Google" 2>/dev/null || echo "anvil")

PULL_ARGS=(
  -U "${GIT_REPO}"
  -C "${GIT_BRANCH}"
  -d /home/debian/ansible
  -i /tmp/ansible-inventory/localhost
  --accept-host-key
  --limit 127.0.0.1
  --extra-vars "gcp_batch_service_account_email=${GCP_BATCH_SERVICE_ACCOUNT_EMAIL}"
  --extra-vars "terra_workspace=${TERRA_WORKSPACE}"
  --extra-vars "terra_namespace=${TERRA_NAMESPACE}"
  --extra-vars "terra_drs_url=${TERRA_DRS_URL}"
  --extra-vars "terra_api_url=${TERRA_API_URL}"
)

if [ "$RESTORE_GALAXY" = "true" ]; then
    PULL_ARGS+=(--extra-vars "restore_galaxy=true")
    echo "[$(date)] - Galaxy Restore Mode: Enabled"
else
    echo "[$(date)] - Galaxy Restore Mode: Disabled"
fi

if [ -n "$GALAXY_URL_PREFIX" ]; then
    PULL_ARGS+=(--extra-vars "galaxy_prefix=${GALAXY_URL_PREFIX}")
    echo "[$(date)] - Galaxy URL prefix passed to ansible: ${GALAXY_URL_PREFIX}"
fi

PULL_ARGS+=(playbook.yml)

mkdir -p /tmp/ansible-inventory
cat > /tmp/ansible-inventory/localhost << EOF
[vms]
127.0.0.1 ansible_connection=local ansible_python_interpreter="/usr/bin/python3"

[all:vars]
ansible_user="debian"
rke2_token="defaultSecret12345"
rke2_additional_sans=["${HOST_IP}"]
rke2_debug=true
nfs_size="${PV_SIZE}"
galaxy_persistence_size="${PV_SIZE}"
galaxy_db_password="gxy-db-password"
galaxy_user="${GALAXY_USER}"
EOF

echo "[$(date)] - Inventory file created at /tmp/ansible-inventory/localhost; running ansible-pull..."
echo "[$(date)] - Running: ANSIBLE_CALLBACKS_ENABLED=profile_tasks ANSIBLE_HOST_PATTERN_MISMATCH=ignore ansible-pull ${PULL_ARGS[@]}"

ANSIBLE_CALLBACKS_ENABLED=profile_tasks ANSIBLE_HOST_PATTERN_MISMATCH=ignore ansible-pull "${PULL_ARGS[@]}"
DEBIAN_EOF

echo "[$(date)] - Bootstrap script completed."
