#!/usr/bin/env bash

set -e -x

##
# This is a startup script designed to run on Leo-created Dataproc clusters.
#
# It starts up Jupyter and Welder processes. It also optionally deploys welder on a
# cluster if not already installed.
##

# Detaults. Assumes welder is enabled and does not need to be deployed.
welder_enabled=true
deploy_welder=false

while [ "$1" != "" ]; do
    case $1 in
        --project)
            shift
            google_project=$1
            ;;
        --cluster-name)
            shift
            cluster_name=$1
            ;;
        --owner-email)
            shift
            owner_email=$1
            ;;
        --enable-welder)
            welder_enabled=true
            ;;
        --deploy-welder)
            deploy_welder=true
            welder_enabled=true
            ;;
        --welder-docker-image)
            shift
            welder_docker_image=$1
            ;;
    esac
    shift
done

# Sanity check args
if [ -z "$google_project" ] ; then
    echo "Required: --project"
    exit 1
fi
if [ -z "$cluster_name" ] ; then
    echo "Required: --cluster_name"
    exit 1
fi
if [ -z "$owner_email" ] ; then
    echo "Required: --owner-email"
    exit 1
fi
if [ "$deploy_welder" = true ] && [ -z "$welder_docker_image" ] ; then
    echo "Required: --welder-docker-image"
    exit 1
fi

# Set notebooks_dir appropriately depending on whether welder is enabled
if [ "$welder_enabled" = true ] ; then
    notebooks_dir=/home/jupyter-user/notebooks
else
    notebooks_dir=/home/jupyter-user
fi

if [ "$deploy_welder" = true ] ; then
    echo "Deploying Welder on cluster $google_project / $cluster_name..."

    # Set EVs needed for welder-docker-compose
    export WELDER_SERVER_NAME=welder-server
    export WELDER_DOCKER_IMAGE=$welder_docker_image
    export GOOGLE_PROJECT=$google_project
    export CLUSTER_NAME=$cluster_name
    export OWNER_EMAIL=$owner_email

    # Run welder-docker-compose
    gcloud auth configure-docker
    docker-compose -f /etc/welder-docker-compose.yaml up -d

    # Set EVs inside Jupyter container necessary for welder
    docker exec -it jupyter-server bash -c "echo $'export WELDER_ENABLED=true\nexport NOTEBOOKS_DIR=$notebooks_dir' >> /home/jupyter-user/.bashrc"

    # Move existing notebooks to new notebooks dir
    docker exec -it jupyter-server bash -c "ls -I jupyter.log -I localization.log -I notebooks /home/jupyter-user | xargs -d '\n'  -I file cp -R file $notebooks_dir"
fi

# Start Jupyter
echo "Starting Jupyter on cluster $google_project / $cluster_name..."
docker exec -d jupyter-server /bin/bash -c "/etc/jupyter/scripts/run-jupyter.sh $notebooks_dir || /usr/local/bin/jupyter notebook"

# Start welder, if enabled
if [ "$welder_enabled" = true ] ; then
    echo "Starting Welder on cluster $google_project / $cluster_name..."
    docker exec -d welder-server /opt/docker/bin/entrypoint.sh
fi