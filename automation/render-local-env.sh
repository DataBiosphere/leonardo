#!/usr/bin/env bash
set -e

## Run from automation/
## Clones the firecloud-automated-testing repo, pulls templatized configs, and renders them to src/test/resources

# Defaults
WORKING_DIR=$PWD
VAULT_TOKEN=$(cat ~/.vault-token)
FIRECLOUD_AUTOMATED_TESTING_BRANCH=master
ENV=dev
SERVICE=leonardo
LOCAL_UI=${LOCAL_UI:-false}  # local ui defaults to false unless set in the env

FC_INSTANCE=fiab
if [ $LOCAL_UI = "true" ]; then
    FC_INSTANCE=local
fi

# Parameters
FIRECLOUD_AUTOMATED_TESTING_BRANCH=${1:-$FIRECLOUD_AUTOMATED_TESTING_BRANCH}
VAULT_TOKEN=${2:-$VAULT_TOKEN}
ENV=${3:-$ENV}
SERVICE_ROOT=${4:-$SERVICE}

SCRIPT_ROOT=${SERVICE_ROOT}/automation

confirm () {
    # call with a prompt string or use a default
    read -r -p "${1:-Are you sure?} [y/N] " response
    case $response in
        [yY])
            shift
            $@
            ;;
        *)
            ;;
    esac
}

# clone the firecloud-automated-testing repo
clone_repo() {
    original_dir=$PWD
    cd ../..
    echo "Currently in ${PWD}"
    confirm "OK to clone here?" git clone git@github.com:broadinstitute/firecloud-automated-testing.git
    cd $original_dir
}

pull_configs() {
    original_dir=$WORKING_DIR
    cd ../..
    cd firecloud-automated-testing
    echo "Currently in ${PWD}"
    git stash
    git checkout ${FIRECLOUD_AUTOMATED_TESTING_BRANCH}
    git pull
    cd $original_dir
}

render_configs() {
    original_dir=$WORKING_DIR
    cd ../..
    docker pull broadinstitute/dsde-toolbox:dev
    docker run -it --rm -e VAULT_TOKEN=${VAULT_TOKEN} \
        -e ENVIRONMENT=${ENV} -e ROOT_DIR=${WORKING_DIR} -v $PWD/firecloud-automated-testing/configs:/input -v $PWD/$SCRIPT_ROOT:/output \
        -e OUT_PATH=/output/src/test/resources -e INPUT_PATH=/input -e LOCAL_UI=$LOCAL_UI -e FC_INSTANCE=$FC_INSTANCE \
        broadinstitute/dsde-toolbox:dev render-templates.sh

    # pull service-specific application.conf
    docker run -it --rm -e VAULT_TOKEN=${VAULT_TOKEN} \
        -e ENVIRONMENT=${ENV} -e ROOT_DIR=${WORKING_DIR} -v $PWD/firecloud-automated-testing/configs/$SERVICE:/input -v $PWD/$SCRIPT_ROOT:/output \
        -e OUT_PATH=/output/src/test/resources -e INPUT_PATH=/input -e LOCAL_UI=$LOCAL_UI -e FC_INSTANCE=$FC_INSTANCE \
        broadinstitute/dsde-toolbox:dev render-templates.sh
    cd $original_dir
}

if [[ $PWD != *"${SCRIPT_ROOT}" ]]; then
    echo "Error: this script needs to be running from the ${SCRIPT_ROOT} directory!"
    exit 1
fi
confirm "Clone firecloud-automated-testing repo?  Skip if you have already run this step before." clone_repo
confirm "Checkout ${FIRECLOUD_AUTOMATED_TESTING_BRANCH} in firecloud-automated-testing? This will stash local changes.  If N, configs will be built from local changes." pull_configs
render_configs
