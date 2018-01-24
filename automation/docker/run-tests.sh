#!/bin/bash

# Parameters
NUM_NODES="${1:-4}"  # default to 4
ENV="${2:-dev}"  # default to dev
export ENV=$ENV
HUB_COMPOSE=hub-compose-fiab.yml

#if test runs against a remote FIAB on a GCE node, put IP in param 3
#if test runs against a local FIAB on a Docker, put "local" in param 3
#else leave blank (test runs against a non FIAB host, such as real dev)
DOCKERHOST="127.0.0.1"
DOCKERHOST=${3:-$DOCKERHOST}
if [ $DOCKERHOST = "alpha" -o $DOCKERHOST = "prod" ];
  then
    DOCKERHOST=
    HUB_COMPOSE=hub-compose.yml
fi
export DOCKERHOST=$DOCKERHOST
TEST_CONTAINER=${4:-automation}
VAULT_TOKEN=$5
WORKING_DIR=${6:-$PWD}
export WORKING_DIR=$WORKING_DIR

if [ -z $VAULT_TOKEN ]; then
    VAULT_TOKEN=$(cat ~/.vault-token)
fi

if [ "$DOCKERHOST" = "local" ]
  then
    docker pull chickenmaru/nettools
    export DOCKERHOST=`docker run --net=docker_default -it --rm chickenmaru/nettools sh -c "ip route|grep default|cut -d' ' -f 3"`
    echo "Docker Host from Docker Perspective: $DOCKERHOST"
fi

# define some colors to use for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

# start up
startup() {
    containers=$(docker ps | grep python_chrome_*)
    if [[ -z $containers ]]; then
        # cleanup old containers
        docker rm -v $(docker ps -a -q -f status=exited)
    else
        echo "Tests are already running on this host.  Kill current tests or try again later."
        exit 1
    fi
    rm -rf $WORKING_DIR/target/ $WORKING_DIR/failure_screenshots/
}
# kill and remove any running containers
cleanup () {
  docker-compose -f ${HUB_COMPOSE} stop
  docker stop $TEST_CONTAINER
}

# cleanup old containers
docker rm -v $(docker ps -a -q -f status=exited)

# catch unexpected failures, do cleanup and output an error message
trap 'cleanup ; printf "${RED}Tests Failed For Unexpected Reasons${NC}\n"'\
  HUP INT QUIT PIPE TERM

# make sure ${WORKING_DIR}/target exists before mapping it into a docker container
mkdir $WORKING_DIR/target

# build and run the composed services
echo "HOST IP: $DOCKERHOST"
docker-compose -f ${HUB_COMPOSE} pull
docker-compose -f ${HUB_COMPOSE} up -d
docker-compose -f ${HUB_COMPOSE} scale chrome=$NUM_NODES

# Make sure ${WORKING_DIR}/chrome/downloads exists and make it writable by the node-chrome containers.
mkdir -p $WORKING_DIR/chrome/downloads
# Without this, the directory permissions don't allow chrome to automatically save downloads which
# leads to a system save dialog opening which Selenium doesn't have any way of handling.
echo '--- Begin ugly but sometimes necessary python stack trace and error "ValueError" that looks bad but actually does something useful ---'
docker-compose -f ${HUB_COMPOSE} exec chrome sudo chmod 777 /app/chrome/downloads
echo '--- End ugly but necessary python error ---'

# render ctmpls
docker pull broadinstitute/dsde-toolbox:dev
docker run --rm -e VAULT_TOKEN=${VAULT_TOKEN} \
    -e ENVIRONMENT=${ENV} -e ROOT_DIR=/app -v ${WORKING_DIR}:/working \
    -e OUT_PATH=/working/target -e INPUT_PATH=/working -e LOCAL_UI=false \
    broadinstitute/dsde-toolbox:dev render-templates.sh

if [ "$DOCKERHOST" != "" ]; then
    HOST_MAPPING="--add-host=firecloud-fiab.dsde-${ENV}.broadinstitute.org:${DOCKERHOST} --add-host=firecloud-orchestration-fiab.dsde-${ENV}.broadinstitute.org:${DOCKERHOST} --add-host=rawls-fiab.dsde-${ENV}.broadinstitute.org:${DOCKERHOST} --add-host=thurloe-fiab.dsde-${ENV}.broadinstitute.org:${DOCKERHOST} --add-host=sam-fiab.dsde-${ENV}.broadinstitute.org:${DOCKERHOST} --add-host=leonardo-fiab.dsde-${ENV}.broadinstitute.org:${DOCKERHOST} -e SLACK_API_TOKEN=$SLACK_API_TOKEN -e BUILD_NUMBER=$BUILD_NUMBER -e SLACK_CHANNEL=${SLACK_CHANNEL}"
fi

TEST_ENTRYPOINT="test"
if [ $ENV = "prod" ]; then
    TEST_ENTRYPOINT="test"
fi
echo $TEST_ENTRYPOINT

# run tests
docker run -e DOCKERHOST=$DOCKERHOST \
    --net=docker_default \
    -e ENV=$ENV \
    -P --rm -t -e CHROME_URL="http://hub:4444/" ${HOST_MAPPING} \
    -v $WORKING_DIR/target/application.conf:/app/src/test/resources/application.conf \
    -v $WORKING_DIR/target/firecloud-account.pem:/app/src/test/resources/firecloud-account.pem \
    -v $WORKING_DIR/target:/app/target \
    -v $WORKING_DIR/chrome/downloads:/app/chrome/downloads \
    -v $WORKING_DIR/failure_screenshots:/app/failure_screenshots \
    -v $WORKING_DIR/output:/app/output \
    -v jar-cache:/root/.ivy -v jar-cache:/root/.ivy2 \
    --link docker_hub_1:hub --name ${TEST_CONTAINER} -w /app \
    ${TEST_CONTAINER}:latest "${TEST_ENTRYPOINT}"


# Grab exit code of tests
TEST_EXIT_CODE=$?

if [ $? -ne 0 ] ; then
  printf "${RED}Docker Compose Failed${NC}\n"
  exit -1
fi

# inspect the output of the test and display respective message
if [ -z ${TEST_EXIT_CODE+x} ] || [ "$TEST_EXIT_CODE" -ne 0 ] ; then
  printf "${RED}Tests Failed${NC} - Exit Code: $TEST_EXIT_CODE\n"
else
  printf "${GREEN}Tests Passed${NC}\n"
fi

# call the cleanup fuction
cleanup

# exit the script with the same code as the test service code
exit $TEST_EXIT_CODE
