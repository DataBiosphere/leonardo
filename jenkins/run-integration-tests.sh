#!/bin/bash

if [ -z "${HOST_IP}" ]; then
    echo "FATAL ERROR: HOST_IP undefined."
    exit 1
fi

if [ -z "${ENV}" ]; then
    echo "FATAL ERROR: ENV undefined."
    exit 2
fi

SCALATESTS=automation-leo
VAULT_TOKEN=$(cat /etc/vault-token-dsde)

# build test docker image
cd ../automation
docker build -f Dockerfile-tests -t $SCALATESTS .

# run tests
cd docker
./run-tests.sh 2 $ENV $HOST_IP $SCALATESTS $VAULT_TOKEN
TEST_EXIT_CODE=$?

# do some cleanup after
#sudo chmod -R 777 logs

# exit with exit code of test script
exit $TEST_EXIT_CODE
