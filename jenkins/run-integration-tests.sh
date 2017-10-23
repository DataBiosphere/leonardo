#!/usr/bin/env bash

if [ -z "${ENV}" ]; then
    echo "FATAL ERROR: ENV undefined."
    exit 2
fi

VAULT_TOKEN=$(cat /etc/vault-token-dsde)

pushd ../automation

./render-local-env.sh $PWD $VAULT_TOKEN $ENV

sbt -Djsse.enableSNIExtension=false -Dheadless=false test
TEST_EXIT_CODE=$?

popd

exit $TEST_EXIT_CODE
