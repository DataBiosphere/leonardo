#!/usr/bin/env bash

build() {
    echo "building jupyter docker image..."
    docker build -t broadinstitute/leonardo-notebooks:$JUPYTER_TAG .
}

push() {
    echo "pushing jupyter docker image..."
    docker push broadinstitute/leonardo-notebooks:$JUPYTER_TAG
}

JUPYTER_COMMAND=$1

JUPYTER_TAG=$2

if [ $JUPYTER_COMMAND = "build" ]; then
    build
elif [ $JUPYTER_COMMAND = "push" ]; then
    push
else
    exit 1
fi