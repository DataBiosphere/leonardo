#!/bin/bash

set -e

if [ -n "$1" ]; then
  JUPYTER_EXTENSION=$1
  JUPYTER_EXTENSION_NAME=`basename ${JUPYTER_EXTENSION%%.*}`
  tar -zxvf ${JUPYTER_EXTENSION} -C ${JUPYTER_HOME}
  jupyter labextension install ${JUPYTER_HOME}/${JUPYTER_EXTENSION_NAME}
fi
