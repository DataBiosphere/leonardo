#!/bin/bash

set -e

if [ -n "$1" ]; then
  JUPYTER_EXTENSION=$1
  JUPYTER_EXTENSION_NAME=`basename ${JUPYTER_EXTENSION%%.*}`
  mkdir ${JUPYTER_HOME}/${JUPYTER_EXTENSION_NAME}
  tar -xzf ${JUPYTER_EXTENSION} -C${JUPYTER_HOME}/${JUPYTER_EXTENSION_NAME}
  pip install -e ${JUPYTER_HOME}/${JUPYTER_EXTENSION_NAME}
  sudo -u jupyter-user jupyter serverextension enable --py ${JUPYTER_EXTENSION_NAME}
fi
