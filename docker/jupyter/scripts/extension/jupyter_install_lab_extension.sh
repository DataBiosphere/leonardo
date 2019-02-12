#!/bin/bash

set -e


if [ -n "$1" ]; then
  JUPYTER_EXTENSION=$1
  if [[ ${JUPYTER_EXTENSION} == *'.js' ]]; then
    JUPYTER_EXTENSION_NAME=`basename ${JUPYTER_EXTENSION%%.*}`
    cookiecutter --no-input https://github.com/jupyterlab/extension-cookiecutter-ts extension_name=${JUPYTER_EXTENSION_NAME}
    cp ${JUPYTER_EXTENSION} ${JUPYTER_EXTENSION_NAME}/lib/plugin.js
    cd ${JUPYTER_EXTENSION_NAME}
    jlpm
    jupyter labextension install . --no-build
  else
    jupyter labextension install $JUPYTER_EXTENSION
  fi
fi

