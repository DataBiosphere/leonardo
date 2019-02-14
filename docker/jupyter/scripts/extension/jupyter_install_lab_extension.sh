#!/bin/bash

set -e


if [ -n "$1" ]; then
  JUPYTER_EXTENSION=$1
  if [[ ${JUPYTER_EXTENSION} == *'.js' ]]; then
    # use jupyterlab extension template to create an extension using the specified JS file
    # see https://github.com/jupyterlab/extension-cookiecutter-js
    JUPYTER_EXTENSION_NAME=`basename ${JUPYTER_EXTENSION%%.*}`
    cookiecutter --no-input https://github.com/jupyterlab/extension-cookiecutter-js extension_name=${JUPYTER_EXTENSION_NAME}
    cp -f ${JUPYTER_EXTENSION} ${JUPYTER_EXTENSION_NAME}/lib/plugin.js
    cd ${JUPYTER_EXTENSION_NAME}
    jlpm
    jupyter labextension install .
  elif [[ ${JUPYTER_EXTENSION} == *'.ts' ]]; then
    # same as above but in typescript, see https://github.com/jupyterlab/extension-cookiecutter-ts
    JUPYTER_EXTENSION_NAME=`basename ${JUPYTER_EXTENSION%%.*}`
    cookiecutter --no-input https://github.com/jupyterlab/extension-cookiecutter-ts extension_name=${JUPYTER_EXTENSION_NAME}
    cp -f ${JUPYTER_EXTENSION} ${JUPYTER_EXTENSION_NAME}/src/index.ts
    cd ${JUPYTER_EXTENSION_NAME}
    jlpm
    jupyter labextension install .
  else
    jupyter labextension install $JUPYTER_EXTENSION
  fi
fi

