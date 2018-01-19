#!/bin/bash

if [ -n "$1" ]; then
  JUPYTER_EXTENSION_ARCHIVE=$1
  JUPYTER_EXTENSION_NAME=`basename ${JUPYTER_EXTENSION_ARCHIVE%%.*}`
  mkdir ${JUPYTER_HOME}/${JUPYTER_EXTENSION_NAME}
  tar -xzf ${JUPYTER_EXTENSION_ARCHIVE} -C${JUPYTER_HOME}/${JUPYTER_EXTENSION_NAME}
  jupyter nbextension install ${JUPYTER_HOME}/${JUPYTER_EXTENSION_NAME}/ --user
  jupyter nbextension enable ${JUPYTER_EXTENSION_NAME}/main
fi 
