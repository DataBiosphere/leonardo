#!/bin/bash

set -e

if [ -n "$1" ]; then
  JUPYTER_EXTENSION=$1
  pip install ${JUPYTER_EXTENSION}
  sudo -u jupyter-user jupyter serverextension enable --py ${JUPYTER_EXTENSION}
fi
