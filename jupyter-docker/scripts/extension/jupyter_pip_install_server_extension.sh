#!/bin/bash

set -e

if [ -n "$1" ]; then
  JUPYTER_EXTENSION=$1
  pip3 install ${JUPYTER_EXTENSION}
  sudo -E -u jupyter-user jupyter serverextension enable --py ${JUPYTER_EXTENSION}
fi
