#!/bin/bash

set -e

 if [ -n "$1" ]; then
  JUPYTER_EXTENSION=$1
  jupyter labextension install ${JUPYTER_EXTENSION}
fi
