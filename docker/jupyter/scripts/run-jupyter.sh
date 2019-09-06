#!/usr/bin/env bash

set -e

# Change the default umask to support R/W access to the shared volume with welder
umask 002

LOG_DIR=${NOTEBOOKS_DIR:-${HOME}}

# Forces python 3
# Note: relies on NOTEBOOKS_DIR environment variable
JUPYTER_BASE="/usr/local/bin/python3 /usr/local/bin/jupyter-notebook"
JUPYTER_CMD="$JUPYTER_BASE --NotebookApp.nbserver_extensions=\"{'jupyter_localize_extension':True}\" &> ${LOG_DIR}/jupyter.log"

echo $JUPYTER_CMD

eval $JUPYTER_CMD
