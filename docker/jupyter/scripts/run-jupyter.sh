#!/usr/bin/env bash

set -e

# Change the default umask to support R/W access to the shared volume with welder
umask 002

NOTEBOOKS_DIR=$1

# Forces python 3
JUPYTER_BASE="/usr/local/bin/python3 /usr/local/bin/jupyter-notebook"
JUPYTER_CMD="$JUPYTER_BASE --NotebookApp.nbserver_extensions=\"{'jupyter_localize_extension':True}\" &> ${NOTEBOOKS_DIR}/jupyter.log"

echo $JUPYTER_CMD

eval $JUPYTER_CMD
