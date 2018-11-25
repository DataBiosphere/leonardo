#!/usr/bin/env bash

set -e

# Forces python 2
JUPYTER_BASE="/usr/bin/python3 /usr/local/bin/jupyter-notebook"
JUPYTER_CMD="$JUPYTER_BASE --NotebookApp.nbserver_extensions=\"{'jupyter_localize_extension':True}\" &> ${HOME}/jupyter.log"

echo $JUPYTER_CMD

eval $JUPYTER_CMD
