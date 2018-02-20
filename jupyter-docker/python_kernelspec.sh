#!/usr/bin/env bash

echo "In python_kernelspec.sh"
if [ -n "$1" ]; then
  KERNELSPEC_HOME=$1

    echo "Got KERNELSPEC_HOMEL: $KERNELSPEC_HOME"
  # Remove original kernel script
  rm /usr/local/share/jupyter/kernels/python2/kernel.json
  echo "remove python2"
  rm /usr/local/share/jupyter/kernels/python3/kernel.json
  echo "remove python3"

  # add new kernel script
  cp python2_kernelspec.json ${KERNELSPEC_HOME}/python2/kernel.json
  echo "cp python2"
  cp python3_kernelspec.json ${KERNELSPEC_HOME}/python3/kernel.json
  echo "cp python3"
fi

