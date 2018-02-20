#!/usr/bin/env bash

if [ -n "$1" ]; then
  KERNELSPEC_HOME=$1

  # Remove original kernel script
  rm /usr/local/share/jupyter/kernels/python2/kernel.json
  rm /usr/local/share/jupyter/kernels/python3/kernel.json

  # add new kernel script
  cp python2_kernelspec.json ${KERNELSPEC_HOME}/python2/kernel.json
  cp python3_kernelspec.json ${KERNELSPEC_HOME}/python3/kernel.json
fi

