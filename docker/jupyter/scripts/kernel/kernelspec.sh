#!/usr/bin/env bash

TMP_KERNELSPEC_DIR=$1
KERNELSPEC_HOME=$2

# TODO: only run this script for the kernels we're actually installing

# Create kernel directories for those which do not already exist
mkdir ${KERNELSPEC_HOME}/python{2,3}
mkdir ${KERNELSPEC_HOME}/pyspark{2,3}
mkdir ${KERNELSPEC_HOME}/ir

# Replace the contents of the Python kernel scripts
sed -e 's/${PY_VERSION}/2/g' -e 's|${JUPYTER_HOME}|'${JUPYTER_HOME}'|g' ${TMP_KERNELSPEC_DIR}/python_kernelspec.tmpl > ${KERNELSPEC_HOME}/python2/kernel.json
sed -e 's/${PY_VERSION}/3/g' -e 's|${JUPYTER_HOME}|'${JUPYTER_HOME}'|g' ${TMP_KERNELSPEC_DIR}/python_kernelspec.tmpl > ${KERNELSPEC_HOME}/python3/kernel.json

# Replace the contents of the PySpark kernel scripts
sed -e 's/${PY_VERSION}/2/g' -e 's|${JUPYTER_HOME}|'${JUPYTER_HOME}'|g' ${TMP_KERNELSPEC_DIR}/pyspark_kernelspec.tmpl > ${KERNELSPEC_HOME}/pyspark2/kernel.json
sed -e 's/${PY_VERSION}/3/g' -e 's|${JUPYTER_HOME}|'${JUPYTER_HOME}'|g' ${TMP_KERNELSPEC_DIR}/pyspark_kernelspec.tmpl > ${KERNELSPEC_HOME}/pyspark3/kernel.json

# Replace the contents of the R kernel script
sed 's|${JUPYTER_HOME}|'${JUPYTER_HOME}'|g' ${TMP_KERNELSPEC_DIR}/r_kernelspec.tmpl > ${KERNELSPEC_HOME}/ir/kernel.json