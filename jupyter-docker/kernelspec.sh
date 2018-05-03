#!/usr/bin/env bash

TMP_KERNELSPEC_DIR=$1
KERNELSPEC_HOME=$2

# replace the contents of the Python kernel scripts
sed 's/${PY_VERSION}/2/g' ${TMP_KERNELSPEC_DIR}/python_kernelspec.tmpl > ${KERNELSPEC_HOME}/python2/kernel.json
sed 's/${PY_VERSION}/3/g' ${TMP_KERNELSPEC_DIR}/python_kernelspec.tmpl > ${KERNELSPEC_HOME}/python3/kernel.json

# replace the contents of the PySpark kernel scripts
sed 's/${PY_VERSION}/2/g' ${TMP_KERNELSPEC_DIR}/pyspark_kernelspec.tmpl > ${KERNELSPEC_HOME}/pyspark2/kernel.json
sed 's/${PY_VERSION}/3/g' ${TMP_KERNELSPEC_DIR}/pyspark_kernelspec.tmpl > ${KERNELSPEC_HOME}/pyspark3/kernel.json


