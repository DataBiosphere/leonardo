#!/bin/bash

# $SPARK_HOME has been mounted in during docker-compose
# $HAIL* has been created from the Dockerfile

LOG=/etc/spark_install_hail.log
SPARK_EXTRAS=/etc/spark-defaults.extras

echo "Running spark_install_hail.sh:" >> $LOG
echo "SPARK_HOME: ${SPARK_HOME}" >> $LOG
echo "HAIL_HOME: ${HAIL_HOME}" >> $LOG
echo "HAILJAR: ${HAILJAR}" >> $LOG
echo "HAILPYTHON: ${HAILPYTHON}" >> $LOG
echo "HAILZIP: ${HAILZIP}" >> $LOG

# Render our "extras" with the Hail configs
cat <<EOT >> ${SPARK_EXTRAS}
###################
# required by Hail
###################

# Distributes from master node to the working directories of executors

spark.jars ${HAIL_HOME}/${HAILJAR}
spark.submit.pyFiles ${HAIL_HOME}/${HAILZIP},${HAIL_HOME}/${HAILPYTHON}

# Add JARs to Classpaths: driver can use absolute paths
spark.driver.extraClassPath ${HAIL_HOME}/${HAILJAR}

# Add JARs to Classpaths: distributed to executor working directory by above spark.jars directive
spark.executor.extraClassPath ${HAIL_HOME}/${HAILJAR}

# Hail needs at least 50GB

spark.sql.files.maxPartitionBytes=100000000000
spark.sql.files.openCostInBytes=100000000000
EOT

# Append our extras to the Dataproc-provided spark-defaults
cat ${SPARK_EXTRAS} >> ${SPARK_HOME}/conf/spark-defaults.conf