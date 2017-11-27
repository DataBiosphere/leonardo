#!/bin/bash

cd /etc

# /etc/spark/conf/spark-defaults.conf has been mounted in during docker-compose
# $HAILHASH has been ENV'd in from the Dockerfile
# spark-defaults.conf.template is ADDed in the Dockerfile

echo "Running construct-spark-defaults.sh:" >> /etc/construct-spark-defaults.log
echo "HAILHASH: $HAILHASH" >> /etc/construct-spark-defaults.log
echo "SPARK_HOME: $SPARK_HOME" >> /etc/construct-spark-defaults.log

# Move the mounted spark-defaults.conf to the right place
cp /etc/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
# Render our "extras" using find and replace [HAILHASH] with the hash
sed "s|\[HAILHASH\]|$HAILHASH|g" $SPARK_HOME/conf/spark-defaults.conf.template > $SPARK_HOME/conf/spark-defaults.extras
# Append our extras to the Dataproc-provided spark-defaults
cat $SPARK_HOME/conf/spark-defaults.extras >> $SPARK_HOME/conf/spark-defaults.conf
