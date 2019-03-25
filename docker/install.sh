#!/bin/bash

set -e

LEONARDO_DIR=$1
cd $LEONARDO_DIR
rm -f leonardo*.jar

# Test
sbt -batch -J-Xms4g -J-Xmx4g test -Dmysql.host=mysql -Dmysql.port=3306
sbt -batch -J-Xms4g -J-Xmx4g assembly
LEONARDO_JAR=$(find target | grep 'leonardo.*\.jar')
mv $LEONARDO_JAR .
sbt clean
