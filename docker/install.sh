#!/bin/bash

set -e

LEONARDO_DIR=$1
cd $LEONARDO_DIR
rm -f leonardo*.jar

# Test
JAVA_OPTS="-Dmysql.host=mysql -Dmysql.port=3306 -Xms8g -Xmx8g" sbt -J-XX:MaxMetaspaceSize=1g "project http" clean test assembly
LEONARDO_JAR=$(find http/target | grep 'http-assembly.*\.jar')

# new generated jar name starts with `http`, but renaming it to `leonardo*.jar`
NEW_JAR_NAME=$(echo $LEONARDO_JAR|sed 's/http\-/leonardo\-/g' )
mv $LEONARDO_JAR $NEW_JAR_NAME
mv $NEW_JAR_NAME ./
sbt clean
