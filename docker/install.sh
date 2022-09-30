#!/bin/bash

set -e

LEONARDO_DIR=$1
cd $LEONARDO_DIR
rm -f leonardo*.jar

# Test
JAVA_OPTS="-Dmysql.host=mysql -Dmysql.port=3306 -Xmx4G -Xms4G -Xss4m -XX:ReservedCodeCacheSize=128m" sbt -v -J-XX:MaxMetaspaceSize=1g "project http" clean assembly
LEONARDO_JAR=$(find http/target | grep 'http-assembly.*\.jar')

# new generated jar name starts with `http`, but renaming it to `leonardo*.jar`
NEW_JAR_NAME=$(echo $LEONARDO_JAR|sed 's/http\-/leonardo\-/g' )
mv $LEONARDO_JAR $NEW_JAR_NAME
mv $NEW_JAR_NAME ./
sbt clean
