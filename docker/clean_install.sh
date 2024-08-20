#!/bin/bash

# This script runs sbt assembly to produce a target jar file.
# Used by build_jar.sh
# chmod +x must be set for this script
set -eux

LEONARDO_DIR=$1
cd $LEONARDO_DIR

export SBT_OPTS="-Xmx6G -Xms6G -Xss4m -Dsbt.ivy.home=/home/vsts/.ivy2"
echo "starting sbt clean assembly ..."

sbt -v \
  "project http" \
  "set assembly / test := {}"  \
  clean assembly

echo "... clean assembly complete, finding and moving jar ..."
LEONARDO_JAR=$(find http/target | grep 'http-assembly.*\.jar')

# new generated jar name starts with `http`, but renaming it to `leonardo*.jar`
NEW_JAR_NAME=$(echo $LEONARDO_JAR| sed 's/http\-/leonardo\-/g')
mv $LEONARDO_JAR $NEW_JAR_NAME
mv $NEW_JAR_NAME ./
echo "... jar moved."