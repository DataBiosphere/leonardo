#!/bin/bash

set -e

LEONARDO_DIR=$1
cd $LEONARDO_DIR

# Test
sbt -J-Xms4g -J-Xmx4g test
sbt -J-Xms4g -J-Xmx4g assembly
LEONARDO_JAR=$(find target | grep 'leonardo.*\.jar')
mv $LEONARDO_JAR .
sbt clean
