#!/bin/bash
set -e
GATK_VERSION=4.0.6.0
GATK_ZIP_PATH=/tmp/gatk-$GATK_VERSION.zip

#download the gatk zip if it doesn't already exist
if ! [ -f $GATK_ZIP_PATH ]; then
  # curl and follow redirects and output to a temp file
  curl -L -o $GATK_ZIP_PATH https://github.com/broadinstitute/gatk/releases/download/$GATK_VERSION/gatk-$GATK_VERSION.zip
fi

# unzip with forced overwrite (if necessary) to /bin
unzip -o $GATK_ZIP_PATH -d /etc/

# make a symlink to gatk right inside bin so it's available from the existing PATH
ln -s /etc/gatk-$GATK_VERSION/gatk /bin/gatk