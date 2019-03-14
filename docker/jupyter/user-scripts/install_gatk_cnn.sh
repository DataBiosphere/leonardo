#!/bin/bash

apt-get install -yq --no-install-recommends \
    certifi=2016.2.28=py36_0 \
    intel-openmp=2018.0.0 \
    mkl=2018.0.1 \
    mkl-service=1.1.2 \
    readline=6.2=2 \
    setuptools=36.4.0=py36_1 \
    sqlite=3.13.0=0 \
    tk=8.5.18=0 \
    wheel=0.29.0=py36_0 \
    xz=5.2.3=0 \
    zlib=1.2.11=0

pip3 install biopython==1.70
pip3 install bleach==1.5.0
pip3 install cycler==0.10.0
pip3 install enum34==1.1.6
pip3 install h5py==2.7.1
pip3 install html5lib==0.9999999
pip3 install joblib==0.11
pip3 install keras==2.2.0
pip3 install markdown==2.6.9
pip3 install matplotlib==2.1.0
pip3 install numpy==1.13.3
pip3 install pandas==0.21.0
pip3 install patsy==0.4.1
pip3 install protobuf==3.5.0.post1
pip3 install pymc3==3.1
pip3 install pyparsing==2.2.0
pip3 install pysam==0.13
pip3 install python-dateutil==2.6.1
pip3 install pytz==2017.3
pip3 install pyvcf==0.6.8
pip3 install pyyaml==3.12
pip3 install scikit-learn==0.19.1
pip3 install scipy==1.0.0
pip3 install six==1.11.0
pip3 install tensorflow==1.9.0
pip3 install theano==0.9.0
pip3 install tqdm==4.19.4
pip3 install werkzeug==0.12.2

set -e
GATK_VERSION=4.1.0.0
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

pip3 install /etc/gatk-$GATK_VERSION/gatkPythonPackageArchive.zip

