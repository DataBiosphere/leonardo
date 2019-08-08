# adapted from https://hub.docker.com/r/jupyter/base-notebook/ AKA https://github.com/jupyter/docker-stacks/tree/master/base-notebook

FROM debian:stretch

USER root

#######################
# Prerequisites
#######################

ENV DEBIAN_FRONTEND noninteractive
ENV DEBIAN_REPO http://cdn-fastly.deb.debian.org
ENV CRAN_REPO http://cran.mtu.edu

RUN echo "deb $DEBIAN_REPO/debian stretch main"                   > /etc/apt/sources.list \
 && echo "deb $DEBIAN_REPO/debian-security stretch/updates main" >> /etc/apt/sources.list \
 && echo "deb $DEBIAN_REPO/debian stretch-backports main"        >> /etc/apt/sources.list \
 && echo "deb $DEBIAN_REPO/debian testing main"                  >> /etc/apt/sources.list \
 && echo 'APT::Default-Release "oldstable";' | tee -a /etc/apt/apt.conf.d/00local \
 && apt-get update \
 && apt-get -yq dist-upgrade \
 && apt-get install -yq --no-install-recommends \
    unzip \
    nano \
    sudo \
    default-jre \
    default-jdk \
    gnupg \
    dirmngr \
    wget \
    ca-certificates \
    curl \
    build-essential \
    lsb-release \
    procps \
    openssl \
    make \
    aptitude \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    llvm \
    libncurses5-dev \
    libncursesw5-dev \
    xz-utils \
    tk-dev \
    samtools \
    # to support userScript pip installs via git
    git \
    locales \
    jq \

 # R separately because it depends on gnupg installed above
 && echo "deb $CRAN_REPO/bin/linux/debian stretch-cran35/"       >> /etc/apt/sources.list \
 && apt-key adv  --no-tty --keyserver keyserver.ubuntu.com \
    --recv-key 'E19F5F87128899B192B1A2C2AD5F960A256A04AF' \

 # Uncomment en_US.UTF-8 for inclusion in generation
 && sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
 # Generate locale
 && locale-gen \

 # google-cloud-sdk separately because it need lsb-release and other prereqs installed above
 && export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" \
 && echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" > /etc/apt/sources.list.d/google-cloud-sdk.list \
 && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
 && apt-get update \
 && apt-get install -yq --no-install-recommends \
    google-cloud-sdk \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ENV LC_ALL en_US.UTF-8

#######################
# GATK
#######################

RUN set -e
ENV GATK_VERSION 4.1.0.0
ENV GATK_ZIP_PATH /tmp/gatk-${GATK_VERSION}.zip

 #download the gatk zip
RUN curl -L -o $GATK_ZIP_PATH https://github.com/broadinstitute/gatk/releases/download/$GATK_VERSION/gatk-$GATK_VERSION.zip \
 && unzip -o $GATK_ZIP_PATH -d /etc/ \
 && ln -s /etc/gatk-$GATK_VERSION/gatk /bin/gatk

##############################
# Spark / Hadoop / Hive / Hail
##############################

# Use Spark 2.2.0 which corresponds to Dataproc 1.2. See:
#   https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-versions
# Note: we are actually using Spark 2.2.1, but the Hail package is built using 2.2.0
ENV SPARK_VER 2.2.0
ENV SPARK_HOME=/usr/lib/spark

# result of `gsutil cat gs://hail-common/builds/0.2/latest-hash/cloudtools-3-spark-2.2.0.txt` on 26 March 2019
ENV HAILHASH daed180b84d8
ENV HAILJAR hail-0.2-$HAILHASH-Spark-$SPARK_VER.jar
ENV HAILPYTHON hail-0.2-$HAILHASH.zip
ENV HAIL_HOME /etc/hail
ENV KERNELSPEC_HOME /usr/local/share/jupyter/kernels

# Note Spark and Hadoop are mounted from the outside Dataproc VM.
# Make empty conf dirs for the update-alternatives commands.
RUN mkdir -p /etc/spark/conf.dist && mkdir -p /etc/hadoop/conf.empty && mkdir -p /etc/hive/conf.dist \
 && update-alternatives --install /etc/spark/conf spark-conf /etc/spark/conf.dist 100 \
 && update-alternatives --install /etc/hadoop/conf hadoop-conf /etc/hadoop/conf.empty 100 \
 && update-alternatives --install /etc/hive/conf hive-conf /etc/hive/conf.dist 100 \
 && mkdir $HAIL_HOME && cd $HAIL_HOME \
 && wget -nv http://storage.googleapis.com/hail-common/builds/0.2/jars/$HAILJAR \
 && wget -nv http://storage.googleapis.com/hail-common/builds/0.2/python/$HAILPYTHON \
 && cd -

#######################
# Python / Jupyter
#######################

ENV USER jupyter-user
ENV UID 1000
ENV HOME /home/$USER

# The welder uid is consistent with the Welder docker definition here:
#  https://github.com/DataBiosphere/welder/blob/master/project/Settings.scala
# Adding welder-user to the Jupyter container isn't strictly required, but it makes welder-added
# files display nicer when viewed in a terminal.
ENV WELDER_USER welder-user
ENV WELDER_UID 1001

# ensure this matches c.NotebookApp.port in jupyter_notebook_config.py
ENV JUPYTER_PORT 8000
ENV JUPYTER_HOME /etc/jupyter
ENV PYSPARK_DRIVER_PYTHON jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS notebook

ENV PATH $SPARK_HOME:$SPARK_HOME/python:$SPARK_HOME/bin:$HAIL_HOME:$PATH
ENV PYTHONPATH $PYTHONPATH:$HAIL_HOME/$HAILPYTHON:$HAIL_HOME/python:$SPARK_HOME/python:$JUPYTER_HOME/custom

RUN apt-get update \
 && apt-get install -yq --no-install-recommends \
    python\
    python-dev \
    liblzo2-dev \
    python-tk \
    liblzo2-dev \
    libz-dev \

 # NOTE! not sure why, but this must run before pip installation
 && useradd -m -s /bin/bash -N -u $UID $USER \
 && useradd -m -s /bin/bash -N -u $WELDER_UID $WELDER_USER \
 # jessie's default pip doesn't work well with jupyter
 && wget -nv https://bootstrap.pypa.io/get-pip.py \
 && python get-pip.py \
 && pip install ipykernel==4.10.0 \
 && python2 -m ipykernel install --name python2 --display-name "Python 2" \
 # Hail requires decorator
 && pip install -U decorator==4.3.0 \
 && pip install numpy==1.15.2 \
 && pip install py4j==0.10.7 \
 && python -mpip install matplotlib==2.2.3\
 && pip install pandas==0.23.4 \
 && pip install seaborn==0.9.0 \
 && pip install google-api-core==1.5.0 \
 && pip install google-cloud-bigquery==1.7.0 \
 && pip install google-cloud-bigquery-datatransfer==0.1.1 \
 && pip install google-cloud-core==0.28.1 \
 && pip install google-cloud-datastore==1.7.0 \
 && pip install google-cloud-resource-manager==0.28.1 \
 && pip install google-cloud-storage==1.13.0 \
 && pip install google-auth==1.5.1 \
 && pip install --ignore-installed firecloud==0.16.18 \
 && pip install -U scikit-learn==0.20.0 \
 && pip install statsmodels==0.9.0 \
 && pip install ggplot==0.11.5 \
 # fixed current ggplot issue where it imports Timestamp from pandas.lib (deprecated) instead of pandas
 && sed -i 's/pandas.lib/pandas/g' /usr/local/lib/python2.7/dist-packages/ggplot/stats/smoothers.py \
 && pip install bokeh==1.0.0  \
 && pip install pyfasta==0.5.2 \
 && pip install pdoc==0.3.2 \
 && pip install biopython==1.72 \
 && pip install bx-python==0.8.2 \
 && pip install fastinterval==0.1.1 \
 && pip install matplotlib-venn==0.11.5 \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Python 3 Kernel
ENV PYTHON_VERSION 3.6.8

# lifted from https://github.com/docker-library/python/blob/dd36c08c1f94083476a8579b8bf20c4cd46c6400/3.6/stretch/Dockerfile
RUN set -ex \
 \
 && wget -O python.tar.xz "https://www.python.org/ftp/python/${PYTHON_VERSION%%[a-z]*}/Python-$PYTHON_VERSION.tar.xz" \
 && wget -O python.tar.xz.asc "https://www.python.org/ftp/python/${PYTHON_VERSION%%[a-z]*}/Python-$PYTHON_VERSION.tar.xz.asc" \
 && export GNUPGHOME="$(mktemp -d)" \
 && gpg --batch --keyserver keyserver.ubuntu.com --recv-keys "0D96DF4D4110E5C43FBFB17F2D347EA6AA65421D" \
 && gpg --batch --verify python.tar.xz.asc python.tar.xz \
 && { command -v gpgconf > /dev/null && gpgconf --kill all || :; } \
 && rm -rf "$GNUPGHOME" python.tar.xz.asc \
 && mkdir -p /usr/src/python \
 && tar -xJC /usr/src/python --strip-components=1 -f python.tar.xz \
 && rm python.tar.xz \
 \
 && cd /usr/src/python \
 && gnuArch="$(dpkg-architecture --query DEB_BUILD_GNU_TYPE)" \
 && ./configure \
    --build="$gnuArch" \
    --enable-loadable-sqlite-extensions \
    --enable-shared \
    --with-system-expat \
    --with-system-ffi \
    --without-ensurepip \
 && make -j "$(nproc)" \
 && make install \
 && ldconfig \
 \
 && find /usr/local -depth \
    \( \
        \( -type d -a \( -name test -o -name tests \) \) \
        -o \
        \( -type f -a \( -name '*.pyc' -o -name '*.pyo' \) \) \
    \) -exec rm -rf '{}' + \
 && rm -rf /usr/src/python \
 \
 && python3 --version

# make some useful symlinks that are expected to exist
RUN cd /usr/local/bin \
   && ln -s idle3 idle \
   && ln -s pydoc3 pydoc \
   && ln -s python3 python \
   && ln -s python3-config python-config

# if this is called "PIP_VERSION", pip explodes with "ValueError: invalid truth value '<VERSION>'"
ENV PYTHON_PIP_VERSION 19.0.1

RUN set -ex; \
    \
    wget -O get-pip.py 'https://bootstrap.pypa.io/get-pip.py'; \
   \
    python get-pip.py \
        --disable-pip-version-check \
        --no-cache-dir \
        "pip==$PYTHON_PIP_VERSION" \
    ; \
    pip --version; \
    \
    find /usr/local -depth \
        \( \
            \( -type d -a \( -name test -o -name tests \) \) \
            -o \
            \( -type f -a \( -name '*.pyc' -o -name '*.pyo' \) \) \
        \) -exec rm -rf '{}' +; \
    rm -f get-pip.py

RUN apt-get update \
 && apt-get install -t testing -yq --no-install-recommends \
    # for jupyterlab extensions
    nodejs \
    npm \
 && pip3 install tornado==4.5.3 \
# # Hail requires decorator
 && pip3 install -U decorator==4.3.0 \
 && pip3 install parsimonious==0.8.1 \
# # python 3 packages
 && pip3 install numpy==1.15.2 \
 && pip3 install py4j==0.10.7 \
 && python3 -mpip install matplotlib==3.0.0 \
 && pip3 install pandas==0.23.4 \
 && pip3 install seaborn==0.9.0 \
 && pip3 install notebook==5.7.8 \
 && pip3 install jupyter==1.0.0 \
 && pip3 install jupyterlab==0.35.4 \
 && pip3 install python-lzo==1.12 \
 && pip3 install google-api-core==1.5.0 \
 && pip3 install google-cloud-bigquery==1.7.0 \
 && pip3 install google-cloud-bigquery-datatransfer==0.1.1 \
 && pip3 install google-cloud-core==0.28.1 \
 && pip3 install google-cloud-datastore==1.7.0 \
 && pip3 install google-cloud-resource-manager==0.28.1 \
 && pip3 install google-cloud-storage==1.13.0 \
 && pip3 install --ignore-installed firecloud==0.16.18 \
 && pip3 install scikit-learn==0.20.0 \
 && pip3 install statsmodels==0.9.0 \
 && pip3 install ggplot==0.11.5 \
 # fixed current ggplot issue where it imports Timestamp from pandas.lib (deprecated) instead of pandas
 && sed -i 's/pandas.lib/pandas/g' /usr/local/lib/python3.6/site-packages/ggplot/stats/smoothers.py \
 && pip3 install bokeh==1.0.0 \
 && pip3 install pyfasta==0.5.2 \
 && pip3 install pdoc==0.3.2 \
 && pip3 install biopython==1.72 \
 && pip3 install bx-python==0.8.2 \
 && pip3 install fastinterval==0.1.1 \
 && pip3 install matplotlib-venn==0.11.5 \
 && pip3 install bleach==1.5.0 \
 && pip3 install cycler==0.10.0 \
 && pip3 install enum34==1.1.6 \
 && pip3 install h5py==2.7.1 \
 && pip3 install html5lib==0.9999999 \
 && pip3 install joblib==0.11 \
 && pip3 install keras==2.2.0 \
 && pip3 install markdown==2.6.9 \
 && pip3 install patsy==0.4.1 \
 && pip3 install protobuf==3.7.1 \
 && pip3 install pymc3==3.1 \
 && pip3 install pyparsing==2.2.0 \
 && pip3 install pysam==0.13 \
 && pip3 install python-dateutil==2.6.1 \
 && pip3 install pytz==2017.3 \
 && pip3 install pyvcf==0.6.8 \
 && pip3 install pyyaml==3.12 \
 && pip3 install scipy==1.0.0 \
 && pip3 install six==1.11.0 \
 && pip3 install tensorflow==1.9.0 \
 && pip3 install theano==0.9.0 \
 && pip3 install tqdm==4.19.4 \
 && pip3 install werkzeug==0.12.2 \
 && pip3 install certifi==2016.2.28 \
 && pip3 install intel-openmp==2018.0.0 \
 && pip3 install mkl==2018.0.3 \
 && pip3 install readline==6.2 \
 && pip3 install setuptools==36.4.0 \
 && pip3 install wheel \
 # for jupyter_localize_extension
 && pip3 install python-datauri \
 && pip3 install jupyter_contrib_nbextensions \
 && pip3 install jupyter_nbextensions_configurator \
 && pip3 install /etc/gatk-$GATK_VERSION/gatkPythonPackageArchive.zip \
 && pip3 install cookiecutter \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# make pip install to a user directory, instead of a system directory which requires root.
# this is useful so `pip install` commands can be run in the context of a notebook.
ENV PIP_USER=true

#######################
# R Kernel
#######################

# using aptitude for R packages so that all dependencies are automatically installed
RUN aptitude update && aptitude install -y apt-utils \
 && aptitude update && aptitude -t stretch-cran35 install -y \
    r-base=3.5.2-1 \
    r-base-dev=3.5.2-1 \
    r-base-core=3.5.2-1 \
    r-recommended=3.5.2-1 \
 && aptitude install -t stretch-cran35 -y \
    fonts-dejavu \
    tzdata \
    gfortran \
    gcc \
    libcurl4 \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
 && aptitude clean \
 && rm -rf /var/lib/apt/lists/* \
 # fixes broken gfortan dependency needed by some R libraries
 # see: https://github.com/DataBiosphere/leonardo/issues/710
 && ln -s /usr/lib/x86_64-linux-gnu/libgfortran.so.5 /usr/lib/x86_64-linux-gnu/libgfortran.so

RUN R -e 'install.packages(c( \
    "IRdisplay",  \
    "evaluate",  \
    "pbdZMQ",  \
    "devtools",  \
    "uuid",  \
    "reshape2",  \
    "bigrquery",  \
    "googleCloudStorageR", \
    "tidyverse"), \
    repos="http://cran.mtu.edu")' \
 && R -e 'devtools::install_github("DataBiosphere/Ronaldo")' \
 && R -e 'devtools::install_github("IRkernel/IRkernel")' \
 && R -e 'IRkernel::installspec(user=FALSE)' \
 && chown -R $USER:users /home/jupyter-user/.local  \
 && R -e 'devtools::install_github("apache/spark@v2.2.3", subdir="R/pkg")' \
 && mkdir -p /home/jupyter-user/.rpackages \
 && echo "R_LIBS=/home/jupyter-user/.rpackages" > /home/jupyter-user/.Renviron \
 && chown -R $USER:users /home/jupyter-user/.rpackages

#######################
# Customizations
#######################

ADD scripts $JUPYTER_HOME/scripts
ADD custom $JUPYTER_HOME/custom

RUN chown -R $USER:users $JUPYTER_HOME \
 && chown -R $USER:users /usr/local/share/jupyter/lab \
 && find $JUPYTER_HOME/scripts -name '*.sh' -type f | xargs chmod +x \
 && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-6 200 \
 && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-8 100 \
 && $JUPYTER_HOME/scripts/extension/install_jupyter_contrib_nbextensions.sh \
 && mkdir $HOME/.jupyter/.custom/ \
 && cp $JUPYTER_HOME/custom/google_sign_in.js $HOME/.jupyter/.custom/ \
 && cp $JUPYTER_HOME/custom/extension_entry_jupyter.js $HOME/.jupyter/.custom/custom.js \
 && cp $JUPYTER_HOME/custom/safe-mode.js $HOME/.jupyter/.custom/ \
 && cp $JUPYTER_HOME/custom/edit-mode.js $HOME/.jupyter/.custom/ \
 && $JUPYTER_HOME/scripts/extension/jupyter_install_lab_extension.sh $JUPYTER_HOME/custom/extension_entry_jupyterlab.js \
 && mkdir $JUPYTER_HOME/nbconfig

USER $USER
WORKDIR $HOME

EXPOSE $JUPYTER_PORT

# Note: this entrypoint is provided for running Jupyter independently of Leonardo.
# When Leonardo deploys this image onto a cluster, the entrypoint is overwritten to enable
# additional setup inside the container before execution.  Jupyter execution occurs when the
# init-actions.sh script uses 'docker exec' to call run-jupyter.sh.
ENTRYPOINT ["/usr/local/bin/jupyter", "notebook"]
