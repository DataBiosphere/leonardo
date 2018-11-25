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
 && echo 'APT::Default-Release "stable";' | tee -a /etc/apt/apt.conf.d/00local \
 && apt-get update \
 && apt-get -yq dist-upgrade \
 && apt-get install -yq --no-install-recommends \
    nano \
    sudo \
    gnupg \
    dirmngr \
    wget \
    ca-certificates \
    curl \
    build-essential \
    lsb-release \
    procps \
    openssl \
    # to support userScript pip installs via git
    git \
    locales \

 # R separately because it depends on gnupg installed above
 && echo "deb $CRAN_REPO/bin/linux/debian stretch-cran35/"       >> /etc/apt/sources.list \
 && apt-key adv  --no-tty --keyserver keys.gnupg.net \
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
# Java
#######################

ENV JAVA_VER jdk1.8.0_191
ENV JAVA_TGZ jdk-8u191-linux-x64.tar.gz
ENV JAVA_URL http://download.oracle.com/otn-pub/java/jdk/8u191-b12/2787e4a523244c269598db4e85c51e0c/$JAVA_TGZ
ENV JAVA_HOME /usr/lib/jdk/$JAVA_VER

RUN wget --header "Cookie: oraclelicense=accept-securebackup-cookie" $JAVA_URL \
 && mkdir -p /usr/lib/jdk && tar -zxf $JAVA_TGZ -C /usr/lib/jdk \
 && update-alternatives --install /usr/bin/java java $JAVA_HOME/bin/java 100 \
 && update-alternatives --install /usr/bin/javac javac $JAVA_HOME/bin/javac 100 \
 && rm $JAVA_TGZ

##############################
# Spark / Hadoop / Hive / Hail
##############################

# Use Spark 2.2.0 which corresponds to Dataproc 1.2. See:
#   https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-versions
# Note: we are actually using Spark 2.2.1, but the Hail package is built using 2.2.0
ENV SPARK_VER 2.2.0
ENV SPARK_HOME=/usr/lib/spark

# result of `gsutil cat gs://hail-common/builds/devel/latest-hash-spark-2.2.0.txt` on 18 October 2018
ENV HAILHASH 17a988f2a628
ENV HAILJAR hail-devel-$HAILHASH-Spark-$SPARK_VER.jar
ENV HAILPYTHON hail-devel-$HAILHASH.zip
ENV HAILZIP Hail-devel-$HAILHASH-Spark-$SPARK_VER.zip
ENV HAIL_HOME /etc/hail
ENV KERNELSPEC_HOME /usr/local/share/jupyter/kernels

# Note Spark and Hadoop are mounted from the outside Dataproc VM.
# Make empty conf dirs for the update-alternatives commands.
RUN mkdir -p /etc/spark/conf.dist && mkdir -p /etc/hadoop/conf.empty && mkdir -p /etc/hive/conf.dist \
 && update-alternatives --install /etc/spark/conf spark-conf /etc/spark/conf.dist 100 \
 && update-alternatives --install /etc/hadoop/conf hadoop-conf /etc/hadoop/conf.empty 100 \
 && update-alternatives --install /etc/hive/conf hive-conf /etc/hive/conf.dist 100 \
 && mkdir $HAIL_HOME && cd $HAIL_HOME \
 && wget -nv http://storage.googleapis.com/hail-common/builds/devel/jars/$HAILJAR \
 && wget -nv http://storage.googleapis.com/hail-common/builds/devel/python/$HAILPYTHON \
 && wget -nv http://storage.googleapis.com/hail-common/distributions/devel/$HAILZIP \
 && cd -

#######################
# Python / Jupyter
#######################

ENV USER jupyter-user
ENV UID 1000
ENV HOME /home/$USER

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
RUN apt-get update \
 && apt-get install -t testing -yq --no-install-recommends \
    python3.6 \
    python3.6-dev \
    python3-distutils \
 && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.6 100 \
 && python3 get-pip.py \
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
 && pip3 install jupyter==1.0.0 \
 && pip3 install jupyterlab==0.35.2 \
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
 && pip3 install bokeh==1.0.0 \
 && pip3 install pyfasta==0.5.2 \
 && pip3 install pdoc==0.3.2 \
 && pip3 install biopython==1.72 \
 && pip3 install bx-python==0.8.2 \
 && pip3 install fastinterval==0.1.1 \
 && pip3 install matplotlib-venn==0.11.5 \
 # for jupyter_localize_extension
 && pip3 install python-datauri \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# fixed current ggplot issue where it imports Timestamp from pandas.lib (deprecated) instead of pandas
RUN sed -i 's/pandas.lib/pandas/g' /usr/local/lib/python3.6/dist-packages/ggplot/stats/smoothers.py

# make pip install to a user directory, instead of a system directory which requires root.
# this is useful so `pip install` commands can be run in the context of a notebook.
ENV PIP_USER=true

#######################
# R Kernel
#######################

RUN apt-get update && apt-get install -y --no-install-recommends apt-utils \
 && apt-get update && apt-get -t stretch-cran35 install -y --no-install-recommends \
    r-base \
    r-base-dev \
    r-recommended \
 && apt-get install -t stretch-backports -y --no-install-recommends \
    fonts-dejavu \
    tzdata \
    gfortran \
    gcc \
    libcurl4 \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

RUN R -e 'install.packages(c( \
    "IRdisplay",  \
    "evaluate",  \
    "pbdZMQ",  \
    "devtools",  \
    "uuid",  \
    "reshape2",  \
    "bigrquery",  \
    "tidyverse"), \
    repos="http://cran.mtu.edu")' \
 && R -e 'devtools::install_github("DataBiosphere/Ronaldo")' \
 && R -e 'devtools::install_github("IRkernel/IRkernel")' \
 && R -e 'IRkernel::installspec(user=FALSE)' \
 && chown -R $USER:users /home/jupyter-user/.local  \
 && R -e 'devtools::install_github("apache/spark@v2.2.0", subdir="R/pkg")' \
 && mkdir -p /home/jupyter-user/.rpackages \
 && echo "R_LIBS=/home/jupyter-user/.rpackages" > /home/jupyter-user/.Renviron \
 && chown -R $USER:users /home/jupyter-user/.rpackages

#######################
# Utilities
#######################

ADD scripts $JUPYTER_HOME/scripts
ADD custom/jupyter_delocalize.py $JUPYTER_HOME/custom/
ADD custom/jupyter_localize_extension.py $JUPYTER_HOME/custom/

RUN chown -R $USER:users $JUPYTER_HOME \
 && find $JUPYTER_HOME/scripts -name '*.sh' -type f | xargs chmod +x

USER $USER
WORKDIR $HOME

EXPOSE $JUPYTER_PORT
ENTRYPOINT ["/usr/local/bin/jupyter", "notebook"]