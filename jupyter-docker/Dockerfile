# adapted from https://hub.docker.com/r/jupyter/base-notebook/ AKA https://github.com/jupyter/docker-stacks/tree/master/base-notebook

FROM debian:jessie

USER root

#######################
# Prerequisites
#######################

ENV DEBIAN_FRONTEND noninteractive
ENV DEBIAN_REPO http://cdn-fastly.deb.debian.org
ENV CRAN_REPO http://cran.mtu.edu

RUN echo "deb $DEBIAN_REPO/debian jessie main"                   > /etc/apt/sources.list \
 && echo "deb $DEBIAN_REPO/debian-security jessie/updates main" >> /etc/apt/sources.list \
 && echo "deb $DEBIAN_REPO/debian jessie-backports main"        >> /etc/apt/sources.list \
 && echo "deb $CRAN_REPO/bin/linux/debian jessie-cran34/"       >> /etc/apt/sources.list \
 && apt-key adv --keyserver keys.gnupg.net \
    --recv-key 'E19F5F87128899B192B1A2C2AD5F960A256A04AF' \
 && apt-get update \
 && apt-get -yq dist-upgrade \
 && apt-get install -yq --no-install-recommends \
    nano \
    wget \
    gnupg \
    ca-certificates \
    curl \
    build-essential \
    lsb-release \
    procps \
    # to support userScript pip installs via git
    git \

 # google-cloud-sdk separately because it need lsb-release and other prereqs installed above
 && export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" \
 && echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" > /etc/apt/sources.list.d/google-cloud-sdk.list \
 && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
 && apt-get update \
 && apt-get install -yq --no-install-recommends \
    google-cloud-sdk

#######################
# Java
#######################

ENV JAVA_VER jdk1.8.0_171
ENV JAVA_TGZ jdk-8u171-linux-x64.tar.gz
ENV JAVA_URL http://download.oracle.com/otn-pub/java/jdk/8u171-b11/512cd62ec5174c3487ac17c61aaa89e8/$JAVA_TGZ
ENV JAVA_HOME /usr/lib/jdk/$JAVA_VER

RUN wget --header "Cookie: oraclelicense=accept-securebackup-cookie" $JAVA_URL \
 && mkdir -p /usr/lib/jdk && tar -zxf $JAVA_TGZ -C /usr/lib/jdk \
 && update-alternatives --install /usr/bin/java java $JAVA_HOME/bin/java 100 \
 && update-alternatives --install /usr/bin/javac javac $JAVA_HOME/bin/javac 100 \
 && rm $JAVA_TGZ

##############################
# Spark / Hadoop / Hive / Hail
##############################

# Use Spark 2.0.2 which corresponds to Dataproc 1.1. See:
#   https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-versions
# Dataproc supports Spark 2.2.0, but there are no pre-packaged Hail distributions past 2.1.0. See:
#   https://hail.is/docs/stable/getting_started.html
ENV SPARK_VER 2.0.2
ENV SPARK_HOME=/usr/lib/spark

# result of `gsutil cat gs://hail-common/builds/0.1/latest-hash-spark-2.0.2.txt` on 11 January 2018
ENV HAILHASH 5c275cc216e1
ENV HAILJAR hail-0.1-$HAILHASH-Spark-$SPARK_VER.jar
ENV HAILPYTHON hail-0.1-$HAILHASH.zip
ENV HAILZIP Hail-0.1-$HAILHASH-Spark-$SPARK_VER.zip
ENV HAIL_HOME /etc/hail
ENV KERNELSPEC_HOME /usr/local/share/jupyter/kernels

# Note Spark and Hadoop are mounted from the outside Dataproc VM.
# Make empty conf dirs for the update-alternatives commands.
RUN mkdir -p /etc/spark/conf.dist && mkdir -p /etc/hadoop/conf.empty && mkdir -p /etc/hive/conf.dist \
 && update-alternatives --install /etc/spark/conf spark-conf /etc/spark/conf.dist 100 \
 && update-alternatives --install /etc/hadoop/conf hadoop-conf /etc/hadoop/conf.empty 100 \
 && update-alternatives --install /etc/hive/conf hive-conf /etc/hive/conf.dist 100 \
 && mkdir $HAIL_HOME && cd $HAIL_HOME \
 && wget http://storage.googleapis.com/hail-common/builds/0.1/jars/$HAILJAR \
 && wget http://storage.googleapis.com/hail-common/builds/0.1/python/$HAILPYTHON \
 && wget http://storage.googleapis.com/hail-common/distributions/0.1/$HAILZIP \
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
ENV PYTHONPATH $PYTHONPATH:$HAIL_HOME/$HAILPYTHON:$HAIL_HOME/python:$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.3-src.zip:$JUPYTER_HOME/custom

RUN apt-get install -yq --no-install-recommends \
    python \
    python-dev \
    liblzo2-dev \

    # useful for analysis
    python-matplotlib \
    python-pandas \
    python-seaborn \
    python-tk \
    python-numpy \
    liblzo2-dev \
    libz-dev \

 # NOTE! not sure why, but this must run before pip installation
 && useradd -m -s /bin/bash -N -u $UID $USER \
 # jessie's default pip doesn't work well with jupyter 
 && wget https://bootstrap.pypa.io/get-pip.py \
 && python get-pip.py \

 # Hail requires decorator
 && pip install -U decorator==4.2.1 \
 && pip install -U jupyter \
 && pip install google-cloud \
 && pip install --ignore-installed firecloud==0.16.7 \
 && pip install -U scikit-learn \
 && pip install statsmodels==0.8.0 \
 && pip install ggplot \
 && pip install bokeh \
 && pip install pyfasta \
 && pip install pdoc \
 && pip install biopython \
 && pip install bx-python \
 && pip install fastinterval \
 && pip install matplotlib-venn \
 # for jupyter_localize_extension
 && pip install python-datauri

# Python 3 Kernel
RUN apt-get install -yq --no-install-recommends python3 \
        python3-dev \
        python3-matplotlib \
        python3-pandas \
        python3-tk \
        python3-numpy \
 && python3 get-pip.py \
 # install the python 3 kernel
 && pip3 install tornado==4.5.3 \
 && pip3 install ipykernel==4.8.2 \
 && python3 -m ipykernel install --name python3 --display-name "Python 3" \
 # python 3 packages
 && pip3 install seaborn \
 && pip3 install python-lzo \
 && pip3 install google-cloud \
 && pip3 install --ignore-installed firecloud==0.16.7 \
 && pip3 install -U scikit-learn \
 && pip3 install statsmodels==0.8.0 \
 && pip3 install ggplot \
 && pip3 install bokeh \
 && pip3 install pyfasta \
 && pip3 install pdoc \
 && pip3 install biopython \
 && pip3 install bx-python \
 && pip3 install fastinterval \
 && pip3 install matplotlib-venn

# make pip install to a user directory, instead of a system directory which requires root.
# this is useful so `pip install` commands can be run in the context of a notebook.
ENV PIP_USER=true

#######################
# R Kernel
#######################

RUN apt-get -t jessie-cran34 install -y --no-install-recommends \
    r-base \
    r-base-dev \
    r-recommended \
 && apt-get install -y --no-install-recommends \
    fonts-dejavu \
    tzdata \
    gfortran \
    gcc \
    libcurl4-openssl-dev \
    libssl-dev

RUN R -e 'install.packages(c( \
    "repr",  \
    "IRdisplay",  \
    "evaluate",  \
    "crayon",  \
    "pbdZMQ",  \
    "devtools",  \
    "uuid",  \
    "digest",  \
    "ggplot2",  \
    "dplyr",  \
    "tidyr",  \
    "reshape2",  \
    "bigrquery"),  \
    repos="https://cran.mtu.edu")' \
 && R -e 'devtools::install_github("IRkernel/IRkernel")' \
 && R -e 'IRkernel::installspec(user=FALSE)' \
 && chown -R $USER:users /home/jupyter-user/.local  \
 && R -e 'devtools::install_github("apache/spark@v2.0.2", subdir="R/pkg")' \
 && mkdir -p /home/jupyter-user/.rpackages \
 && echo "R_LIBS=/home/jupyter-user/.rpackages" > /home/jupyter-user/.Renviron \
 && chown -R $USER:users /home/jupyter-user/.rpackages

#######################
# Utilities
#######################

ADD kernelspec.sh $JUPYTER_HOME/
ADD python_kernelspec.tmpl $JUPYTER_HOME/
ADD pyspark_kernelspec.tmpl $JUPYTER_HOME/
ADD spark_install_hail.sh $HAIL_HOME/
ADD jupyter_notebook_config.py $JUPYTER_HOME/
ADD jupyter_localize_extension.py $JUPYTER_HOME/custom/
ADD jupyter_delocalize.py $JUPYTER_HOME/custom/
ADD scripts/extension-installation/jupyter_install_notebook_extension.sh $JUPYTER_HOME/
ADD scripts/extension-installation/jupyter_install_server_extension.sh $JUPYTER_HOME/
ADD scripts/extension-installation/jupyter_install_combined_extension.sh $JUPYTER_HOME/
ADD scripts/extension-installation/jupyter_pip_install_notebook_extension.sh $JUPYTER_HOME/
ADD scripts/extension-installation/jupyter_pip_install_server_extension.sh $JUPYTER_HOME/
ADD scripts/extension-installation/jupyter_pip_install_combined_extension.sh $JUPYTER_HOME/


RUN chown -R $USER:users $JUPYTER_HOME \
 && chmod +x $JUPYTER_HOME/jupyter_install_notebook_extension.sh \
 && chmod +x $JUPYTER_HOME/jupyter_install_server_extension.sh \
 && chmod +x $JUPYTER_HOME/jupyter_install_combined_extension.sh \
 && chmod +x $JUPYTER_HOME/jupyter_pip_install_notebook_extension.sh \
 && chmod +x $JUPYTER_HOME/jupyter_pip_install_server_extension.sh \
 && chmod +x $JUPYTER_HOME/jupyter_pip_install_combined_extension.sh \
 && chmod +x $HAIL_HOME/spark_install_hail.sh \
 && chmod +x $JUPYTER_HOME/kernelspec.sh

# This is a hacky way to get the ui to stop flickering as a result of kernel switches, restarts and reconnects from within a notebook
# This edits the already built main.min.js file to prevent the notebook from scheduling a reconnect when a websocket is in a closing state
# As a result, the websocket is allowed to reconnect on its own and doesn't get stuck in an infinite reconnection loop.
# It should be removed at the first opportunity :(
RUN sed -i "s/this._schedule_reconnect();//" ./usr/local/lib/python2.7/dist-packages/notebook/static/notebook/js/main.min.js

USER $USER
WORKDIR $HOME

EXPOSE $JUPYTER_PORT
ENTRYPOINT ["/usr/local/bin/jupyter", "notebook"]
