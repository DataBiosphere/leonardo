# adapted from https://hub.docker.com/r/jupyter/base-notebook/ AKA https://github.com/jupyter/docker-stacks/tree/master/base-notebook

FROM debian:stretch

USER root

#######################
# Prerequisites
#######################

ENV DEBIAN_FRONTEND noninteractive
ENV DEBIAN_REPO http://cdn-fastly.deb.debian.org

RUN echo "deb $DEBIAN_REPO/debian stretch main\ndeb $DEBIAN_REPO/debian-security stretch/updates main" > /etc/apt/sources.list \
 && apt-get update && apt-get -yq dist-upgrade \
 && apt-get install -yq --no-install-recommends \
    nano \
    wget \
    gnupg \
    ca-certificates \
    curl \
    build-essential \
    lsb-release \
    procps \

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

ENV JAVA_VER jdk1.8.0_161
ENV JAVA_TGZ jdk-8u161-linux-x64.tar.gz
ENV JAVA_URL http://download.oracle.com/otn-pub/java/jdk/8u161-b12/2f38c3b165be4555a1fa6e98c45e0808/$JAVA_TGZ
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
    libz-dev \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* \

 # NOTE! not sure why, but this must run before pip installation
 && useradd -m -s /bin/bash -N -u $UID $USER \
 # jessie's default pip doesn't work well with jupyter
 && wget https://bootstrap.pypa.io/get-pip.py \
 && python get-pip.py \

 # Hail requires decorator
 && pip install -U decorator jupyter \
 && pip install google-cloud \
 && pip install firecloud==0.16.7 \
 && pip install -U scikit-learn \
 && pip install ggplot \
 && pip install bokeh \
 && pip install pyfasta \
 && pip install pdoc \
 && pip install biopython \
 && pip install bx-python \
 && pip install fastinterval \
 && pip install matplotlib-venn \
 && pip install "https://github.com/all-of-us/workbench/archive/pyclient-v0-1-rc3.zip#egg=aou_workbench_client&subdirectory=client/py"


# Install Python 3 Kernel
RUN apt-get update \
 && apt-get install -yq --no-install-recommends python3 \
 && python3 get-pip.py \
 && pip3 install ipykernel \
 && python3 -m ipykernel install --name python3 --display-name "Python 3"

# pip3 install python3-dev \
RUN pip3 install matplotlib \
 && pip3 install python-pandas \
 && pip3 install python-seaborn \
 && pip3 install python-tk \
 && pip3 install python-numpy \
 && pip3 install libz-dev \
 #&& pip install -U decorator jupyter \  -- idk if i need this
 && pip3 install google-cloud \
 && pip3 install firecloud==0.16.7 \
 && pip3 install -U scikit-learn \
 && pip3 install ggplot \
 && pip3 install bokeh \
 && pip3 install pyfasta \
 && pip3 install pdoc \
 && pip3 install biopython \
 && pip3 install bx-python \
 && pip3 install fastinterval \
 && pip3 install matplotlib-venn \
 && pip3 install "https://github.com/all-of-us/workbench/archive/pyclient-v0-1-rc3.zip#egg=aou_workbench_client&subdirectory=client/py"



#######################
# Utilities
#######################

ADD spark_install_hail.sh $HAIL_HOME/
ADD jupyter_notebook_config.py $JUPYTER_HOME/
ADD jupyter_localize_extension.py $JUPYTER_HOME/custom/
ADD jupyter_install_notebook_extension.sh $JUPYTER_HOME/

RUN chown -R $USER:users $JUPYTER_HOME \
 && chmod +x $JUPYTER_HOME/jupyter_install_notebook_extension.sh \
 && chmod +x $HAIL_HOME/spark_install_hail.sh

USER $USER
WORKDIR $HOME

EXPOSE $JUPYTER_PORT
ENTRYPOINT ["pyspark"]
