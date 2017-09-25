# adapted from https://hub.docker.com/r/jupyter/base-notebook/ AKA https://github.com/jupyter/docker-stacks/tree/master/base-notebook

# Debian Jessie debootstrap from 2017-02-27
# https://github.com/docker-library/official-images/commit/aa5973d0c918c70c035ec0746b8acaec3a4d7777
FROM debian@sha256:52af198afd8c264f1035206ca66a5c48e602afb32dc912ebf9e9478134601ec4

USER root

ENV DEBIAN_FRONTEND noninteractive
ENV DEBIAN_REPO http://cdn-fastly.deb.debian.org

ENV JAVA_VER jdk1.8.0_131
ENV JAVA_TGZ jdk-8u131-linux-x64.tar.gz
ENV JAVA_URL http://download.oracle.com/otn-pub/java/jdk/8u131-b11/d54c1d3a095b4ff2b6607d096fa80163/$JAVA_TGZ
ENV JAVA_HOME /usr/lib/jdk/$JAVA_VER

ENV SPARK_VER 2.0.2
ENV SPARK_TGZ spark-$SPARK_VER.tgz
ENV SPARK_URL http://d3kbcqa49mib13.cloudfront.net/$SPARK_TGZ
ENV SPARK_HOME /spark/spark-$SPARK_VER

# result of `gsutil cat gs://hail-common/builds/0.1/latest-hash-spark-2.0.2.txt` on 22 September 2017
ENV HAILHASH 6f8598516c88
ENV HAILJAR hail-0.1-$HAILHASH-Spark-$SPARK_VER.jar
ENV HAILPYTHON hail-0.1-$HAILHASH.zip
ENV HAILZIP Hail-0.1-$HAILHASH-Spark-$SPARK_VER.zip
ENV HAIL_HOME /hail

# YARN configuration required by DataProc Spark
# mount this from outside
ENV HADOOP_CONF_DIR /etc/hadoop/conf

ENV PATH $SPARK_HOME:$SPARK_HOME/python:$SPARK_HOME/bin:$HAIL_HOME:$PATH
ENV PYTHONPATH $PYTHONPATH:$HAIL_HOME/$HAILPYTHON:$HAIL_HOME/python:$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.3-src.zip

ENV USER jupyter-user
ENV UID 1000
ENV HOME /home/$USER

# ensure this matches c.NotebookApp.port in jupyter_notebook_config.py
ENV JUPYTER_PORT 8000
ENV JUPYTER_HOME /etc/jupyter/
ENV PYSPARK_DRIVER_PYTHON jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS notebook

RUN echo "deb $DEBIAN_REPO/debian jessie main\ndeb $DEBIAN_REPO/debian-security jessie/updates main" > /etc/apt/sources.list \
 && apt-get update && apt-get -yq dist-upgrade \
 && apt-get install -yq --no-install-recommends \
    wget \
    maven \
    git \
    build-essential \

    python \
    python-dev \

    # useful for analysis
    python-matplotlib \
    python-pandas \
    python-seaborn \
    python-tk \
    python-numpy \
    libz-dev \

 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* \

 # Java
 && wget --header "Cookie: oraclelicense=accept-securebackup-cookie" $JAVA_URL \
 && mkdir -p /usr/lib/jdk && tar -zxf $JAVA_TGZ -C /usr/lib/jdk \
 && update-alternatives --install /usr/bin/java java $JAVA_HOME/bin/java 100 \
 && update-alternatives --install /usr/bin/javac javac $JAVA_HOME/bin/javac 100 \
 && rm $JAVA_TGZ \

 # Spark
 && wget $SPARK_URL && mkdir /spark && tar -zxf $SPARK_TGZ -C /spark \
 && cd $SPARK_HOME && ./build/mvn -Pyarn -DskipTests clean package && cd - \
 && rm $SPARK_TGZ \
 # Needed to access gs:// files in Spark/Hail
 && wget -q https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar -O $SPARK_HOME/gcs-connector.jar \

 # Hail
 && mkdir $HAIL_HOME && cd $HAIL_HOME \
 && wget http://storage.googleapis.com/hail-common/builds/0.1/jars/$HAILJAR \
 && wget http://storage.googleapis.com/hail-common/builds/0.1/python/$HAILPYTHON \
 && wget http://storage.googleapis.com/hail-common/distributions/0.1/$HAILZIP \
 && cd - \

 # NOTE! not sure why, but this must run before pip installation
 && useradd -m -s /bin/bash -N -u $UID $USER \

 # ubuntu's default pip doesn't work well with jupyter
 && wget https://bootstrap.pypa.io/get-pip.py \
 && python get-pip.py \

 # Hail requires decorator
 && pip install -U decorator jupyter \
 && pip install google-cloud \
 && pip install firecloud \
 && pip install -U scikit-learn \
 && pip install ggplot \
 && pip install bokeh \
 && pip install pyfasta \
 && pip install pdoc \
 && pip install biopython \
 && pip install bx-python \
 && pip install fastinterval \
 && pip install matplotlib-venn


ADD spark-defaults.conf.template $SPARK_HOME/conf/
# Spark defaults conf find and replace [HAILHASH] with the hash
RUN sed "s|\[HAILHASH\]|$HAILHASH|g" $SPARK_HOME/conf/spark-defaults.conf.template > $SPARK_HOME/conf/spark-defaults.conf
ADD jupyter_notebook_config.py $JUPYTER_HOME
RUN chown -R $USER:users $JUPYTER_HOME

USER $USER
WORKDIR $HOME

EXPOSE $JUPYTER_PORT
ENTRYPOINT ["pyspark"]
