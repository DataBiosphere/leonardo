FROM rocker/tidyverse:latest 

# google-cloud-sdk
RUN apt-get update && apt-get install -yq --no-install-recommends \
    gnupg \
    curl \
    lsb-release \
 && export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" \
 && echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" > /etc/apt/sources.list.d/google-cloud-sdk.list \
 && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
 && apt-get update \
 && apt-get install -yq --no-install-recommends \
    google-cloud-sdk \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# google-cloud R packages
RUN R -e 'install.packages(c( \
    "bigrquery",  \
    "googleCloudStorageR"), \
    repos="http://cran.mtu.edu")' \
 && R -e 'devtools::install_github("DataBiosphere/Ronaldo")'

ENV RSTUDIO_PORT 8001
ENV RSTUDIO_HOME /etc/rstudio

ADD rserver.conf $RSTUDIO_HOME/rserver.conf

RUN sed 's/alwaysSaveHistory="0"/alwaysSaveHistory="1"/g' \
 && sed 's/loadRData="0"/loadRData="1"/g' \
 && sed 's/saveAction="0"/saveAction="1"/g'

EXPOSE $RSTUDIO_PORT
