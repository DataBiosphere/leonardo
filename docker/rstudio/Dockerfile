FROM rocker/tidyverse:latest

# google-cloud-sdk and python dev libs
RUN apt-get update && apt-get install -yq --no-install-recommends \
    gnupg \
    curl \
    lsb-release \
    libpython-dev \
    libpython3-dev \
 && export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" \
 && echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" > /etc/apt/sources.list.d/google-cloud-sdk.list \
 && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
 && apt-get update \
 && apt-get install -yq --no-install-recommends \
    google-cloud-sdk \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* \
 && curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
 && python get-pip.py

# google-cloud R packages
RUN R -e 'install.packages(c( \
    "bigrquery",  \
    "googleCloudStorageR"), \
    repos="http://cran.mtu.edu")' \
 && R -e 'devtools::install_github("DataBiosphere/Ronaldo")'

ENV RSTUDIO_PORT 8001
ENV RSTUDIO_HOME /etc/rstudio

ADD rserver.conf $RSTUDIO_HOME/rserver.conf

ENV RSTUDIO_USERSETTING /home/rstudio/.rstudio/monitored/user-settings/user-settings

RUN sed -i 's/alwaysSaveHistory="0"/alwaysSaveHistory="1"/g'  $RSTUDIO_USERSETTING \
&& sed -i 's/loadRData="0"/loadRData="1"/g' $RSTUDIO_USERSETTING \
&& sed -i 's/saveAction="0"/saveAction="1"/g' $RSTUDIO_USERSETTING

EXPOSE $RSTUDIO_PORT
