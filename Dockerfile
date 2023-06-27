# Multi-stage build:
#   1. Build the Helm client Go lib
#   2. Deploy Leonardo pointing to the Go lib

FROM golang:1.20 AS helm-go-lib-builder

# TODO Consider moving repo set-up to the build script to make CI versioning easier
RUN mkdir /helm-go-lib-build && \
    cd /helm-go-lib-build && \
    git clone https://github.com/broadinstitute/helm-scala-sdk.git && \
    cd helm-scala-sdk && \
    git checkout master && \
    cd helm-go-lib && \
    go build -o libhelm.so -buildmode=c-shared main.go

# Use this graalvm image if we need to use jstack etc
# FROM ghcr.io/graalvm/graalvm-ce:ol8-java11-21.0.0.2
FROM us.gcr.io/broad-dsp-gcr-public/base/jre:17-debian

EXPOSE 8080
EXPOSE 5050

ENV GIT_HASH $GIT_HASH
ENV HELM_DEBUG 1
# WARNING: If you are changing any versions here, update it in the reference.conf
ENV TERRA_APP_SETUP_VERSION 0.0.19
ENV TERRA_APP_VERSION 0.5.0
# This is galaxykubeman, which references Galaxy
ENV GALAXY_VERSION 2.5.2
ENV NGINX_VERSION 4.3.0
# If you update this here, make sure to also update reference.conf:
ENV CROMWELL_CHART_VERSION 0.2.268
ENV CROWELL_ON_AZURE_CHART_VERSION 0.2.268
ENV WDS_CHART_VERSION 0.4.0
ENV HAIL_BATCH_CHART_VERSION 0.1.9
ENV RSTUDIO_CHART_VERSION 0.2.0

RUN mkdir /leonardo
COPY ./leonardo*.jar /leonardo
COPY --from=helm-go-lib-builder /helm-go-lib-build/helm-scala-sdk/helm-go-lib /leonardo/helm-go-lib

# Install the Helm3 CLI client using a provided script because installing it via the RHEL package managing didn't work
RUN curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 && \
    chmod 700 get_helm.sh && \
    ./get_helm.sh --version v3.11.2 && \
    rm get_helm.sh

# Add the repos containing nginx, galaxy, setup apps, custom apps, cromwell and aou charts
RUN helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx && \
    helm repo add galaxy https://raw.githubusercontent.com/cloudve/helm-charts/anvil/ && \
    helm repo add terra-app-setup-charts https://storage.googleapis.com/terra-app-setup-chart && \
    helm repo add terra https://terra-app-charts.storage.googleapis.com && \
    helm repo add cromwell-helm https://broadinstitute.github.io/cromwhelm/charts/ && \
    helm repo add aad-pod-identity https://raw.githubusercontent.com/Azure/aad-pod-identity/master/charts && \
    helm repo add terra-helm https://terra-helm.storage.googleapis.com && \
    helm repo add aou-rstudio-chart https://storage.googleapis.com/terra-app-helm/aou-rstudio-chart && \
    helm repo update

COPY ./wds-0.4.0.tgz /leonardo
RUN tar -xzf /leonardo/wds-0.4.0.tgz -C /leonardo

# .Files helm helper can't access files outside a chart. Hence in order to populate cert file properly, we're
# pulling `terra-app-setup` locally and add cert files to the chart.
# Leonardo will install the chart from local version.
# We are also caching charts so they are not downloaded with every helm-install
RUN cd /leonardo && \
    helm repo update && \
    helm pull terra-app-setup-charts/terra-app-setup --version $TERRA_APP_SETUP_VERSION --untar && \
    helm pull galaxy/galaxykubeman --version $GALAXY_VERSION --untar && \
    helm pull terra/terra-app --version $TERRA_APP_VERSION --untar  && \
    helm pull ingress-nginx/ingress-nginx --version $NGINX_VERSION --untar && \
    helm pull cromwell-helm/cromwell --version $CROMWELL_CHART_VERSION --untar && \
    helm pull cromwell-helm/cromwell-on-azure --version $CROWELL_ON_AZURE_CHART_VERSION --untar && \
    helm pull aou-rstudio-chart/aou-rstudio-chart --version $RSTUDIO_CHART_VERSION --untar && \
    helm pull oci://terradevacrpublic.azurecr.io/hail/hail-batch-terra-azure --version $HAIL_BATCH_CHART_VERSION --untar && \
    cd /

# Install https://github.com/apangin/jattach to get access to JDK tools
RUN apt-get update && \
    apt-get install jattach

# Copy Terra-docker-versions-candidate.json from bucket into docker root
RUN curl -fsSL -o /terra-docker-versions-candidate.json \
    https://storage.googleapis.com/terra-docker-image-documentation/terra-docker-versions-candidate.json

# Add Leonardo as a service (it will start when the container starts)
# 1. "Exec" form of CMD necessary to avoid `sh` stripping environment variables with periods in them,
#    used for Lightbend config
# 2. $JAVA_OPTS and filesystem like /leonardo/leonardo*.jar both necessary as long as Leonardo runs on
#    Kubernetes without foundation (firecloud-develop requires former, old chart requires latter)
# We use the "exec" form but call `bash` to accomplish both 1 and 2
CMD ["/bin/bash", "-c", "java $JAVA_OPTS -jar $(find /leonardo -name 'leonardo*.jar')"]
