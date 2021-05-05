# Multi-stage build:
#   1. Build the Helm client Go lib
#   2. Deploy Leonardo pointing to the Go lib

FROM golang:1.14.6-stretch AS helm-go-lib-builder

# TODO Consider moving repo set-up to the build script to make CI versioning easier
RUN mkdir /helm-go-lib-build && \
    cd /helm-go-lib-build && \
    git clone https://github.com/broadinstitute/helm-scala-sdk.git && \
    cd helm-scala-sdk && \
    git checkout master && \
    cd helm-go-lib && \
    go build -o libhelm.so -buildmode=c-shared main.go

FROM ghcr.io/graalvm/graalvm-ce:ol8-java11-21.0.0.2

EXPOSE 8080
EXPOSE 5050

ENV GIT_HASH $GIT_HASH
ENV HELM_DEBUG 1
# WARNING: If you are changing any versions here, update it in the reference.conf
ENV TERRA_APP_SETUP_VERSION 0.0.2
ENV TERRA_APP_VERSION 0.3.0
ENV GALAXY_VERSION 1.0.0
ENV NGINX_VERSION 3.23.0

RUN mkdir /leonardo
COPY ./leonardo*.jar /leonardo
COPY --from=helm-go-lib-builder /helm-go-lib-build/helm-scala-sdk/helm-go-lib /leonardo/helm-go-lib

# Install the Helm3 CLI client using a provided script because installing it via the RHEL package managing didn't work
RUN curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 && \
    chmod 700 get_helm.sh && \
    ./get_helm.sh --version v3.2.4 && \
    rm get_helm.sh

# Add the repos containing nginx and galaxy charts
# TODO update Galaxy repo when 1.0.0 is officially released
RUN helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx && \
    helm repo add galaxy https://raw.githubusercontent.com/almahmoud/helm-charts/gkm-release-2101-2/ && \
    helm repo add terra-app-setup-charts https://storage.googleapis.com/terra-app-setup-chart && \
    helm repo add terra https://terra-app-charts.storage.googleapis.com && \
    helm repo update

# .Files helm helper can't access files outside a chart. Hence in order to populate cert file properly, we're
# pulling `terra-app-setup` locally and add cert files to the chart.
# Leonardo will install the chart from local version.
# We are also cacheing charts so they are not downloaded with every helm-install

RUN pushd /leonardo && \
    helm pull terra-app-setup-charts/terra-app-setup --version $TERRA_APP_SETUP_VERSION --untar && \
    helm pull galaxy/galaxykubeman --version $GALAXY_VERSION --untar && \
    helm pull terra/terra-app --version $TERRA_APP_VERSION --untar  && \
    helm pull ingress-nginx/ingress-nginx --version $NGINX_VERSION --untar && \
    popd

# Add Leonardo as a service (it will start when the container starts)
CMD java $JAVA_OPTS -jar $(find /leonardo -name 'leonardo*.jar')
