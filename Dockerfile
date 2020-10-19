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

FROM oracle/graalvm-ce:20.2.0-java8

EXPOSE 8080
EXPOSE 5050

ENV GIT_HASH $GIT_HASH

RUN mkdir /leonardo
COPY ./leonardo*.jar /leonardo
COPY --from=helm-go-lib-builder /helm-go-lib-build/helm-scala-sdk/helm-go-lib /leonardo/helm-go-lib

# Install the Helm3 CLI client using a provided script because installing it via the RHEL package managing didn't work
RUN curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 && \
    chmod 700 get_helm.sh && \
    ./get_helm.sh --version v3.2.4 && \
    rm get_helm.sh

# Add the repos containing nginx and galaxy charts
RUN helm repo add stable https://kubernetes-charts.storage.googleapis.com/ && \
    helm repo add galaxy https://raw.githubusercontent.com/cloudve/helm-charts/anvil/ && \
    helm repo update

# Add Leonardo as a service (it will start when the container starts)
CMD java $JAVA_OPTS -jar $(find /leonardo -name 'leonardo*.jar')