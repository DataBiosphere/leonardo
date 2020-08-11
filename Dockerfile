# Multi-stage build:
#   1. Build the Helm client Go lib
#   2. Deploy Leonardo pointing to the Go lib

FROM golang:1.14.6-stretch AS helm-go-lib-builder

# TODO Consider moving repo set-up to the build script to make CI versioning easier
RUN mkdir /helm-go-lib-build && \
    cd /helm-go-lib-build && \
    git clone https://github.com/broadinstitute/helm-scala-sdk.git && \
    cd helm-scala-sdk/helm-go-lib && \
    go build -o libhelm.dylib -buildmode=c-shared main.go

FROM oracle/graalvm-ce:20.0.0-java8

EXPOSE 8080
EXPOSE 5050

ENV GIT_HASH $GIT_HASH

RUN mkdir /leonardo
COPY ./leonardo*.jar /leonardo
COPY --from=helm-go-lib-builder /helm-go-lib-build/helm-scala-sdk/helm-go-lib /leonardo/helm-go-lib

# Add Leonardo as a service (it will start when the container starts)
CMD java $JAVA_OPTS -jar $(find /leonardo -name 'leonardo*.jar')