#!/bin/bash
# v1.0 zarsky@broad
# v2.0 jcanas@broad

hash fswatch 2>/dev/null || {
    echo >&2 "This script requires fswatch (https://github.com/emcrisostomo/fswatch), but it's not installed. On Darwin, just \"brew install fswatch\".  Aborting."; exit 1;
}

if [ -z ${HELM_SCALA_SDK_DIR+x} ]; then
  echo "Looks like HELM_SCALA_SDK_DIR isn't set. Please 'export HELM_SCALA_SDK_DIR=[helm-scala-sdk repo location]' with no trailing slash"
  exit 1
fi

docker rm -f leonardo-proxy sqlproxy leonardo-helm-lib leonardo-sbt
docker network rm fc-leonardo
docker network rm fc-leonardo
pkill -P $$

clean_up () {
    echo
    echo "Cleaning up after myself..."
    docker rm -f leonardo-proxy sqlproxy leonardo-helm-lib leonardo-sbt
    docker network rm fc-leonardo
    pkill -P $$
}
# trap clean_up EXIT HUP INT QUIT PIPE TERM 0 20

echo "Creating shared volumes if they don't exist..."
docker volume create --name jar-cache
docker volume create --name coursier-cache
docker volume create --name helm-lib-build

REPO_ROOT="$(git rev-parse --show-toplevel)"
SECRETS_DIR="${REPO_ROOT}/http/src/main/resources/rendered"
HELMLIB_BUILD_DIR="${REPO_ROOT}/local/helm-scala-sdk/out"

start_server () {
    docker network create fc-leonardo

    echo "Creating Google sqlproxy container..."
    docker create --name sqlproxy \
    --restart "always" \
    --network="fc-leonardo" \
    -p 3306:3306 \
    --env-file="${SECRETS_DIR}/sqlproxy.env" \
    broadinstitute/cloudsqlproxy:1.11_20180808

    docker cp ${SECRETS_DIR}/leonardo-account.json sqlproxy:/etc/sqlproxy-service-account.json

    # echo "Creating helm docker container..."
    # docker build -t helm-lib - < $HELM_SCALA_SDK_DIR/Dockerfile
    # docker create --name leonardo-helm-lib -v helm-lib-build:/build \
    # helm-lib

    echo "Creating SBT docker container..."
    docker create -it --name leonardo-sbt -w /app \
    -v jar-cache:/root/.ivy -v jar-cache:/root/.ivy2 \
    -v coursier-cache:/home/sbtuser/.cache \
    -v helm-lib-build:/helm-go-lib-build \
    -v "${REPO_ROOT}:/app" \
    -p 25050:5050 -p 8080:8080 -p 9000:9000 \
    --network=fc-leonardo \
    --env-file="${SECRETS_DIR}/sbt.env" \
    -e HOSTNAME=$HOSTNAME \
    hseeberger/scala-sbt:17.0.2_1.6.2_3.1.1 \
    sbt http/run

    echo "Copying files to SBT docker container..."
    docker cp ${SECRETS_DIR}/leonardo-account.pem leonardo-sbt:/etc/leonardo-account.pem
    docker cp ${SECRETS_DIR}/leonardo-account.json leonardo-sbt:/etc/leonardo-account.json
    docker cp ${SECRETS_DIR}/jupyter-server.crt leonardo-sbt:/etc/jupyter-server.crt
    docker cp ${SECRETS_DIR}/jupyter-server.key leonardo-sbt:/etc/jupyter-server.key
    docker cp ${SECRETS_DIR}/leo-client.p12 leonardo-sbt:/etc/leo-client.p12
    docker cp ${SECRETS_DIR}/rootCA.key leonardo-sbt:/etc/rootCA.key
    docker cp ${SECRETS_DIR}/rootCA.pem leonardo-sbt:/etc/rootCA.pem

    echo "Creating proxy..."
    docker create --name leonardo-proxy \
    --restart "always" \
    --network=fc-leonardo \
    --env-file="${SECRETS_DIR}/proxy.env" \
    -p 20080:80 -p 30443:443 \
    us.gcr.io/broad-dsp-gcr-public/openidc-terra-proxy:v0.1.17

    docker cp ${SECRETS_DIR}/server.crt leonardo-proxy:/etc/ssl/certs/server.crt
    docker cp ${SECRETS_DIR}/server.key leonardo-proxy:/etc/ssl/private/server.key
    docker cp ${SECRETS_DIR}/oauth2.conf leonardo-proxy:/etc/apache2/mods-enabled/oauth2.conf
    docker cp ${SECRETS_DIR}/site.conf leonardo-proxy:/etc/apache2/sites-available/site.conf

    echo "Starting sqlproxy..."
    docker start sqlproxy
    echo "Starting proxy..."
    docker start leonardo-proxy
    # echo "Starting SBT..."
    # docker start -ai leonardo-sbt
}
start_server

