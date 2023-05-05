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

if [ -a ./.docker-rsync-local.pid ]; then
    echo "Looks like clean-up wasn't completed, doing it now..."
    docker rm -f leonardo-rsync-container leonardo-proxy leonardo-sbt sqlproxy
    docker network rm fc-leonardo
    pkill -P $(< "./.docker-rsync-local.pid")
    rm ./.docker-rsync-local.pid
fi

clean_up () {
    echo
    echo "Cleaning up after myself..."
    docker rm -f leonardo-rsync-container leonardo-proxy sqlproxy leonardo-helm-lib leonardo-sbt
    docker network rm fc-leonardo
    pkill -P $$
    rm ./.docker-rsync-local.pid
}
trap clean_up EXIT HUP INT QUIT PIPE TERM 0 20

echo "Creating shared volumes if they don't exist..."
docker volume create --name leonardo-shared-source
docker volume create --name jar-cache

SECRETS_DIR="$(git rev-parse --show-toplevel)/rendered"

echo "Launching rsync container..."
docker run -d \
    --name leonardo-rsync-container \
    -v leonardo-shared-source:/working \
    -e DAEMON=docker \
    tjamet/rsync

run_rsync ()  {
    rsync --blocking-io -azl --delete -e "docker exec -i" . leonardo-rsync-container:working \
        --filter='+ /build.sbt' \
        --filter='+ /http/' \
        --filter='+ /http/src/***' \
        --filter='+ /core/' \
        --filter='+ /core/src/***' \
        --filter='+ /config/***' \
        --filter='+ /docker/***' \
        --filter='+ /jupyter-docker/***' \
        --filter='+ /project/***' \
        --filter='+ /.git/***' \
        --filter='- *'
}
echo "Performing initial file sync..."
run_rsync
fswatch -o . | while read f; do run_rsync; done &
echo $$ > ./.docker-rsync-local.pid

start_server () {
    docker network create fc-leonardo

    echo "Creating Google sqlproxy container..."
    docker create --name sqlproxy \
    --restart "always" \
    --network="fc-leonardo" \
    --env-file="${SECRETS_DIR}/sqlproxy.env" \
    broadinstitute/cloudsqlproxy:1.11_20180808

    docker cp ${SECRETS_DIR}/leonardo-account.json sqlproxy:/etc/sqlproxy-service-account.json

    echo "Creating helm docker container..."
    docker build -t helm-lib - < $HELM_SCALA_SDK_DIR/Dockerfile
    docker create --name leonardo-helm-lib -v /build \
    helm-lib

    echo "Creating SBT docker container..."
    docker create -it --name leonardo-sbt \
    -v leonardo-shared-source:/app -w /app \
    -v jar-cache:/root/.ivy -v jar-cache:/root/.ivy2 \
    -p 25050:5050 -p 8080:8080 -p 9000:9000 \
    --network=fc-leonardo \
    --env-file="env/local.env" \
    --env-file="rendered/secrets.env" \
    -e HOSTNAME=$HOSTNAME \
    -e SBT_OPTS='-Xmx4G -Xms4G -Xss2M -XX:+UseG1GC' \
    -e JAVA_OPTS='-DXmx4G -DXms4G -DXss2M -Dsun.net.spi.nameservice.provider.1=default -Dsun.net.spi.nameservice.provider.2=dns,Jupyter -Djna.library.path=/helm-go-lib-build -Dconfig.resource=leo.conf' \
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
    docker cp /etc/localtime leonardo-sbt:/etc/localtime
    docker cp leonardo-helm-lib:/build /tmp
    docker cp /tmp/build/ leonardo-sbt:/helm-go-lib-build

    echo "Creating proxy..."
    docker create --name leonardo-proxy \
    --restart "always" \
    --network=fc-leonardo \
    --env-file="rendered/proxy.env" \
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
    echo "Starting SBT..."
    docker start -ai leonardo-sbt
}
start_server

