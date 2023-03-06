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
    --env-file="./config/sqlproxy.env" \
    broadinstitute/cloudsqlproxy:1.11_20180808

    docker cp config/sqlproxy-service-account.json sqlproxy:/etc/sqlproxy-service-account.json

    echo "Creating helm docker container..."
    docker build -t helm-lib - < $HELM_SCALA_SDK_DIR/Dockerfile
    docker create --name leonardo-helm-lib -v /build \
    helm-lib

    echo "Creating SBT docker container..."
    docker create -it --name leonardo-sbt \
    -v leonardo-shared-source:/app -w /app \
    -v jar-cache:/root/.ivy -v jar-cache:/root/.ivy2 \
    -p 25050:5050 \
    --network=fc-leonardo \
    -e HOSTNAME=$HOSTNAME \
    -e JAVA_OPTS='-Dconfig.file=/app/config/leonardo.conf -DXmx4G -DXms4G -DXss2M -Dsun.net.spi.nameservice.provider.1=default -Dsun.net.spi.nameservice.provider.2=dns,Jupyter -Djna.library.path=/helm-go-lib-build' \
    hseeberger/scala-sbt:17.0.2_1.6.2_3.1.1 \
    sbt http/run

    echo "Copying files to SBT docker container..."
    docker cp config/leonardo-account.pem leonardo-sbt:/etc/leonardo-account.pem
    docker cp config/leonardo-account.json leonardo-sbt:/etc/leonardo-account.json
    docker cp config/jupyter-server.crt leonardo-sbt:/etc/jupyter-server.crt
    docker cp config/jupyter-server.key leonardo-sbt:/etc/jupyter-server.key
    docker cp config/leo-client.p12 leonardo-sbt:/etc/leo-client.p12
    docker cp config/rootCA.key leonardo-sbt:/etc/rootCA.key
    docker cp config/rootCA.pem leonardo-sbt:/etc/rootCA.pem
    docker cp /etc/localtime leonardo-sbt:/etc/localtime
    docker cp leonardo-helm-lib:/build /tmp
    docker cp /tmp/build/ leonardo-sbt:/helm-go-lib-build


    echo "Creating proxy..."
    docker create --name leonardo-proxy \
    --restart "always" \
    --network=fc-leonardo \
    -p 20080:80 -p 30443:443 \
    -e PROXY_URL='http://leonardo-sbt:8080/' \
    -e PROXY_URL2='http://leonardo-sbt:8080/api' \
    -e PROXY_URL3='http://leonardo-sbt:8080/register' \
    -e CALLBACK_URI='https://local.dsde-dev.broadinstitute.org/oauth2callback' \
    -e LOG_LEVEL='debug' \
    -e SERVER_NAME='local.dsde-dev.broadinstitute.org' \
    -e REMOTE_USER_CLAIM='sub' \
    -e ENABLE_STACKDRIVER='yes' \
    -e FILTER2='AddOutputFilterByType DEFLATE application/json text/plain text/html application/javascript application/x-javascript' \
    us.gcr.io/broad-dsp-gcr-public/openidc-terra-proxy:v0.1.10

    docker cp config/server.crt leonardo-proxy:/etc/ssl/certs/server.crt
    docker cp config/server.key leonardo-proxy:/etc/ssl/private/server.key
    docker cp config/ca-bundle.crt leonardo-proxy:/etc/ssl/certs/ca-bundle.crt
    docker cp config/oauth2.conf leonardo-proxy:/etc/apache2/mods-enabled/oauth2.conf
    docker cp config/site.conf leonardo-proxy:/etc/apache2/sites-available/site.conf

    echo "Starting sqlproxy..."
    docker start sqlproxy
    echo "Starting proxy..."
    docker start leonardo-proxy
    echo "Starting SBT..."
    docker start -ai leonardo-sbt
}
start_server

