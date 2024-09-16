#!/usr/bin/env bash

# The CloudSQL console simply states "MySQL 5.6" so we may not match the minor version number
MYSQL_VERSION=5.7
start() {

    echo "attempting to remove old $CONTAINER container..."
    docker rm -f $CONTAINER || echo "docker rm failed. $CONTAINER not found."

    # start up mysql
    echo "starting up mysql container..."
    docker run --name $CONTAINER \
               -e MYSQL_ROOT_PASSWORD=leonardo-test \
               -e 'MYSQL_ROOT_HOST=%' \
               -e MYSQL_USER=leonardo-test \
               -e MYSQL_PASSWORD=leonardo-test \
               -e MYSQL_DATABASE=leotestdb \
               -d \
               -p 3311:3306 \
               mysql/mysql-server:$MYSQL_VERSION

    # validate mysql
    echo "running mysql validation..."
    docker run --rm \
               --link $CONTAINER:mysql \
               -v $PWD/docker/sql_validate.sh:/working/sql_validate.sh \
               mysql:$MYSQL_VERSION \
               /working/sql_validate.sh $TARGET

    if [ 0 -eq $? ]; then
        echo "mysql validation succeeded."
    else
        echo "mysql validation failed."
        exit 1
    fi

}

stop() {
    echo "Stopping docker $CONTAINER container..."
    docker stop $CONTAINER || echo "mysql stop failed. container $CONTAINER already stopped."
    docker rm -v $CONTAINER || echo "mysql rm -v failed.  container $CONTAINER already destroyed."
}

if [ ${#@} == 0 ]; then
    echo "Usage: $0 stop|start <service>"
    exit 1
fi

COMMAND=$1
TARGET=${2:-leonardo}
CONTAINER=${3:-leonardo-mysql}

if [ ${#@} == 0 ]; then
    echo "Usage: $0 stop|start"
    exit 1
fi

if [ $COMMAND = "start" ]; then
    start
elif [ $COMMAND = "stop" ]; then
    stop
else
    exit 1
fi
