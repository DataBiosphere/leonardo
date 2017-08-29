#!/usr/bin/env bash

# The CloudSQL console simply states "MySQL 5.6" so we may not match the minor version number
MYSQL_VERSION=5.6
start() {
    echo "starting up mysql container..."
    docker run --name $CONTAINER -e MYSQL_ROOT_PASSWORD=rawls-test -e MYSQL_USER=rawls-test -e MYSQL_PASSWORD=rawls-test -e MYSQL_DATABASE=testdb -d -p 3310:3306 mysql/mysql-server:$MYSQL_VERSION

    # validate mysql
    echo "running mysql validation..."
    docker run --rm --link $CONTAINER:mysql -v $PWD/docker/sql_validate.sh:/working/sql_validate.sh broadinstitute/dsde-toolbox /working/sql_validate.sh
    if [ 0 -eq $? ]; then
        echo "mysql validation succeeded."
    else
        echo "mysql validation failed."
        exit 1
    fi
}

stop() {
    echo "Stopping docker $CONTAINER container..."
    docker stop $CONTAINER || echo "mysql stop failed. container already stopped."
    docker rm -v $CONTAINER || echo "mysql rm -v failed.  container already destroyed."
}

CONTAINER=mysql-$(date +%s)
COMMAND=$1

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
