#!/usr/bin/env bash

start() {
    # check if mysql is running
    RUNNING=$(docker inspect -f {{.State.Running}} $CONTAINER || echo "false")

    # mysql set-up
    if ! $RUNNING; then
        # force remove mysql in case it is stopped
        echo "attempting to remove old $CONTAINER container..."
        docker rm -f $CONTAINER || echo "docker rm failed. nothing to rm."

        # start up mysql
        echo "starting up mysql container..."
        docker run --name $CONTAINER -e MYSQL_ROOT_PASSWORD=${SERVICE}-test -e MYSQL_USER=${SERVICE}-test -e MYSQL_PASSWORD=${SERVICE}-test -e MYSQL_DATABASE=leotestdb -d -p 3310:3306 mysql/mysql-server:5.7.15

        # validate mysql
        echo "running mysql validation..."
        docker run --rm --link mysql:mysql -v $PWD/docker/sql_validate.sh:/working/sql_validate.sh broadinstitute/dsde-toolbox /working/sql_validate.sh ${SERVICE}
        if [ 0 -eq $? ]; then
            echo "mysql validation succeeded."
        else
            echo "mysql validation failed."
            exit 1
        fi
    fi
}

stop() {
    echo "Stopping docker $CONTAINER container..."
    docker stop $CONTAINER || echo "mysql stop failed. container already stopped."
    docker rm -v $CONTAINER || echo "mysql rm -v failed.  container already destroyed."
}

CONTAINER=mysql-$(date +%s)
COMMAND=$1
SERVICE=$2
if [ $COMMAND = "start" ]; then
    start
elif [ $COMMAND = "stop" ]; then
    stop
else
    exit 1
fi
