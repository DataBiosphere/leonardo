#!/bin/bash

stop() {
    echo "Stopping proxies..."
    docker rm -f leonardo-proxy sqlproxy
    echo "Deleting network..."
    docker network rm fc-leonardo
    pkill -P $$
}

SECRETS_DIR="$(git rev-parse --show-toplevel)/http/src/main/resources/rendered"

start() {
    docker network create fc-leonardo

    echo "Creating Google sqlproxy container..."
    docker create --name sqlproxy \
    --restart "always" \
    -p 3306:3306 \
    --env-file="${SECRETS_DIR}/sqlproxy.env" \
    broadinstitute/cloudsqlproxy:1.11_20180808

    echo "Copying files to CloudSQL proxy container..."
    docker cp ${SECRETS_DIR}/leonardo-account.json sqlproxy:/etc/sqlproxy-service-account.json

    echo "Creating proxy..."
    docker create --name leonardo-proxy \
    --restart "always" \
    --env-file="${SECRETS_DIR}/proxy.env" \
    -p 20080:80 -p 30443:443 \
    us.gcr.io/broad-dsp-gcr-public/openidc-terra-proxy:v0.1.17

    echo "Copying files to proxy container..."
    docker cp ${SECRETS_DIR}/server.crt leonardo-proxy:/etc/ssl/certs/server.crt
    docker cp ${SECRETS_DIR}/server.key leonardo-proxy:/etc/ssl/private/server.key
    docker cp ${SECRETS_DIR}/oauth2.conf leonardo-proxy:/etc/apache2/mods-enabled/oauth2.conf
    docker cp ${SECRETS_DIR}/site.conf leonardo-proxy:/etc/apache2/sites-available/site.conf

    echo "Starting sqlproxy..."
    docker start sqlproxy
    echo "Starting proxy..."
    docker start leonardo-proxy
}

HELP_TEXT=$(cat <<EOF
 ${0} [command] [flags]
 Manage the CloudSQL and Apache proxies.

 Commands:
   start:   Start the CloudSQL and Apache proxies.
   stop:    Stop the CloudSQL and Apache proxies.
   restart: Restart the CloudSQL and Apache proxies.
 Flags:
   -h | --help: Print this help message.
EOF
)

print_help() {
    echo -e "${HELP_TEXT}"
    exit 0
}

# If no command or flags are specified, ask about everything.
if [ -z "${1}" ]; then
    echo "ERROR: No command or flag specified."
    print_help
fi

while [ "${1}" != "" ]; do
    case ${1} in
        start)
            if [ ! -z ${COMMAND} ]; then
                echo "Error: You can specify up to one action at a time."
                print_help
            fi
            COMMAND="start"
            ;;
        stop)
            if [ ! -z ${COMMAND} ]; then
                echo "Error: You can specify up to one action at a time."
                print_help
            fi
            COMMAND="stop"
            ;;
        restart)
            echo ${COMMAND}
            if [ ! -z ${COMMAND} ]; then
                echo "Error: You can specify up to one action at a time."
                print_help
            fi
            COMMAND="restart"
            ;;
        -h|--help)
            print_help
            ;;
        *)
            echo "Unrecognized argument '${1}'."
            echo "run '${0} -h' to see available arguments."
            exit 1
            ;;
    esac
    shift
done

case ${COMMAND} in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        stop
        start
        ;;
esac
