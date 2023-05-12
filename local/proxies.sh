#!/bin/bash

stop() {
    echo "Stopping proxies..."
    docker rm -f leonardo-proxy sqlproxy
}

SECRETS_DIR="$(git rev-parse --show-toplevel)/http/src/main/resources/rendered"

start() {
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
    -p 20080:80 -p 443:443 \
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

dbconnect() {
    # Check for mysql
    if ! command -v mysql &> /dev/null; then
        echo "ERROR: mysql not found. Install with \`brew install mysql\`."
        exit 1
    fi

    # Check for CloudSQL proxy container.
    local _csp_search=$(docker container ls --format "{{.Names}}" | grep leonardo-proxy)
    if [ "${_csp_search}" = "" ]; then
        echo "ERROR: The CloudSQL proxy isn't running. Use \`${0} start\` to start the proxies."
        exit 1
    fi

    # Check for env vars
    if [ -z "${DB_USER}" ]; then
        echo "INFO: Missing required env var DB_USER. Using 'leonardo'."
        echo "See README.md for how to render (\"Dependencies\") and source env vars (\"Run Leo\")."
        export DB_USER=leonardo
    elif [ -z "${DB_PASSWORD}" ]; then
        echo "ERROR: Missing required env var DB_PASSWORD."
        echo "See README.md for how to render (\"Dependencies\") and source env vars (\"Run Leo\")."
        exit 1
    fi

    mysql -u ${DB_USER} -p${DB_PASSWORD} --host 127.0.0.1 --port 3306
}

HELP_TEXT=$(cat <<EOF
 ${0} [command] [flags]
 Manage the CloudSQL and Apache proxies.

 Commands:
   start:     Start the CloudSQL and Apache proxies.
   stop:      Stop the CloudSQL and Apache proxies.
   restart:   Restart the CloudSQL and Apache proxies.
   dbconnect: Connect to the CloudSQL proxied MySQL db.
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
        dbconnect)
            echo ${COMMAND}
            if [ ! -z ${COMMAND} ]; then
                echo "Error: You can specify up to one action at a time."
                print_help
            fi
            COMMAND="dbconnect"
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
    dbconnect)
        dbconnect
        ;;
esac
