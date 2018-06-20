# Create a container that will configure the front-end and serve from a static directory.
#
#  Example
#
#    docker run
#     -e "FILTER_TO_CURRENT_USER=false" \
#     -e "DPLOY_ENVIRONMENT=local" \
#     -e "OAUTH_CLIENT_ID=my-client-id" \
#     -e "DEFAULT_PROJECT=my-project" \
#     -e "DISABLE_PROJECT_ENTRY=true" \
#     -e "STARTUP_SCRIPT_URI=" \
#     -e "EXTRA_GOOGLE_SCOPES=" \
#     gcr.io/leonardo-service/leonardo-ui


FROM nginx:latest

RUN apt-get update \
    && apt-get install -y jq \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY build/ /usr/share/nginx/html/
COPY write_config.sh /usr/bin/write_config.sh

CMD /bin/bash -c "write_config.sh /usr/share/nginx/html/config.json && nginx -g 'daemon off;'"
