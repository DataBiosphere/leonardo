#!/usr/bin/env bash

# Updates the Leo jar of a fiab to reflect current local code.
# Run using "./automation/hotswap.sh fiab-your-fiab-name" from inside the leo directory

if [ -z "$1" ]
    then
        echo "No arguments supplied. Please provide FIAB name as an argument."
        exit 1
fi

FIAB=$1

# Example: /Users/qi/workspace/leonardo/http/target/scala-2.12/http-assembly-0.1-437ee4a9-SNAPSHOT.jar
LEO_JAR_PATH=$(sbt -Dsbt.log.noformat=true "project http" assembly | tail -3 | head -2 | grep -o '/Users[^ ]*')
LEO_JAR_NAME=$(basename $LEO_JAR_PATH)
# Rename the jar to leonardo-assembly-0.1-437ee4a9-SNAPSHOT.jar so that the fiab-start Jenkins job will pick up the right jar file
NEW_LEO_JAR_NAME=$(echo ${LEO_JAR_NAME}|sed 's/http\-/leonardo\-/g' )

gcloud compute scp ${LEO_JAR_PATH} ${FIAB}:/tmp --zone=us-central1-a --project broad-dsde-dev

gcloud compute ssh --project broad-dsde-dev --zone us-central1-a ${FIAB} << EOSSH
    sudo docker exec -it firecloud_leonardo-app_1 sh -c "rm -f /leonardo/*jar"
    sudo docker cp /tmp/${LEO_JAR_NAME} firecloud_leonardo-app_1:/leonardo/${NEW_LEO_JAR_NAME}
    sudo docker restart firecloud_leonardo-app_1
    sudo docker restart firecloud_leonardo-proxy_1
EOSSH
