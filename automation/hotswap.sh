#!/usr/bin/env bash

#Updates the Leo jar of a fiab to reflect current local code.
#Run using "./automation/hotswap.sh fiab-your-fiab-name" from inside the leo directory

if [ -z "$1" ]
    then
        echo "No arguments supplied. Please provide fiab name as an argument."
        exit 1
fi

FIAB=$1

# Example: /Users/qi/workspace/leonardo/http/target/scala-2.12/http-assembly-0.1-437ee4a9-SNAPSHOT.jar
FULL_LEO_JAR=$(sbt -Dsbt.log.noformat=true "project http" assembly | tail -3 | head -2 | grep -o '/Users[^ ]*')
SHORT_LEO_JAR=$(basename $FULL_LEO_JAR)
NEW_JAR_NAME=$(echo ${SHORT_LEO_JAR}|sed 's/http\-/leonardo\-/g' ) # rename the jar to leonardo-assembly-0.1-437ee4a9-SNAPSHOT.jar so that fiab start script will pick up the right jar file

gcloud compute scp ${FULL_LEO_JAR} ${FIAB}:/tmp --zone=us-central1-a --project broad-dsde-dev

gcloud compute ssh --project broad-dsde-dev --zone us-central1-a ${FIAB} << EOSSH
    sudo docker exec -it firecloud_leonardo-app_1 rm -f /leonardo/*jar
    sudo docker cp /tmp/${SHORT_LEO_JAR} firecloud_leonardo-app_1:/leonardo/${NEW_JAR_NAME}
    sudo docker restart firecloud_leonardo-app_1
    sudo docker restart firecloud_leonardo-proxy_1
EOSSH
