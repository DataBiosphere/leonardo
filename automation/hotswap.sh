#!/usr/bin/env bash

#Updates the Leo jar of a fiab to reflect current local code.
#Run using "./automation/hotswap.sh fiab-your-fiab-name" from inside the leo directory

if [ -z "$1" ]
    then
        echo "No arguments supplied. Please provide fiab name as an argument."
        exit 1
fi

FIAB=$1

FULL_LEO_JAR=$(sbt assembly | tail -3 | head -2 | grep -o '/Users[^ ]*')
SHORT_LEO_JAR=$(echo ${FULL_LEO_JAR} | grep -oP 'leonardo-assembly[^ ]*')

gcloud compute scp ${FULL_LEO_JAR} ${FIAB}:/tmp --zone=us-central1-a --project broad-dsde-dev

gcloud compute ssh --project broad-dsde-dev --zone us-central1-a ${FIAB} << EOSSH
    OLD_JAR=(sudo docker exec -it firecloud_leonardo-app_1 ls /leonardo/)
    sudo docker exec -it firecloud_leonardo-app_1 rm /leonardo/\${OLD_JAR}
    sudo docker cp /tmp/${SHORT_LEO_JAR} firecloud_leonardo-app_1:/leonardo/
    sudo docker restart firecloud_leonardo-app_1
    sudo docker restart firecloud_leonardo-proxy_1
EOSSH
