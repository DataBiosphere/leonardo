#!/usr/bin/env bash

# Updates the Leo jar of a FIAB to reflect current local code.
# Run using "./automation/hotswap.sh <your FIAB name>" at the root of the leonardo repo clone

set -eu

# see https://stackoverflow.com/questions/3601515/how-to-check-if-a-variable-is-set-in-bash
if [ -z ${1+x} ]; then
      echo "No arguments supplied. Please provide FIAB name as an argument."
      exit 1
fi

FIAB=$1

printf "Generating the Leo jar. This will fail silently here if you have any compilation errors, or print a success message otherwise\n\n"
# Example: /Users/qi/workspace/leonardo/http/target/scala-2.12/http-assembly-0.1-437ee4a9-SNAPSHOT.jar
LEO_JAR_PATH=$(sbt -Dsbt.log.noformat=true "project http" assembly | tail -3 | head -2 | grep -o '/Users[^ ]*')

printf "Jar successfully generated."

LEO_JAR_NAME=$(basename $LEO_JAR_PATH)

# Rename the jar to leonardo-assembly-0.1-437ee4a9-SNAPSHOT.jar so the fiab-start Jenkins job picks up the right jar file
NEW_LEO_JAR_NAME=$(echo ${LEO_JAR_NAME}|sed 's/http\-/leonardo\-/g' )

printf "\n\nCopying ${LEO_JAR_PATH} to /tmp on FIAB '${FIAB}'...\n\n"
gcloud compute scp ${LEO_JAR_PATH} ${FIAB}:/tmp --zone=us-central1-a --project broad-dsde-dev

printf "\n\nRemoving the old Leo jars on the FIAB...\n\n"
# For some reason including the rm command to be executed on the FIAB within the EOSSH block below doesn't work
gcloud compute ssh --project broad-dsde-dev --zone us-central1-a ${FIAB} -- 'sudo docker exec -it firecloud_leonardo-app_1 sh -c "rm -f /leonardo/*jar"'

printf "\n\nCopying the Leo jar to the right location on the FIAB, and restarting the Leo app and proxy...\n\n"
gcloud compute ssh --project broad-dsde-dev --zone us-central1-a ${FIAB} << EOSSH
    sudo docker cp /tmp/${LEO_JAR_NAME} firecloud_leonardo-app_1:/leonardo/${NEW_LEO_JAR_NAME}
    sudo docker restart firecloud_leonardo-app_1
    sudo docker restart firecloud_leonardo-proxy_1
EOSSH
