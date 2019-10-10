#!/usr/bin/env bash

#Updates the Leo jar of a fiab to reflect current local code.
#Run using "./automation/hotswap_perf.sh" from inside the leo directory


# Example: /Users/qi/workspace/leonardo/http/target/scala-2.12/http-assembly-0.1-437ee4a9-SNAPSHOT.jar
FULL_LEO_JAR=$(sbt -Dsbt.log.noformat=true "project http" assembly | tail -3 | head -2 | grep -o '/Users[^ ]*')
SHORT_LEO_JAR=$(basename $FULL_LEO_JAR)
NEW_JAR_NAME=$(echo ${SHORT_LEO_JAR}|sed 's/http\-/leonardo\-/g' ) # rename the jar to leonardo-assembly-0.1-437ee4a9-SNAPSHOT.jar so that fiab start script will pick up the right jar file

swapJar() {
  HOSTNAME="gce-leonardo-perf$1"
  echo "swapping $HOSTNAME"
  gcloud compute scp ${FULL_LEO_JAR} ${HOSTNAME}:/tmp --zone=us-central1-a --project broad-dsde-perf

gcloud compute ssh --project broad-dsde-perf --zone us-central1-a ${HOSTNAME} << EOSSH
    sudo docker exec -it leonardo_app_1 rm -f /leonardo/*SNAPSHOT.jar
    sudo docker cp /tmp/${SHORT_LEO_JAR} leonardo_app_1:/leonardo/${NEW_JAR_NAME}
    sudo docker restart leonardo_app_1
    sudo docker restart leonardo_proxy_1
EOSSH
}

for i in 401 501 502
do
  swapJar $i
done