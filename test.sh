#!/bin/bash

sbt assembly
gcloud compute scp /Users/akarukap/Documents/firecloud/leonardo/target/scala-2.12/leonardo-assembly-0.1-5f9b556-SNAPSHOT.jar fiab-akarukap-secure-glider:/tmp --zone=us-central1-a
gcloud compute ssh --project broad-dsde-dev --zone us-central1-a fiab-akarukap-secure-glider | sudo docker exec -it firecloud_leonardo-app_1 ls /leonardo/ | sudo docker exec -it firecloud_leonardo-app_1 rm /leonardo/leonardo-assembly-0.1-5f9b556-SNAPSHOT.jar | sudo docker cp /tmp/leonardo-assembly-0.1-5f9b556-SNAPSHOT.jar firecloud_leonardo-app_1:/leonardo/ | sudo docker restart firecloud_leonardo-app_1 | sudo docker restart firecloud_leonardo-proxy_1
