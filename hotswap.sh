#!/bin/bash

new_jar=$(grep -Po '/Users[^ ]+' <<<$(sbt assembly | tail -3))
echo $new_jar
out=$(gcloud compute scp $(new_jar::-1) $1:/tmp --zone=us-central1-a)
echo $out
#gcloud compute ssh --project broad-dsde-dev --zone us-central1-a $1
#old_jar = $(sudo docker exec -it firecloud_leonardo-app_1 ls /leonardo/)
#echo $old_jar

