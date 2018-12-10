#!/usr/bin/env bash


#FIAB=$1

LEO_JAR=$(sbt assembly | tail -3 | head -2| grep -oP "/Users)

echo ${LEO_JAR}

# gcloud compute scp ${LEO_JAR} ${FIAB}:/tmp --zone=us-central1-a --project broad-dsde-dev
