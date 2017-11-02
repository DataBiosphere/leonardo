#!/usr/bin/env bash

SBT_CMD=${1-"testOnly *Leonardo*"}
echo $SBT_CMD

set -o pipefail

sbt -Djsse.enableSNIExtension=false -Dheadless=true "${SBT_CMD}" | tee testout.txt
TEST_EXIT_CODE=$?
sbt clean

cat testout.txt | sed -n -e '/Run completed/,$p' | sed $'s,\x1b\\[[0-9;]*[a-zA-Z],,g' > output/testsummary.txt

if [[ $SLACK_API_TOKEN ]] && [[ $SLACK_CHANNEL ]]; then
    if [[ $TEST_EXIT_CODE != 0 ]]; then COLOR=danger; else COLOR=good; fi
    BUILD_NAME=fiab-test-${BUILD_NUMBER}
    ATTACHMENTS='[{"color":"'"${COLOR}"'","text":"'"${BUILD_NAME}"'"]'
    
    curl -F channels=#${SLACK_CHANNEL} -F token=${SLACK_API_TOKEN} -F attachments=${ATTACHMENTS} https://slack.com/api/chat.postMessage
    curl -F file=@output/testsummary.txt -F channels=#${SLACK_CHANNEL} -F token=${SLACK_API_TOKEN} -F filename=${BUILD_NAME} https://slack.com/api/files.upload
fi

if [[ $TEST_EXIT_CODE != 0 ]]; then exit $TEST_EXIT_CODE; fi
