#!/bin/bash
# Parameters
time=$1

echo $time


cp test-reports/*.xml

grep "<testcase" /test-reports/TEST-org.broadinstitute.dsde.workbench.NotebooksCanaryTest.xml | grep "time"


      echo "[{\"eventType\":\"NotebooksCanaryTestProd\",\"type\":\"Cluster\",\"timeToComplete (sec)\":\"$time\"}]" > canary_events.json
      cat canary_events.json | gzip -c | curl --data-binary @- -X POST -H "Content-Type: application/json" -H "X-Insert-Key: $newRelicKey" -H "Content-Encoding: gzip" https://insights-collector.newrelic.com/v1/accounts/1862859/events

exit 0