#!/bin/bash
      echo "[{\"eventType\":\"NotebooksCanaryTestProd\",\"type\":\"Cluster\",\"status\": \"$clusterStatus\",\"timeToComplete (sec)\":\"$timer\"}]" > canary_events.json
      cat canary_events.json | gzip -c | curl --data-binary @- -X POST -H "Content-Type: application/json" -H "X-Insert-Key: $newRelicKey" -H "Content-Encoding: gzip" https://insights-collector.newrelic.com/v1/accounts/1862859/events

exit 0
