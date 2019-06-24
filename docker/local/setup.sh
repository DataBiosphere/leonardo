#!/bin/bash

docker exec leo-dev-jupyter jupyter nbextension install /etc/jupyter/edit-mode/ --user
docker exec leo-dev-jupyter jupyter nbextension enable edit-mode/main

curl -X POST -H "Content-Type: application/json" http://localhost:8080/storageLinks -d "$(cat << EOF
{
  "localBaseDirectory": "foo",
  "localSafeModeBaseDirectory": "foo-safe",
  "cloudStorageDirectory": "gs://fc-0bf6534f-a9cb-4d58-b311-c6a1c65a359f/welder-test",
  "pattern": ""
}
EOF
)"

curl -X POST -H "Content-Type: application/json" http://localhost:8080/objects -d "$(cat << EOF
{
  "action": "localize",
  "entries": [{
    "sourceUri": "gs://fc-0bf6534f-a9cb-4d58-b311-c6a1c65a359f/welder-test/bar.ipynb",
    "localDestinationPath": "foo/bar.ipynb"
  }]
}
EOF
)"
