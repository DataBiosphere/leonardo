#!/bin/bash

(cd $(git rev-parse --show-toplevel)/docker/local && mkdir -p etc-jupyter/custom)

mkdir /tmp/welder-data
chmod -R a+rwx /tmp/welder-data

# Substitute templated vars in the notebook config.
pushd $(git rev-parse --show-toplevel) > /dev/null
readonly etc_dir=docker/local/etc-jupyter
readonly nb_config="${etc_dir}/jupyter_notebook_config.py"
cp src/main/resources/jupyter/jupyter_notebook_config.py "${nb_config}"
sed -i 's/$(contentSecurityPolicy)/""/' "${nb_config}"

cp docker/jupyter/custom/*.py "${etc_dir}/custom/"

mkdir -p "${etc_dir}/edit-mode"
cp src/main/resources/jupyter/edit-mode.js "${etc_dir}/edit-mode/main.js"
sed -i 's/\$(googleProject)/"test-project"/ ; s/\$(clusterName)/"test-cluster"/' "${etc_dir}/edit-mode/main.js"

chmod -R a+rwx ${etc_dir}

popd > /dev/null

(sleep 10 && ./setup.sh)&

docker-compose up

