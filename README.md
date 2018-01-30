[![Build Status](https://travis-ci.org/broadinstitute/leonardo.svg?branch=develop)](https://travis-ci.org/broadinstitute/leonardo) [![Coverage Status](https://coveralls.io/repos/github/broadinstitute/leonardo/badge.svg?branch=develop)](https://coveralls.io/github/broadinstitute/leonardo?branch=develop)

# Leonardo

Leo provisions Spark clusters through [Google Dataproc](https://cloud.google.com/dataproc/) and installs [Jupyter notebooks](http://jupyter.org/) and [Hail](https://hail.is/) on them. It can also proxy end-user connections to the Jupyter interface in order to provide authorization for particular users.

For more information and an overview, see the [wiki](https://github.com/broadinstitute/leonardo/wiki).

Swagger API documentation: https://notebooks.firecloud.org/

## Project status
This project is under active development. It is not yet ready for independent production deployment. See the [roadmap](https://github.com/DataBiosphere/leonardo/wiki#roadmap) section of the wiki for details.

## Configurability

Documentation on how to configure Leo is Coming Soon™. Until then, a brief overview: there are two points at which Leonardo is pluggable.

### Authorization provider

Leo provides two modes of authorization out of the box:
1. By whitelist
2. Through [Sam](github.com/broadinstitute/sam), the Workbench IAM service

Users wanting to roll their own authorization mechanism can do so by subclassing `LeoAuthProvider` and setting up the Leo configuration file appropriately.

### Service account provider

There are (up to) three service accounts used in the process of spinning up a notebook cluster:

1. The Leo service account itself, used to _make_ the call to Google Dataproc
2. The service account _passed_ to [dataproc clusters create](https://cloud.google.com/sdk/gcloud/reference/dataproc/clusters/create) via the `--service-account` parameter, whose credentials will be used to set up the instance and localized into the [GCE metadata server](https://cloud.google.com/compute/docs/storing-retrieving-metadata)
3. The service account that will be localized into the user environment and returned when any application asks [for application default credentials](https://developers.google.com/identity/protocols/application-default-credentials).

Currently, Leo uses its own SA for #1, and the same per-user project-specific SA for #2 and #3, which it fetches from [Sam](github.com/broadinstitute/sam). Users wanting to roll their own service account provision mechanism by subclassing `ServiceAccountProvider` and setting up the Leo configuration file appropriately.

## Building and running Leonardo
Clone the repo.
```
$ git clone https://github.com/broadinstitute/leonardo.git
$ cd leonardo
```
Ensure docker is running. Spin up MySQL locally:
```
$ ./docker/run-mysql.sh start leonardo
```
Build Leonardo and run tests.
```
export SBT_OPTS="-Xmx2G -Xms1G -Dmysql.host=localhost -Dmysql.port=3311"
sbt clean compile test
```
Once you're done, tear down MySQL.
```
./docker/run-mysql.sh stop leonardo
```

## Building Leonardo docker image

To install git-secrets
```$xslt
brew install git-secrets
```
To ensure git hooks are run
```$xslt
cp -r hooks/ .git/hooks/
chmod 755 .git/hooks/apply-git-secrets.sh
```

To build jar, leonardo docker image, and leonardo-notebooks docker image
```
./docker/build.sh jar -d build
```

To build jar, leonardo docker image, and leonardo-notebooks docker image 
and push to repos `broadinstitute/leonardo` and `broadinstitute/leonardo-notebooks` 
tagged with git hash
```
./docker/build.sh jar -d push
```

To build the leonardo-notebooks docker image with a given tag
````
bash ./jupyter-docker/build.sh build <TAG NAME>
````

To push the leonardo-notebooks docker image you built
to repo `broadinstitute/leonardo-notebooks`

````
bash ./jupyter-docker/build.sh push <TAG NAME>
````
