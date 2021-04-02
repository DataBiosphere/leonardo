[![Build Status](https://github.com/DataBiosphere/leonardo/workflows/Unit%20tests/badge.svg)](https://github.com/DataBiosphere/leonardo/actions) [![codecov](https://codecov.io/gh/DataBiosphere/leonardo/branch/develop/graph/badge.svg)](https://codecov.io/gh/DataBiosphere/leonardo)

# Leonardo

Leonardo serves as a way to launch compute within the terra security boundary. It does so via multiple different cloud hardware virtualization mechanisms, currently leveraging only the Google cloud platform.

Leonardo supports launching the following 'hardware' for compute:
- Spark clusters through [Google Dataproc](https://cloud.google.com/dataproc/)
- Virtual machines through [Google Compute Engine](https://cloud.google.com/compute)
- Kubernetes 'apps' through [Google Kubernetes Engine](https://cloud.google.com/kubernetes-engine)

Currently, leonardo supports the launching of custom docker images in virtual machines and dataproc. It also supports launching applications in kubernetes, with a spotlight on galaxy. 

- For more information on APIs, see [swagger](http/src/main/resources/swagger/api-docs.yaml).
- For more information on custom docker images, see the [terra-docker repo](https://github.com/DataBiosphere/terra-docker)
- For more information on applications we support in kubernetes, see the [terra-apps repo](https://github.com/DataBiosphere/terra-app)
- For more information on galaxy, see the [galaxy project](https://github.com/galaxyproject)

It is recommended to consume these APIs and functionality via the [Terra UI](https://terra.bio/) 

We use JIRA instead of the issues page on Github. If you would like to see what we are working you can visit our [active sprint](https://broadworkbench.atlassian.net/secure/RapidBoard.jspa?rapidView=35&projectKey=IA) or our [backlog](https://broadworkbench.atlassian.net/secure/RapidBoard.jspa?rapidView=35&projectKey=IA&view=planning&selectedIssue=IA-1753&epics=visible&issueLimit=100&selectedEpic=IA-1715) on JIRA. You will need to set-up an account to access, but it is open to the public.

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
$ git clone https://github.com/DataBiosphere/leonardo.git 
$ cd leonardo
```

### Run Leonardo unit tests

Leonardo requires Java 8 due to a dependency on Java's DNS SPI functionality. This feature is removed in Java 9 and above.

Ensure docker is running. Spin up MySQL locally:
```
$ ./docker/run-mysql.sh start leonardo  
```

Note, if you see error like
```
Warning: Using a password on the command line interface can be insecure.
ERROR 2003 (HY000): Can't connect to MySQL server on 'mysql' (113)
Warning: Using a password on the command line interface can be insecure.
ERROR 2003 (HY000): Can't connect to MySQL server on 'mysql' (113)
Warning: Using a password on the command line interface can be insecure.
ERROR 2003 (HY000): Can't connect to MySQL server on 'mysql' (113)
```
Run `docker system prune -a`

Build Leonardo and run all unit tests.
```
export SBT_OPTS="-Xmx2G -Xms1G -Dmysql.host=localhost -Dmysql.port=3311 -Duser.timezone=UTC"
sbt clean compile "project http" test
```
You can also run a particular test suite, e.g.
```
sbt "testOnly *LeoAuthProviderHelperSpec"
```
or a particular test within a suite, e.g.
```
sbt "testOnly org.broadinstitute.dsde.workbench.leonardo.runtimes.RuntimeCreationDiskSpec -- -z "create runtime and attach a persistent disk""
```
where `map` is a substring within the test name.

Once you're done, tear down MySQL.
```
./docker/run-mysql.sh stop leonardo
```

Do `docker restart leonardo-mysql` if you see `java.sql.SQLNonTransientConnectionException: Too many connections` error

* Running tests against FIAB
Checking FIAB mysql (fina password in /etc/leonardo.conf in firecloud_leonardo-app_1 container)
```bash
docker exec -it firecloud_leonardo-mysql_1 bash
root@2f5efbd4f138:/# mysql -u leonardo -p
```

## Run scalafmt
Learn more about [scalafmt](https://scalameta.org/scalafmt/docs/installation.html)
- `sbt scalafmtAll`

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

