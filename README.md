[![Build Status](https://github.com/DataBiosphere/leonardo/workflows/Unit%20tests/badge.svg)](https://github.com/DataBiosphere/leonardo/actions) [![codecov](https://codecov.io/gh/DataBiosphere/leonardo/branch/develop/graph/badge.svg)](https://codecov.io/gh/DataBiosphere/leonardo)

# Leonardo

Leonardo serves as a way to launch compute within the Terra security boundary. It does so via multiple different cloud hardware virtualization mechanisms, currently leveraging only the Google Cloud Platform.

Leonardo supports launching the following services for compute:
- Spark clusters through [Google Dataproc](https://cloud.google.com/dataproc/)
- Virtual machines through [Google Compute Engine](https://cloud.google.com/compute)
- Kubernetes 'apps' through [Google Kubernetes Engine](https://cloud.google.com/kubernetes-engine)

Currently, leonardo supports the launching of custom docker images for Jupyter and Rstudio in virtual machines and Dataproc. It also supports launching applications in Kubernetes, with a spotlight on Galaxy. 

- For more information on APIs, see [swagger](https://notebooks.firecloud.org/)
- For more information on custom docker images, see the [terra-docker repo](https://github.com/DataBiosphere/terra-docker)
- For more information on applications we support in Kubernetes, see the [terra-apps repo](https://github.com/DataBiosphere/terra-app)
- For more information on Galaxy, see the [Galaxy Project](https://github.com/galaxyproject)

It is recommended to consume these APIs and functionality via the [Terra UI](https://terra.bio/) 

We use JIRA instead of the issues page on Github. If you would like to see what we are working you can visit our [active sprint](https://broadworkbench.atlassian.net/secure/RapidBoard.jspa?rapidView=35&projectKey=IA) or our [backlog](https://broadworkbench.atlassian.net/secure/RapidBoard.jspa?rapidView=35&projectKey=IA&view=planning&selectedIssue=IA-1753&epics=visible&issueLimit=100&selectedEpic=IA-1715) on JIRA. You will need to set-up an account to access, but it is open to the public.

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

