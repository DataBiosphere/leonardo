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

## Java client library
for sbt:

```libraryDependencies += "org.broadinstitute.dsde.workbench" %% "leonardo-client" % "0.1-<git hash>"```

where ```<git hash>``` is the first 7 characters of the commit hash of the HEAD of develop

Example Scala Usage:
```
import org.broadinstitute.dsde.workbench.client.leonardo.api.RuntimesApi
import org.broadinstitute.dsde.workbench.client.leonardo.ApiClient
import org.broadinstitute.dsde.workbench.client.leonardo.model.GetRuntimeResponse

class LeonardoClient(leonardoBasePath: String) {
  private def leonardoApi(accessToken: String): RuntimesApi = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(leonardoBasePath)
    new RuntimesApi(apiClient)
  }

  def getAzureRuntimeDetails(token: String, workspaceId: String, runtimeName: String): GetRuntimeResponse = {
    val leonardoApi = leonardoApi(token)
    leonardoApi.getAzureRuntime(workspaceId, runtimeName)
  }
}

```

## Building and running Leonardo
Clone the repo.
```
$ git clone https://github.com/DataBiosphere/leonardo.git
$ cd leonardo
```

### Running Leo Locally

#### Setup
Install gcloud using Homebrew:
```
brew install --cask google-cloud-sdk
```
Update the helm-scala-sdk submodule:
```
git submodule init && git submodule update
```

#### VPN
You must be connected to the VPN if working remotely.

#### Dependencies
Leo needs a copy of the Go Helm library and secrets, files, and env vars stored in k8s.

To build the Go Helm library and get k8s resources, run:
```
./local/depends.sh -y
```
To only build the Go Helm library, run:
```
./local/depends.sh helm
```
To only get k8s resources, run:
```
./local/depends.sh configs
```

#### Overrides
By adding entries to `./local/overrides.env`, you can override the value of any variable from k8s.

#### Unsetting
By adding entries to `./local/unset.env`, you can remove variables from k8s. Applied after retrieving
variables from k8s and before applying overrides.

#### Host alias
If you haven't already, add `127.0.0.1       local.dsde-dev.broadinstitute.org` to `/etc/hosts`:
```
sudo sh -c "echo '127.0.0.1       local.dsde-dev.broadinstitute.org' >> /etc/hosts"
```

#### Run proxies
To run the CloudSQL and Apache proxies, run:
```
./local/proxies.sh start
```
You can also stop them:
```
./local/proxies.sh stop
```
Or restart them:
```
./local/proxies.sh restart
```

#### Run Leo
Export required env vars as created by `./local/depends.sh`:
```
. ./http/src/main/resources/rendered/sbt.env.sh
```
Call the sbt `http/run` target:
```
sbt http/run
```
Or start an sbt shell and go from there:
```
sbt
```

#### Cleanup
When you're done, stop sbt (e.g. using Ctrl+C) and stop the proxies:
```
./local/proxies.sh stop
```

### Run Leonardo unit tests

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
Run `docker system prune -a`. If the error persists, try restarting your laptop.

Build Leonardo and run all unit tests.
```
export JAVA_OPTS="-Dheadless=false -Duser.timezone=UTC -Xmx4g -Xss2M -Xms4G"
sbt clean compile "project http" test
```
You can also run a particular test suite, e.g.
```
sbt "testOnly *LeoAuthProviderHelperSpec"
```
or a particular test within a suite, e.g.

```
sbt "testOnly *LeoPubsubMessageSubscriberSpec -- -z "handle Azure StopRuntimeMessage and stop runtime""
```
where `map` is a substring within the test name.

If you made a change to the leonardo Db by adding a changeset xml file, and then adding that file path to the changelog
file, you have to set `initWithLiquibase = true` in the leonardo.conf file for these changes to be reflected in the unit
tests. Once youare done testing your changes, make sure to switch it back to `initWithLiquibase = false`, as this can do
some damage if you are running local Leo against Dev!

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

To build jar and leonardo docker image
```
./docker/build.sh jar -d build
```

To build jar and leonardo docker image
and push to repos `broadinstitute/leonardo`
tagged with git hash
```
./docker/build.sh jar -d push
```


## Github actions

Leonardo has custom runners for github actions, as they require more than the default 30GB provisioned by the `ubuntu-latest` Github runners

There are 3 nodes, you can view them here: https://github.com/DataBiosphere/leonardo/settings/actions/runners. They have 100GB currently. Devops can be contacted to increase the size if needed, but we only need ~60GB at time of writing.