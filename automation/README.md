Quickstart: running swintegration tests locally on Mac/Docker 

## Running in docker

See [firecloud-automated-testing](https://github.com/broadinstitute/firecloud-automated-testing).


## Running directly (with real chrome)

### Set Up

```
brew install chromedriver
```

Note: Leonardo integration tests are not currently web-based but may fail due to dependencies without chromedriver

```bash
./render-local-env.sh [branch of firecloud-automated-testing] [vault token] [env] [service root]
```

**Arguments:** (arguments are positional)

* branch of firecloud-automated-testing
    * Configs branch; defaults to `master`
* Vault auth token
	* Defaults to reading it from the .vault-token via `$(cat ~/.vault-token)`.
* env
	* Environment of your FiaB; defaults to `dev`
* service root
    * the name of your local clone of leonardo if not `leonardo`
	
### Run tests

`sbt -Djsse.enableSNIExtension=false -Dheadless=false "project automation" test`

IntelliJ
- Edit Configurations -> Defaults -> ScalaTest
- set VM parameters `-Djsse.enableSNIExtension=false -Dheadless=false`
- set Working dir to local dir
- use classpath and SDK of the leonardoTests module
- should be able to right-click-run
- If you get an error like `selenium.common.exceptions.SessionNotCreatedException: Message: session not created: This version of ChromeDriver only supports Chrome version 75`,
download the chrome driver from `https://sites.google.com/a/chromium.org/chromedriver/downloads` that has the same version of your local chrome. Update `chromeSettings.chromedriverPath`
in `application.conf` to the new chrome driver that you just downloaded

Note: If the test you're trying to run is annotated with `@DoNotDiscover`, do the following for running the individual test
- Comment out `@DoNotDiscover` of the test you are running
- add `with GPAllocBeforeAndAfterAll` to `ClusterFixtureSpec`

### Developing locally

If you are developing a test that uses ClusterFixture to re-use the same cluster between tests, you can speed up development significantly by reusing the same cluster between runs:
- running the tests once with the lines `deleteRonCluster() 
    unclaimBillingProject()` commented out in the function `afterAll()` within the file `ClusterFixtureSpec.scala`
- adding these lines to the Spec you are working in, where the project and cluster name are the project and cluster generated in the previous test 

```
debug = true
mockedCluster = mockCluster("[GOOGLE_PROJECT]","[CLUSTER_NAME]")
```
