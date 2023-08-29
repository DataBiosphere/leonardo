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
BEE_NAME=[Your BEE instance name] ./render-local-env.sh [branch of firecloud-automated-testing] [vault token] [env] [service root]
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
- use classpath and SDK of the leonardoTests (automation) module
- IntelliJ -> Preferences -> Build, Execution, Deployment -> Build Tools -> sbt: Check the two boxes next to `Use sbt shell`
- should be able to right-click-run on the particular test
- If you get an error like `selenium.common.exceptions.SessionNotCreatedException: Message: session not created: This version of ChromeDriver only supports Chrome version 75`,
download the chrome driver from `https://sites.google.com/a/chromium.org/chromedriver/downloads` that has the same version of your local chrome. Update `chromeSettings.chromedriverPath`
in `application.conf` to the new chrome driver that you just downloaded

Note: If the test you're trying to run is annotated with `@DoNotDiscover`, do the following for running the individual test
- Comment out `@DoNotDiscover` of the test you are running
- Have `Spec` extend `NewBillingProjectAndWorkspaceBeforeAndAfterAll` directly or indirectly:
	- If the `Spec` extends `ClusterFixtureSpec`/`RuntimeFixtureSpec`, add `with NewBillingProjectAndWorkspaceBeforeAndAfterAll` to `ClusterFixtureSpec`/`RuntimeFixtureSpec`. 
	- If not, add `with NewBillingProjectAndWorkspaceBeforeAndAfterAll` to the `Spec` directly.

### Developing azure automation tests locally

When running azure automation tests locally against a fresh [bee](https://broadworkbench.atlassian.net/wiki/spaces/IA/pages/104399223/Callisto+Developer+Handbook#Running-in-a-BEE), you will have to perform a few extra steps:

- Change all sections of `application.conf` to have the proper bee/terra URLS. For example, for a bee named `jc-bee-10`:
```
fireCloud {
  baseUrl = "https://firecloud.jc-bee-10.bee.envs-terra.bio/"
  orchApiUrl = "https://firecloudorch.jc-bee-10.bee.envs-terra.bio/"


  orchApiUrl = "https://firecloudorch.jc-bee-10.bee.envs-terra.bio/"
  rawlsApiUrl = "https://rawls.jc-bee-10.bee.envs-terra.bio/"
  samApiUrl = "https://sam.jc-bee-10.bee.envs-terra.bio/"
  thurloeApiUrl = "https://thurloe.jc-bee-10.bee.envs-terra.bio/"

  fireCloudId = "4c717998-7442-11e6-8b77-86f30ca893d3"

  # TODO more Config class nonsense
  tcgaAuthDomain = “TCGA-dbGaP-Authorized”


  gpAllocApiUrl = "https://gpalloc-qa.dsp-techops.broadinstitute.org/api/"

}

leonardo {

  apiUrl = "https://leonardo.jc-bee-10.bee.envs-terra.bio/"

  notebooksServiceAccountEmail = "leonardo-qa@broad-dsde-qa.iam.gserviceaccount.com"

}
```

This above will allow the tests to run against a fresh bee. Then, tests can be run via `sbt  "project automation" "testOnly *[my-file-name]"`

To save time developing, you will likely want to reference the billing project and rawls workspace created in the first run of the tests.
To do so:
- Find the appropriate comment in `beforeAll` within `AzureBillingBeforeAndAfter` to override the billing project
- Find the appropriate comment in `withRawlsWorkspace` to override the workspace

If the test fails with some intermediate resources remaining:
- Be sure to check the `staticTestingMrg` in the [azure portal](https://portal.azure.com/#@azure.dev.envs-terra.bio/resource/subscriptions/f557c728-871d-408c-a28b-eb6b2141a087/resourceGroups/staticTestingMrg/overview) periodically to ensure you are not leaking resources when testing
- It may be helpful to clean up your BEE's leonardo DB since only one runtime can exist per workspace. You can find instructions to get shell mysql access to your BEE in the [leonardo handbook](https://broadworkbench.atlassian.net/wiki/spaces/IA/pages/2839576631/How+to+BEE#Connecting-to-your-BEE%E2%80%99s-databases).
  - Once you have shell access to your BEE's leonardo mysql, run the following *to delete all Leo runtime records*: `DELETE FROM CLUSTER_ERROR WHERE 1=1; DELETE FROM CLUSTER_IMAGE WHERE 1=1; DELETE FROM RUNTIME_CONTROLLED_RESOURCE WHERE 1=1; DELETE FROM CLUSTER WHERE 1=1; DELETE FROM RUNTIME_CONFIG WHERE 1=1; DELETE FROM PERSISTENT_DISK WHERE 1=1;`
