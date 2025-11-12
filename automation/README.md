Quickstart: running integration tests locally on Mac/Docker 

## Set Up
**Arguments:** (arguments are positional)

* branch of firecloud-automated-testing
	* Configs branch; defaults to `master`
* Vault auth token
	* Defaults to reading it from the .vault-token via `$(cat ~/.vault-token)`.
* env
	* Environment of your BEE; defaults to `dev`
* service root
	* the name of your local clone of leonardo if not `leonardo`

### Run tests (GCP)

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