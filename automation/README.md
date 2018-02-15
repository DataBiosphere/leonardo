Quickstart: running swintegration tests locally on Mac/Docker 

## Running in docker

See [firecloud-automated-testing](https://github.com/broadinstitute/firecloud-automated-testing).


## Running directly (with real chrome)

### Set Up

```
brew install chromedriver
```

Note: Leonardo integration tests are not currently web-based but may fail due to dependencies without chromedriver

Render configs:
```bash
./render-local-env.sh [branch of firecloud-automated-testing] [working dir] [vault token] [env]
```

**Arguments:** (arguments are positional)

* branch of firecloud-automated-testing
    * Configs branch; defaults to `master`
* Working directory
	* Defaults to `$PWD`.
* Vault auth token
	* Defaults to reading it from the .vault-token via `$(cat ~/.vault-token)`.
* env
	* Environment of your FiaB; defaults to `dev`
	
##### Using a local UI

Set `LOCAL_UI=true` before calling `render-local-env.sh`.   When starting your UI, run:

```bash
FIAB=true ./config/docker-rsync-local-ui.sh
```
	
### Run tests

`sbt -Djsse.enableSNIExtension=false -Dheadless=false test`

IntelliJ
- Edit Configurations -> Defaults -> ScalaTest
- set VM parameters `-Djsse.enableSNIExtension=false -Dheadless=false`
- set Working dir to local dir
- use classpath and SDK of the leonardoTests module
- should be able to right-click-run
