Quickstart: running swintegration tests locally on Mac/Docker 

`brew install chromedriver`

Note: Leonardo integration tests are not currently web-based but may fail due to dependencies without chromedriver

`./render-local-env.sh $PWD $(cat ~/.vault-token) dev`

`sbt -Djsse.enableSNIExtension=false -Dheadless=false test`

IntelliJ
- Edit Configurations -> Defaults -> ScalaTest
- set VM parameters `-Djsse.enableSNIExtension=false -Dheadless=false`
- set Working dir to local dir
- use classpath and SDK of the leonardoTests module
- should be able to right-click-run