# Leonardo Github Actions

This readme details the workflows contained in this repo and their purpose. 

## Auto merge
The [automerge workflow](workflows/automerge.yml) is ran against PRs with the label `automerge` and by the user `broadboat`. PRs that fit this criteria are auto-approved.

## Azure Automation tests
The [azure automation test workflow](workflows/azure_automation_test.yml) is run twice a day or on demand. It runs tests in the `LeonardoAzureSuite`. 
It triggers a downstream action in the repo [terra-github-workflows](https://github.com/broadinstitute/terra-github-workflows) that provisions a bee, and provides an azure billing project to the test suite.

## Azure e2e app tests
The [Azure app tests](workflows/azure_e2e_release_promotion_tests.yml) are run on a beehive hook when leonardo is deployed to staging, and deploy various apps via leonardo to a test workspace. These tests live in the Leonardo repo, but are not maintained by the IA team. They are maintained by Analysis Journeys, and deal with WDS and Cromwell apps.

## Consumer Contract tests
The [consumer contract tests](workflows/consumer_contract_tests.yml) are documented extensively in the workflow file itself. This handles Leo's dependencies on external systems.
Specifically, the main contract they verify is that between Sam and Leo. They do this via Pact.io. This workflow creates contracts based on those dependencies. 

## Custom Image generation
The [custom image generaton](workflows/custom_image_generation.yml) can be run on-demand to generate the custom VM image (dataproc or GCE) that is used as the ISO, or disk imae, for VMs provisioned by leonardo. These jobs cache the docker images used by users of these VMs to improve VM creation times.

## Leonardo build, publish and test
This [job](workflows/leo-build-tag-publish-and-run-tests.yml) builds leonanrdo, publishes the image, provisions a bee with that image of leonardo, and then runs the automation test suite against that bee. It also reports the build to sherlock. 
Very similar to the azure job and will likely be consolidated with it soon, they don't differ much, mainly the sbt test command is different. They both rely on the same downstream [terra-github-workflows](https://github.com/broadinstitute/terra-github-workflows) jobs.

## Publish Java client
This [job](workflows/publish_java_client.yml) publishes a swagger-generated client for leonardo to artifactory consumable by any JVM application. It is used in our automation tests, and by other teams within the broad in their automation tests. 
This will run on any PR commits/commits to dev. To iterate on the swagger page/client config, it publishes `-SNAP` versions you can consume in your project dependencies.

## Tag
This [job](workflows/tag.yml) is responsible for generating the tag associated with build artifacts for various workflows. It is a broad extension of [github-tag-action](github-tag-action), and can be found in this [repo](https://github.com/DataBiosphere/github-actions/tree/master/actions/bumper) 

## Trivy
This [job](workflows/trivy.yml) is an action maintained by the Security team. It runs scans on the Leonardo Dockerfile to ensure security concerns are not violated. See further details in the [trivy repo](https://github.com/broadinstitute/dsp-appsec-trivy-action).

## Unit test
This [job](workflows/unit_test.yml) spins up a local sql docker container and executes the leonardo unit tests against it. It is also responsible for ensuring the code is properly formatted and runs a code coverage diff. 

## Consumer contract verification
This [job](workflows/verify_consumer_contracts.yml) handles external systems dependencies on Leo. It pulls contracts from Pact Broken, and then makes sure Leo's endpoints work the way the external callers expect.
An example of this would be modeling and verifying AOU's dependency on Leo endpoints. 







