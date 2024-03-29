# Leonardo Github Actions

This readme details the workflows contained in this repo and their purpose. 

## Auto merge
The [automerge workflow](workflows/automerge.yml) is ran against PRs with the label `automerge` that are made by the user `broadboat`. PRs that fit this criteria are auto-approved and merged.

## Azure Automation tests
The [azure automation test workflow](workflows/azure_automation_test.yml) is run twice a day and on-demand. It runs tests in the `LeonardoAzureSuite` automation test suite against a dynamically created BEE. 
It triggers a downstream action in the repo [terra-github-workflows](https://github.com/broadinstitute/terra-github-workflows) that provisions the BEE, and provides an Azure billing project to the test suite. The BEE created by this job is configured to clean up any resources created via the tests.

## Azure data plane app tests
The [Azure app tests](workflows/azure_e2e_release_promotion_tests.yml) are run on a beehive hook when leonardo is promoted to staging. These tests deploy various apps via Leonardo to a test workspace. These tests live in the Leonardo repo, but are not maintained by the IA team. They are maintained by Analysis Journeys, and deal with WDS and Cromwell apps.

## Leonardo contract tests against external systems
The [consumer contract tests](workflows/consumer_contract_tests.yml) are documented extensively in the workflow file itself. This handles **Leo's dependencies on external systems**.
Specifically, the main contract they verify is that between Sam and Leo. They do this via Pact.IO. This workflow creates and pushes contracts based on those dependencies to Pact Broker.

## External contract tests against Leonardo
This [job](workflows/verify_consumer_contracts.yml) handles **external system's dependencies on Leo**. It pulls contracts from Pact Broker, and then makes sure Leonardo's endpoints work the way the external callers expect.
An example of this would be modeling and verifying AOU's dependency on Leo endpoints.

## Custom Image generation
The [custom image generaton](workflows/custom_image_generation.yml) can be run on-demand to generate the custom VM image (Dataproc or GCE) that is used as the ISO, or disk image, for VMs provisioned by Leonardo. These jobs cache the docker images used by users of these VMs on the disk image to improve VM creation times.

## Leonardo build, tag, publish, and test
This [job](workflows/leo-build-tag-publish-and-run-tests.yml) builds Leonardo, tags and publishes the image, provisions a BEE with that image of Leonardo, and then runs the GCP automation test suite against that BEE. It also reports the build to Sherlock for future promotion. 
Very similar to the Azure job and will likely be consolidated with it soon. They don't differ much; a copy-paste with a different sbt test command. They both rely on the same downstream [terra-github-workflows](https://github.com/broadinstitute/terra-github-workflows) jobs.

## Publish Java client
This [job](workflows/publish_java_client.yml) publishes a swagger-generated client for leonardo to artifactory consumable by any JVM application. It is used in our automation tests, and by other teams that depend on leonardo API calls in their application or tests. 
This will run on both PR commits and commits to dev. The purpose of it running on PR commits is it can help iterate on the swagger page/client changes or fixes. Any versions published against a PR will be suffixed with `-SNAP`. An example of an sbt import can be found below. To find a specific version, you can look at the logs for a run.
```
"org.broadinstitute.dsde.workbench" %% "leonardo-client" % "1.3.6-35973f1-SNAP"
```

## Tag
This [job](workflows/tag.yml) is responsible for generating the tag associated with build artifacts and consumed by multiple workflows in this repo. It is a broad extension of [github-tag-action](github-tag-action), and can be found in this [repo](https://github.com/DataBiosphere/github-actions/tree/master/actions/bumper) 

## Trivy
This [job](workflows/trivy.yml) is an action maintained by the Security team. It runs scans on the Leonardo Dockerfile to ensure security concerns are not violated. See further details in the [trivy repo](https://github.com/broadinstitute/dsp-appsec-trivy-action).

## Unit test
This [job](workflows/unit_test.yml) spins up a local sql Docker container and executes the Leonardo unit tests against it. It is also responsible for ensuring the code is properly formatted (via `scalaFmt`), and runs a code coverage diff.
