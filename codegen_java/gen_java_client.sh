set -e

docker run --rm -v ${PWD}:/local openapitools/openapi-generator-cli:v6.4.0 generate -i /local/http/src/main/resources/swagger/api-docs.yaml -g java -o /local/codegen_java --api-package org.broadinstitute.dsde.workbench.client.leonardo.api --model-package org.broadinstitute.dsde.workbench.client.leonardo.model --template-dir /local/codegen_java/templates --library okhttp-gson
cd codegen_java
sbt test