import sbt._

object Dependencies {
  val jacksonV = "2.9.0"
  val akkaV = "2.4.17"
  val akkaHttpV = "10.0.5"

  val workbenchModelV   = "0.10-6800f3a"
  val workbenchGoogleV  = "0.16-2947b3a-SNAP"
  val serviceTestV = "0.5-d440a49-SNAP"
  val leoModelV = "0.1-9671e70-SNAP"

  val excludeWorkbenchModel =   ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-model_2.12")
  val excludeWorkbenchGoogle =   ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-google_2.12")

  val workbenchModel: ModuleID =  "org.broadinstitute.dsde.workbench" %% "workbench-model"  % workbenchModelV
  val workbenchGoogle: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-google" % workbenchGoogleV excludeAll excludeWorkbenchModel

  val workbenchServiceTest: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-service-test" % serviceTestV % "test" classifier "tests" excludeAll (
    excludeWorkbenchModel,
    excludeWorkbenchGoogle)

  val leoModel: ModuleID = "org.broadinstitute.dsde.workbench" %% "leonardo-model" % leoModelV excludeAll (
    excludeWorkbenchModel,
    excludeWorkbenchGoogle)

  val rootDependencies = Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonV,
    "ch.qos.logback" % "logback-classic" % "1.1.3",
    "com.google.apis" % "google-api-services-oauth2" % "v1-rev112-1.20.0" excludeAll (
      ExclusionRule("com.google.guava", "guava-jdk5"),
      ExclusionRule("org.apache.httpcomponents", "httpclient")
    ),
    "com.google.api-client" % "google-api-client" % "1.22.0" excludeAll (
      ExclusionRule("com.google.guava", "guava-jdk5"),
      ExclusionRule("org.apache.httpcomponents", "httpclient")),
    "org.webjars"           %  "swagger-ui"    % "2.2.5",
    "com.typesafe.akka"   %%  "akka-http-core"     % akkaHttpV,
    "com.typesafe.akka"   %%  "akka-http"           % akkaHttpV,
    "com.typesafe.akka"   %%  "akka-testkit"        % akkaV     % "test",
    "com.typesafe.akka"   %%  "akka-slf4j"          % akkaV,
    "org.scalatest"       %%  "scalatest"     % "3.0.1"   % "test",
    "org.seleniumhq.selenium" % "selenium-java" % "3.8.1" % "test",
    "org.apache.commons" % "commons-text" % "1.2",

    workbenchModel,
    workbenchGoogle,
    leoModel,
    workbenchServiceTest,

    // required by workbenchGoogle
    "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.6" % "provided"
  )
}
