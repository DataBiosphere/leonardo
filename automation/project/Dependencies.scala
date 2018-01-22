import sbt._

object Dependencies {
  val jacksonV = "2.8.4"
  val akkaV = "2.4.17"
  val akkaHttpV = "10.0.5"

  val workbenchModelV   = "0.8-d97f551"
  val workbenchGoogleV  = "0.10-b95b2c1"
  //UPDATE THIS WITH FINAL HASH
  val serviceTestV = "0.1.1-alpha-sam-fb82d4c-SNAP"

  val excludeWorkbenchModel =   ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-model_2.11")

  val workbenchModel: ModuleID =  "org.broadinstitute.dsde.workbench" %% "workbench-model"  % workbenchModelV
  val workbenchGoogle: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-google" % workbenchGoogleV excludeAll excludeWorkbenchModel
  val workbenchServiceTest: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-service-test" % serviceTestV % "test" classifier "tests" excludeAll excludeWorkbenchModel

  val rootDependencies = Seq(
    // proactively pull in latest versions of Jackson libs, instead of relying on the versions
    // specified as transitive dependencies, due to OWASP DependencyCheck warnings for earlier versions.
    "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonV,
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonV,
    "com.fasterxml.jackson.core" % "jackson-core" % jacksonV,
    "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % jacksonV,
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
    "com.typesafe.akka"   %%  "akka-stream-testkit" % "2.4.11",
    "com.typesafe.akka"   %%  "akka-http"           % akkaHttpV,
    "com.typesafe.akka"   %%  "akka-testkit"        % akkaV     % "test",
    "com.typesafe.akka"   %%  "akka-slf4j"          % akkaV,
    "org.specs2"          %%  "specs2-core"   % "3.7"  % "test",
    "org.scalatest"       %%  "scalatest"     % "2.2.6"   % "test",
    "org.seleniumhq.selenium" % "selenium-java" % "3.8.1" % "test",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
    "org.apache.commons" % "commons-text" % "1.2",

    workbenchModel,
    workbenchGoogle,
    workbenchServiceTest,

    // required by workbenchGoogle
    "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.6" % "provided"
  )
}
