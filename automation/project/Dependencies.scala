import sbt._

object Dependencies {
  val scalaV = "2.12"

  val jacksonV = "2.9.5"
  val akkaV = "2.5.13"
  val akkaHttpV = "10.1.2"

  val workbenchModelV   = "0.12-a19203d"
  val workbenchGoogleV  = "0.16-f2a0020"
  val workbenchGoogle2V = "0.5-f8ebba7"

   val serviceTestV = "0.16-f0e5d47"

  val excludeWorkbenchModel  = ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-model_" + scalaV)
  val excludeWorkbenchGoogle = ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-google_" + scalaV)
  val excludeWorkbenchMetrics   = ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-metrics_2.12")
  val excludeIoGrpc             = ExclusionRule(organization = "io.grpc", name = "grpc-core")
  val excludeFindbugsJsr        = ExclusionRule(organization = "com.google.code.findbugs", name = "jsr305")
  val excludeGoogleOauth2    = ExclusionRule(organization = "com.google.apis", name = "google-api-services-oauth2")
  val excludeGoogleApiClient = ExclusionRule(organization = "com.google.api-client", name = "google-api-client")
  val excludeGoogleJsr305    = ExclusionRule(organization = "com.google.code.findbugs", name = "jsr305")
  val excludeGuavaJdk5       = ExclusionRule(organization = "com.google.guava", name = "guava-jdk5")
  val excludeGuava           = ExclusionRule(organization = "com.google.guava", name = "guava")
  val excludeApacheHttpClient = ExclusionRule(organization = "org.apache.httpcomponents", name = "httpclient")
  val excludeGrpc            = ExclusionRule(organization = "io.grpc", name = "grpc-core")
  val excludeGoogleError     = ExclusionRule(organization = "com.google.errorprone", name = "error_prone_annotations")
  val excludeSlf4j           = ExclusionRule(organization = "org.slf4j", name = "slf4j-api")
  val excludeHttpComponent      = ExclusionRule(organization = "org.apache.httpcomponents", name = "httpclient")
  val excludeJacksonCore     = ExclusionRule(organization = "com.fasterxml.jackson.core", name =  "jackson-core")
  val excludeJacksonAnnotation  = ExclusionRule(organization = "com.fasterxml.jackson.core", name = "jackson-annotations")

  val workbenchModel: ModuleID  =  "org.broadinstitute.dsde.workbench" %% "workbench-model"  % workbenchModelV excludeAll (excludeGoogleJsr305, excludeGoogleError)
  val workbenchGoogle: ModuleID = "org.broadinstitute.dsde.workbench"  %% "workbench-google" % workbenchGoogleV excludeAll (excludeWorkbenchModel, excludeGoogleOauth2, excludeGoogleJsr305, excludeGoogleApiClient, excludeGrpc, excludeGoogleError, excludeSlf4j)
  val workbenchGoogle2: ModuleID     = "org.broadinstitute.dsde.workbench" %% "workbench-google2"  % workbenchGoogle2V excludeAll (excludeWorkbenchModel, excludeGuava, excludeGuavaJdk5, excludeWorkbenchMetrics, excludeIoGrpc, excludeGoogleError, excludeFindbugsJsr, excludeGoogleApiClient, excludeHttpComponent)


  val workbenchServiceTest: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-service-test" % serviceTestV % "test" classifier "tests" excludeAll (
    excludeWorkbenchModel,
    excludeWorkbenchGoogle)

  val rootDependencies = Seq(
    // proactively pull in latest versions of Jackson libs, instead of relying on the versions
    // specified as transitive dependencies, due to OWASP DependencyCheck warnings for earlier versions.
    "com.fasterxml.jackson.core" % "jackson-annotations"  % jacksonV,
    "com.fasterxml.jackson.core" % "jackson-databind"     % jacksonV excludeAll (excludeJacksonAnnotation),
    "com.fasterxml.jackson.core" % "jackson-core"         % jacksonV,
    "com.fasterxml.jackson.module" % ("jackson-module-scala_" + scalaV) % jacksonV,
    "ch.qos.logback"  % "logback-classic" % "1.2.3"  % "test",
    "com.google.apis" % "google-api-services-oauth2" % "v1-rev142-1.23.0" excludeAll (
      excludeGuavaJdk5,
      excludeApacheHttpClient,
      excludeGoogleJsr305,
      excludeJacksonCore),
    "com.google.api-client" % "google-api-client"   % "1.23.0" excludeAll (
      excludeGuavaJdk5,
      excludeApacheHttpClient,
      excludeGoogleJsr305,
      excludeJacksonCore),

    "com.typesafe.akka"   %%  "akka-http-core"      % akkaHttpV,
    "com.typesafe.akka"   %%  "akka-stream-testkit" % akkaV    % "test",
    "com.typesafe.akka"   %%  "akka-actor"          % akkaV,
    "com.typesafe.akka"   %%  "akka-http"           % akkaHttpV,
    "com.typesafe.akka"   %%  "akka-testkit"        % akkaV     % "test",
    "com.typesafe.akka"   %%  "akka-slf4j"          % akkaV,
    "org.scalatest"       %%  "scalatest"           % "3.0.5"   % "test",
    "org.seleniumhq.selenium" % "selenium-java"     % "3.14.0" % "test",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0",
    "org.apache.commons" % "commons-text"           % "1.2",
    "io.grpc" % "grpc-core" % "1.12.0" excludeAll(excludeGuava),

    workbenchModel,
    workbenchGoogle,
    workbenchGoogle2,
    workbenchServiceTest,

    // required by workbenchGoogle
    "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.6" % "provided"
  )
}
