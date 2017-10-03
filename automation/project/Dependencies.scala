import sbt._

object Dependencies {
  val jacksonV = "2.8.4"
  val akkaV = "2.4.17"
  val akkaHttpV = "10.0.5"

  val rootDependencies = Seq(
    // proactively pull in latest versions of Jackson libs, instead of relying on the versions
    // specified as transitive dependencies, due to OWASP DependencyCheck warnings for earlier versions.
    "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonV,
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonV,
    "com.fasterxml.jackson.core" % "jackson-core" % jacksonV,
    "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % jacksonV,
    "ch.qos.logback" % "logback-classic" % "1.1.3",
    "com.google.apis" % "google-api-services-oauth2" % "v1-rev112-1.20.0" exclude ("org.apache.httpcomponents", "httpclient"),
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
    "org.seleniumhq.selenium" % "selenium-java" % "2.35.0" % "test",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
  )
}
