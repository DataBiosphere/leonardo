import sbt._

object Dependencies {
  val akkaV = "2.5.3"
  val akkaHttpV = "10.0.6"
  val jacksonV = "2.8.7"
  val googleV = "1.22.0"

  def excludeGuavaJDK5(m: ModuleID): ModuleID = m.exclude("com.google.guava", "guava-jdk5")

  val rootDependencies = Seq(
    // proactively pull in latest versions of Jackson libs, instead of relying on the versions
    // specified as transitive dependencies, due to OWASP DependencyCheck warnings for earlier versions.
    "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonV,
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonV,
    "com.fasterxml.jackson.core" % "jackson-core" % jacksonV,

    "ch.qos.logback" % "logback-classic" % "1.2.1",
    "com.getsentry.raven" % "raven-logback" % "7.8.6",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
    "org.webjars"          %  "swagger-ui"    % "2.2.5",
    "com.typesafe.akka"   %%  "akka-actor"    % akkaV,
    "com.typesafe.akka"   %%  "akka-contrib"  % akkaV,
    "com.typesafe.akka"   %%  "akka-testkit"  % akkaV     % "test",
    "com.typesafe.akka"   %%  "akka-slf4j"    % akkaV,
    "com.typesafe.akka"   %%  "akka-http" % akkaHttpV,
    "com.typesafe.akka"   %%  "akka-http-testkit" % akkaHttpV % "test",
    "com.typesafe.akka"   %%  "akka-http-spray-json" % akkaHttpV,
    "com.iheart"          %%  "ficus" % "1.4.0",
      //    "com.typesafe.akka"   %%  "akka-http-jackson" % akkaHttpV,
    "org.scalatest"       %%  "scalatest"     % "3.0.1"   % "test",
    excludeGuavaJDK5("com.google.apis"     % "google-api-services-dataproc" % ("v1-rev53-" + googleV)),
    "org.broadinstitute.dsde.workbench" %% "workbench-util" % "0.2-f87e766",
    "org.broadinstitute.dsde.workbench" %% "workbench-model" % "0.1-f87e766",
    "org.broadinstitute.dsde.workbench" %% "workbench-google" % "0.1-6924e2f"
  )
}
