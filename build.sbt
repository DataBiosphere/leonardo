import Settings._
import Testing._

lazy val root = project.in(file("."))
  .settings(
    name := "leonardo",
    skip in publish := true
  ).aggregate(core, http, automation, subscriber)

lazy val core = project.in(file("core"))
  .settings(coreSettings)

lazy val http = project.in(file("http"))
  .settings(httpSettings)
  .dependsOn(core % "test->test;compile->compile")
  .dependsOn(subscriber % "test->test;compile->compile")
  .withTestSettings

lazy val subscriber = project.in(file("subscriber"))
  .settings(subscriberSettings)
  .dependsOn(core % "test->test;compile->compile")
  .withTestSettings

lazy val automation = project.in(file("automation"))
  .settings(automationSettings)
  .dependsOn(core % "test->test;compile->compile")

scalafixDependencies in ThisBuild += "org.scalatest" %% "autofix" % "3.1.0.1"
