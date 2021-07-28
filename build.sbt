import Settings._
import Testing._

lazy val root = project.in(file("."))
  .settings(
    name := "leonardo",
    publish / skip := true
  ).aggregate(core, http, automation)

lazy val core = project.in(file("core"))
  .settings(coreSettings)

lazy val http = project.in(file("http"))
  .settings(httpSettings)
  .dependsOn(core % "test->test;compile->compile")
  .withTestSettings

lazy val automation = project.in(file("automation"))
  .settings(automationSettings)
  .dependsOn(core % "test->test;compile->compile")

ThisBuild / scalafixDependencies += "org.scalatest" %% "autofix" % "3.1.0.1"

ThisBuild / scalacOptions += "-P:semanticdb:synthetics:on"
