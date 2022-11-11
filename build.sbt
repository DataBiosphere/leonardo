import Settings._
import Testing._

lazy val root = project.in(file("."))
  .settings(
    name := "leonardo",
    publish / skip := true
  ).aggregate(core, http, automation, pact)

lazy val core = project.in(file("core"))
  .settings(coreSettings)

lazy val http = project.in(file("http"))
  .settings(httpSettings)
  .dependsOn(core % "test->test;compile->compile")
  .withTestSettings

lazy val automation = project.in(file("automation"))
  .settings(automationSettings)
  .dependsOn(core % "test->test;compile->compile")

lazy val pact = project.in(file("pact"))
  .enablePlugins(ScalaPactPlugin)
  .settings(pactSettings)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

ThisBuild / scalafixDependencies += "org.scalatest" %% "autofix" % "3.1.0.1"
