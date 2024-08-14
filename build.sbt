import Settings._
import Testing._

lazy val root = project.in(file("."))
  .settings(
    name := "leonardo",
    publish / skip := true
  ).aggregate(core, http, automation, pact4s)

lazy val core = project.in(file("core"))
  .settings(coreSettings)

lazy val http = project.in(file("http"))
  .settings(httpSettings)
  .dependsOn(core % "test->test;compile->compile")
  .withTestSettings

lazy val automation = project.in(file("automation"))
  .settings(automationSettings)
  .dependsOn(core % "test->test;compile->compile")

lazy val pact4s = project.in(file("pact4s"))
  .settings(pact4sSettings)
  .dependsOn(http % "test->test;compile->compile")

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.concat
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

ThisBuild / scalafixDependencies += "org.scalatest" %% "autofix" % "3.1.0.1"
