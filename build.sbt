import Settings._
import Testing._

lazy val root = project.in(file("."))
  .settings(
    name := "leonardo",
    skip in publish := true,
    rootSettings
  ).aggregate(http, automation)

lazy val http = project.in(file("http"))
  .settings(rootSettings)
  .withTestSettings

lazy val automation = project.in(file("automation"))
  .settings(automationSettings)

Revolver.settings

Revolver.enableDebugging(port = 5051, suspend = false)

mainClass in reStart := Some("org.broadinstitute.dsde.workbench.leonardo.Boot")

// When JAVA_OPTS are specified in the environment, they are usually meant for the application
// itself rather than sbt, but they are not passed by default to the application, which is a forked
// process. This passes them through to the "re-start" command, which is probably what a developer
// would normally expect.
javaOptions in reStart ++= sys.env.get("JAVA_OPTS").map(_.split(" ").toSeq).getOrElse(Seq.empty)