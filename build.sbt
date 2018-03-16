import Settings._
import Testing._





lazy val leoModel = project.in(file("model"))
  .settings(modelSettings:_*)
  .withTestSettings

lazy val leoService = project.in(file("service"))
  .settings(leoServiceSettings:_*)
  .dependsOn(leoModel)
  .withTestSettings

lazy val leo = project.in(file("."))
  .settings(rootSettings:_*)
  .aggregate(leoModel)
  .aggregate(leoService)
  .dependsOn(leoService)
  .withTestSettings

Revolver.settings

Revolver.enableDebugging(port = 5051, suspend = false)

mainClass in reStart := Some("org.broadinstitute.dsde.workbench.leonardo.Boot")

// When JAVA_OPTS are specified in the environment, they are usually meant for the application
// itself rather than sbt, but they are not passed by default to the application, which is a forked
// process. This passes them through to the "re-start" command, which is probably what a developer
// would normally expect.
javaOptions in reStart ++= sys.env.get("JAVA_OPTS").map(_.split(" ").toSeq).getOrElse(Seq.empty)