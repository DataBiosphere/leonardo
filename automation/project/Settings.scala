import Dependencies._
import sbt.Keys._
import sbt._

object Settings {

  //coreDefaultSettings + defaultConfigs = the now deprecated defaultSettings
  val commonBuildSettings = Defaults.coreDefaultSettings ++ Defaults.defaultConfigs ++ Seq(
    javaOptions += "-Xmx2G",
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
  )

  val commonCompilerSettings = Seq(
    "-unchecked",
    "-deprecation",
    "-feature",
    "-encoding", "utf8",
    "-target:jvm-1.8",
    "-Xmax-classfile-name", "100"
  )

  val testSettings = List(
    testOptions in Test += Tests.Argument("-oF")
  )

  //common settings for all sbt subprojects
  val commonSettings =
    commonBuildSettings ++ testSettings ++ List(
    organization  := "org.broadinstitute.dsde.firecloud",
    scalaVersion  := "2.11.8",
    scalacOptions ++= commonCompilerSettings
  )

  //the full list of settings for the root project that's ultimately the one we build into a fat JAR and run
  //coreDefaultSettings (inside commonSettings) sets the project name, which we want to override, so ordering is important.
  //thus commonSettings needs to be added first.
  val rootSettings = commonSettings ++ List(
    name := "Leonardo-Tests",
    libraryDependencies ++= rootDependencies
  )
}
