import Dependencies._
import sbt.Keys._
import sbt._

object Settings {
  // for org.broadinstitute.dsde.workbench modules
  val artifactory = "https://broadinstitute.jfrog.io/broadinstitute/"
  val commonResolvers = List(
    "artifactory-releases" at artifactory + "libs-release",
    "artifactory-snapshots" at artifactory + "libs-snapshot"
  )

  //coreDefaultSettings + defaultConfigs = the now deprecated defaultSettings
  val commonBuildSettings = Defaults.coreDefaultSettings ++ Defaults.defaultConfigs ++ Seq(
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
    testOptions in Test += Tests.Argument("-oFD", "-u", "test-reports", "-fW", "test-reports/TEST-summary.log")
  )

  //common settings for all sbt subprojects
  val commonSettings =
    commonBuildSettings ++ testSettings ++ List(
    organization  := "org.broadinstitute.dsde.firecloud",
    scalaVersion  := "2.12.8",
    resolvers ++= commonResolvers,
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
