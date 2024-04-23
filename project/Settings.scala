import Dependencies._
import Merging._
import Testing._
import Version._
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyPlugin.autoImport._
import scalafix.sbt.ScalafixPlugin.autoImport.scalafixSemanticdb
// sbt is still on 2.12
import scala.collection.JavaConverters._

object Settings {

  val artifactory = "https://broadinstitute.jfrog.io/broadinstitute/"

  val commonResolvers = List(
    "artifactory-releases" at artifactory + "libs-release",
    "artifactory-snapshots" at artifactory + "libs-snapshot"
  )

  //coreDefaultSettings + defaultConfigs = the now deprecated defaultSettings
  val commonBuildSettings = Defaults.coreDefaultSettings ++ Defaults.defaultConfigs ++ Seq(
    javaOptions += "-Xmx2G",
    javacOptions ++= Seq("--release", "17"),
    Compile / console / scalacOptions --= Seq("-Ywarn-unused:imports", "-Xfatal-warnings"), //disable unused fatal warning in `sbt console`
    Test / console / scalacOptions --= Seq("-Ywarn-unused:imports", "-Xfatal-warnings"), //disable unused fatal warning in `sbt test:console`
    Test / scalacOptions --= List("-Ywarn-dead-code", "-deprecation", "-Xfatal-warnings"),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision
  )

  val commonCompilerSettings = Seq(
    "-release:17",
    "-deprecation",                      // Emit warning and location for usages of deprecated APIs.
    "-encoding", "utf-8",                // Specify character encoding used by source files.
    "-explaintypes",                     // Explain type errors in more detail.
    "-feature",                          // Emit warning and location for usages of features that should be imported explicitly.
    "-language:existentials",            // Existential types (besides wildcard types) can be written and inferred
    "-language:higherKinds",             // Allow higher-kinded types
    "-language:implicitConversions",     // Allow definition of implicit functions called views
    "-unchecked",                        // Enable additional warnings where generated code depends on assumptions.
    "-Xcheckinit",                       // Wrap field accessors to throw an exception on uninitialized access.
    "-Xfatal-warnings",                  // Fail the compilation if there are any warnings.
    // "-Xlint:adapted-args",               // Warn if an argument list is modified to match the receiver.
    "-Xlint:constant",                   // Evaluation of a constant arithmetic expression results in an error.
    "-Xlint:delayedinit-select",         // Selecting member of DelayedInit.
    "-Xlint:doc-detached",               // A Scaladoc comment appears to be detached from its element.
    "-Xlint:inaccessible",               // Warn about inaccessible types in method signatures.
    "-Xlint:infer-any",                  // Warn when a type argument is inferred to be `Any`.
    "-Xlint:missing-interpolator",       // A string literal appears to be missing an interpolator id.
    "-Xlint:nullary-unit",               // Warn when nullary methods return Unit.
    "-Xlint:option-implicit",            // Option.apply used implicit view.
    "-Xlint:package-object-classes",     // Class or object defined in package object.
    "-Xlint:poly-implicit-overload",     // Parameterized overloaded implicit methods arClusterComponent.scalae not visible as view bounds.
    "-Xlint:private-shadow",             // A private field (or class parameter) shadows a superclass field.
    "-Xlint:stars-align",                // Pattern sequence wildcard must align with sequence component.
    "-Xlint:type-parameter-shadow",      // A local type parameter shadows a type already in scope.
//    "-Yno-adapted-args",                 // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
    "-Ywarn-dead-code",                  // Warn when dead code is identified.
    "-Ywarn-extra-implicit",             // Warn when more than one implicit parameter section is defined.
//    "-Ywarn-numeric-widen",              // Warn when numerics are widened.
    "-Ywarn-unused:implicits",           // Warn if an implicit parameter is unused.
    "-Ywarn-unused:imports",             // Warn if an import selector is not referenced.
//    "-Ywarn-unused:locals",              // Warn if a local definition is unused.
//    "-Ywarn-unused:params",              // Warn if a value parameter is unused.
    "-Ywarn-unused:patvars",             // Warn if a variable bound in a pattern is unused.
//    "-Ywarn-unused:privates",            // Warn if a private member is unused.
//    "-Ywarn-value-discard",               // Warn when non-Unit expression results are unused.
    "-language:postfixOps"
  )

  //sbt assembly settings
  val commonAssemblySettings = Seq(
    assembly / assemblyMergeStrategy := customMergeStrategy((assembly / assemblyMergeStrategy).value),
  //  Try to fix the following error. We're not using akka-stream, so it should be safe to exclude `akka-protobuf`
  //  [error] /Users/qi/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.11.4/protobuf-java-3.11.4.jar:google/protobuf/field_mask.proto
  //  [error] /Users/qi/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/typesafe/akka/akka-protobuf-v3_2.12/2.6.1/akka-protobuf-v3_2.12-2.6.1.jar:google/protobuf/field_mask.proto
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp filter {_.data.getName == "akka-protobuf-v3_2.13-2.6.3.jar"}
    },
    assembly / test := {}
  )

  //common settings for all sbt subprojects
  val commonSettings =
    commonBuildSettings ++ List(
    organization  := "org.broadinstitute.dsde.workbench",
    scalaVersion  := "2.13.12",
    resolvers ++= commonResolvers,
    scalacOptions ++= commonCompilerSettings
  )

  val coreSettings = commonSettings ++ commonTestSettings ++ List(
    libraryDependencies ++= coreDependencies
    //the version is applied in versionSettings and is set to 0.1-githash.
    //we don't really use it for anything but we might when we publish our model
  ) ++ versionSettings

  //the full list of settings for the root project that's ultimately the one we build into a fat JAR and run
  //coreDefaultSettings (inside commonSettings) sets the project name, which we want to override, so ordering is important.
  //thus commonSettings needs to be added first.
  val httpSettings = commonSettings ++ commonTestSettings ++ List(
    libraryDependencies ++= httpDependencies
    //the version is applied in versionSettings and is set to 0.1-githash.
    //we don't really use it for anything but we might when we publish our model
  ) ++ commonAssemblySettings ++ versionSettings

  val automationSettings = commonSettings ++ List(
    libraryDependencies ++= automationDependencies,
    dependencyOverrides ++= Dependencies.automationOverrides,
    /**
      * sbt forking jvm -- sbt provides 2 testing modes: forked vs not forked.
      * -- forked: each task (test class) is executed in a forked JVM.
      *    Test results are segregated, easy to read.
      * -- not forked: all tasks (test classes) are executed in same sbt JVM.
      *    Test results are not segregated, hard to read.
      *
      */

    /**
      * Specify that all tests will be executed in a single external JVM.
      * By default, tests executed in a forked JVM are executed sequentially.
      */
    Test / fork := true,

    /**
      * forked tests can optionally be run in parallel.
      */
    Test / testForkedParallel := true,

    /**
      * When forking, use the base directory as the working directory
      */
    Test / baseDirectory := (ThisBuild / baseDirectory).value,

    /*
     * Enables (true) or disables (false) parallel execution of tasks (test classes).
     * In not-forked mode: test classes are run in parallel in different threads, in same sbt jvm.
     * In forked mode: each test class runs tests in sequential order, in a separated jvm.
     */
    Test / parallelExecution := true,

    /**
      * disable sbt's log buffering
      */
    Test / logBuffered := false,


    //See executing suites in parallel section here: https://www.scalatest.org/user_guide/using_the_runner
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-P8"),

    /**
      * Control the number of forked JVMs allowed to run at the same time by
      *  setting the limit on Tags.ForkedTestGroup tag, 1 is default.
      *  Warning: can't set too high (set at 10 would crashes OS)
      */
    Global / concurrentRestrictions := Seq(Tags.limitAll(16)),

    /**
      * Forked JVM options
      */
    Test / javaOptions ++= Seq("-Xmx2G"),

    /**
      * copy system properties to forked JVM
      */
    Test / javaOptions ++= Seq({
      val props = System.getProperties
      props.stringPropertyNames().asScala.toList.map { key => s"-D$key=${props.getProperty(key)}"}.mkString(" ")
    }),

    Test / testGrouping := {
      (Test / definedTests).value.map { test =>
        /**
          * debugging print out:
          *
          * println("test.name: " + test.name)
          * println("(Test/baseDirectory).value: " + (Test / baseDirectory).value)
          * println("(baseDirectory in ThisBuild).value: " + (baseDirectory in ThisBuild).value)
          *
          * val envirn = System.getenv()
          *   envirn.keySet.forEach {
          * key => s"-D$key=${envirn.get(key)}"
          * println(s"-D$key=${envirn.get(key)}")
          * }
          */

        val options = ForkOptions()
          .withConnectInput(true)
          .withWorkingDirectory(Some((Test / baseDirectory).value))
          .withOutputStrategy(Some(sbt.StdoutOutput))
          .withRunJVMOptions(
            Vector(
              s"-Dlogback.configurationFile=${(Test / baseDirectory).value.getAbsolutePath}/src/test/resources/logback-test.xml",
              s"-Djava.util.logging.config.file=${(Test / baseDirectory).value.getAbsolutePath}/src/test/resources/logback-test.xml",
              s"-Dtest.name=${test.name}",
              s"-Ddir.name=${(Test / baseDirectory).value}",
              s"-Dheadless=${Option(System.getProperty("headless")).getOrElse("false")}"))
        Tests.Group(
          name = test.name,
          tests = Seq(test),
          runPolicy = Tests.SubProcess(options)
        )
      }
    },

    Test / testOptions += Tests.Argument("-oFD", "-u", "test-reports", "-fW", "test-reports/TEST-summary.log")
  )

  val pact4sSettings = commonSettings ++ commonTestSettings ++ List(
    libraryDependencies ++= pact4sDependencies,

    /**
     * Invoking pact tests from root project (sbt "project pact" test)
     * will launch tests in a separate JVM context that ensures contracts
     * are written to the pact/target/pacts folder. Otherwise, contracts
     * will be written to the root folder.
     */
    Test / fork := true

  ) ++ commonAssemblySettings ++ versionSettings
}
