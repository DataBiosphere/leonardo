import Settings._
import sbt._
import sbt.Keys.{fork, javaOptions, parallelExecution, testForkedParallel}
import scala.collection.JavaConverters._


lazy val leonardoTests = project.in(file("."))
  .settings(rootSettings:_*)

version := "1.0"

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
Test / fork := true

/**
  * forked tests can optionally be run in parallel.
  */
Test / testForkedParallel := true

/**
  * When forking, use the base directory as the working directory
  */
Test / baseDirectory := (baseDirectory in ThisBuild).value

/*
 * Enables (true) or disables (false) parallel execution of tasks (test classes).
 * In not-forked mode: test classes are run in parallel in different threads, in same sbt jvm.
 * In forked mode: each test class runs tests in sequential order, in a separated jvm.
 */
Test / parallelExecution := true

/**
  * disable sbt's log buffering
  */
Test / logBuffered := false

/**
  * Control the number of forked JVMs allowed to run at the same time by
  *  setting the limit on Tags.ForkedTestGroup tag, which is 1 by default.
  *  Warning: can't set too high (set at 10 would crashes OS)
  */
Global / concurrentRestrictions := Seq(Tags.limit(Tags.ForkedTestGroup, 5))

/**
  * Forked JVM options
  */
Test / javaOptions ++= Seq("-Xmx2G")

/**
  * copy system properties to forked JVM
  */
Test / javaOptions ++= Seq({
  val props = System.getProperties
  props.stringPropertyNames().asScala.toList.map { key => s"-D$key=${props.getProperty(key)}"}.mkString(" ")
})

testGrouping in Test := {
  (definedTests in Test).value.map { test =>
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
          s"-Dheadless=${Option(System.getProperty("headless")).getOrElse("false")}",
          s"-Djsse.enableSNIExtension=${Option(System.getProperty("jsse.enableSNIExtension")).getOrElse("false")}"))
    Tests.Group(
      name = test.name,
      tests = Seq(test),
      runPolicy = Tests.SubProcess(options)
    )
  }
}
