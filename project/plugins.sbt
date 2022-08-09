addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.2.0")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.2")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.6")

addSbtPlugin(
  "com.github.cb372" % "sbt-explicit-dependencies" % "0.2.16"
) // Use `unusedCompileDependencies` to see unused dependencies

addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.9.34")
