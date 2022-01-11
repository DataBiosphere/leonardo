addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.1.0")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.9.3")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.5")

addSbtPlugin(
  "com.github.cb372" % "sbt-explicit-dependencies" % "0.2.16"
) // Use `unusedCompileDependencies` to see unused dependencies

addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.9.33")
