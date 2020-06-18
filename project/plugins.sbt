addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")

addSbtPlugin("io.spray" % "sbt-revolver" % "0.9.1")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.2.5")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.3.4")

addSbtPlugin(
  "com.github.cb372" % "sbt-explicit-dependencies" % "0.2.11"
) // Use `unusedCompileDependencies` to see unused dependencies

addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.9.15")
