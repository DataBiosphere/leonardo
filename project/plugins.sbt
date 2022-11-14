addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.0.0")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.5")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.6")

addSbtPlugin(
  "com.github.cb372" % "sbt-explicit-dependencies" % "0.2.16"
) // Use `unusedCompileDependencies` to see unused dependencies

addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.10.4")

dependencyOverrides ++= Seq(
  "org.scala-lang.modules" % "scala-xml_2.12" % "2.1.0"
)

addSbtPlugin("com.itv" % "sbt-scalapact" % "4.4.0")
