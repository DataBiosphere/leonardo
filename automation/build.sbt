import Settings._
import sbt._

lazy val leonardoTests = project.in(file("."))
  .settings(rootSettings:_*)

version := "1.0"

/**
  * disable sbt's log buffering
  */
Test / logBuffered := false