import sbt.Keys._
import sbt._

object Version {
  private def getVersionString =
    // either specify git model hash as an env var or derive it
    // if building from the broadinstitute/scala-baseimage docker image use env var
    // (scala-baseimage doesn't have git in it)
    sys.env.getOrElse("GIT_HASH", sys.process.Process("git rev-parse --short HEAD") !!).trim()

  val versionSettings: Seq[Setting[_]] =
    Seq(version := getVersionString)
}
