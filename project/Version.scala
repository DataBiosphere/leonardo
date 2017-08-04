import sbt.Keys._
import sbt._


object Version {
  val baseModelVersion = "0.1"

  def getVersionString = {
    def getLastModelCommitFromGit = { s"""git rev-parse --short HEAD""" !! }

    // either specify git model hash as an env var or derive it
    // if building from the broadinstitute/scala-baseimage docker image use env var
    // (scala-baseimage doesn't have git in it)
    val lastModelCommit = sys.env.getOrElse("GIT_MODEL_HASH", getLastModelCommitFromGit ).trim()
    val version = baseModelVersion + "-" + lastModelCommit

    // The project isSnapshot string passed in via command line settings, if desired.
    val isSnapshot = sys.props.getOrElse("project.isSnapshot", "true").toBoolean

    // For now, obfuscate SNAPSHOTs from sbt's developers: https://github.com/sbt/sbt/issues/2687#issuecomment-236586241
    if (isSnapshot) s"$version-SNAPSHOT" else version
  }

  val rootVersionSettings: Seq[Setting[_]] =
    Seq(version := getVersionString)
}
