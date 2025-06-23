import sbt.Keys._
import sbt._

/** NOTE: This was lifted wholesale from Sam and Cromwell.
 */

object Publishing {

  private val garBase = "artifactregistry://us-central1-maven.pkg.dev/dsp-artifact-registry/"

  private def garResolver(isSnapshot: Boolean): Resolver = {
    val repoType = if (isSnapshot) "snapshot" else "release"
    // Previously, the JFrog resolver included ;build.timestamp=$buildTimestamp in the URL.
    // If needed, consider including build metadata in the version string instead.
    val repoUrl = s"${garBase}libs-$repoType-standard"
    val repoName = "gar-publish"
    repoName at repoUrl
  }

  val publishSettings: Seq[Setting[_]] =
    // we only publish to libs-release-local (now libs-release-standard in GAR) because of a bug in sbt that makes
    // snapshots take priority over the local package cache.
    // see here: https://github.com/sbt/sbt/issues/2687#issuecomment-236586241
    Seq(
      publishTo := Option(garResolver(false)),
      Compile / publishArtifact := true,
      Test / publishArtifact := true,
    )

  val noPublishSettings: Seq[Setting[_]] =
    Seq(
      publish := {},
      publishLocal := {}
    )
}
