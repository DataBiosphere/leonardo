package org.broadinstitute.dsde.workbench.leonardo

import ca.mrvisser.sealerate
import enumeratum.{Enum, EnumEntry}

import scala.util.matching.Regex

/** Container registry, e.g. Google Container Repository, GitHub Container Repository, Dockerhub */
sealed trait ContainerRegistry extends EnumEntry with Product with Serializable {
  def regex: Regex
}
object ContainerRegistry extends Enum[ContainerRegistry] {
  val values = findValues

  final case object GCR extends ContainerRegistry {
    val regex: Regex =
      """^((?:us\.|eu\.|asia\.)?gcr.io)/([\w.-]+/[\w.-]+)(?::(\w[\w.-]+))?(?:@([\w+.-]+:[A-Fa-f0-9]{32,}))?$""".r
    override def toString: String = "GCR"
  }

  final case object GAR extends ContainerRegistry {
    val regex: Regex =
      """^([a-z]+(?:-[a-z]+\d?)?-docker.pkg.dev)/([\w.-]+/[\w.-]+/[\w.-]+)(?::(\w[\w.-]+))?(?:@([\w+.-]+:[A-Fa-f0-9]{32,}))?$""".r
    override def toString: String = "GAR"
  }

  final case object GHCR extends ContainerRegistry {
    val regex: Regex =
      """^(ghcr.io)/((?:[\w.-]+/)+[\w.-]+)(?::(\w[\w.-]+))?(?:@([\w+.-]+:[A-Fa-f0-9]{32,}))?$""".r
    override def toString: String = "GHCR"
  }

  // Repo format: https://docs.docker.com/docker-hub/repos/
  final case object DockerHub extends ContainerRegistry {
    val regex: Regex = """^([\w.-]+/[\w.-]+)(?::(\w[\w.-]+))?(?:@([\w+.-]+:[A-Fa-f0-9]{32,}))?$""".r
    override def toString: String = "DockerHub"
  }

  def inferRegistry(imageUrl: String): Option[ContainerRegistry] =
    allRegistries
      .find(image => image.regex.pattern.asPredicate().test(imageUrl))

  val allRegistries: Set[ContainerRegistry] = sealerate.values[ContainerRegistry]
}

/** Information about an image including its URL and registry */
final case class ContainerImage(imageUrl: String, registry: ContainerRegistry)

object ContainerImage {
  def fromImageUrl(imageUrl: String): Option[ContainerImage] = {
    val registryOpt = ContainerRegistry.inferRegistry(imageUrl)
    registryOpt.map(r => ContainerImage(imageUrl, r))
  }
  def fromPrefixAndHash(prefix: String, hash: String): Option[ContainerImage] = {
    val imageUrl = prefix + ":" + hash
    val registryOpt = ContainerRegistry.inferRegistry(imageUrl)
    registryOpt.map(r => ContainerImage(imageUrl, r))
  }
}
