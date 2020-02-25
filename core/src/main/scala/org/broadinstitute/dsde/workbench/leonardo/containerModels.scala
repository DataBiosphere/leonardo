package org.broadinstitute.dsde.workbench.leonardo

import ca.mrvisser.sealerate

import scala.util.matching.Regex

/** Container registry, e.g. GCR, Dockerhub */
sealed trait ContainerRegistry extends Product with Serializable {
  def regex: Regex
}
object ContainerRegistry {
  final case object GCR extends ContainerRegistry {
    val regex: Regex =
      """^((?:us\.|eu\.|asia\.)?gcr.io)/([\w.-]+/[\w.-]+)(?::(\w[\w.-]+))?(?:@([\w+.-]+:[A-Fa-f0-9]{32,}))?$""".r
    override def toString: String = "gcr"
  }

  // Repo format: https://docs.docker.com/docker-hub/repos/
  final case object DockerHub extends ContainerRegistry {
    val regex: Regex = """^([\w.-]+/[\w.-]+)(?::(\w[\w.-]+))?(?:@([\w+.-]+:[A-Fa-f0-9]{32,}))?$""".r
    override def toString: String = "docker hub"
  }

  val allRegistries: Set[ContainerRegistry] = sealerate.values[ContainerRegistry]
}

/** Information about an image including its URL and registry */
sealed trait ContainerImage extends Product with Serializable {
  def imageUrl: String
  def registry: ContainerRegistry
}
object ContainerImage {

  final case class GCR(imageUrl: String) extends ContainerImage {
    val registry: ContainerRegistry = ContainerRegistry.GCR
  }

  final case class DockerHub(imageUrl: String) extends ContainerImage {
    val registry: ContainerRegistry = ContainerRegistry.DockerHub
  }

  def stringToJupyterDockerImage(imageUrl: String): Option[ContainerImage] =
    List(ContainerRegistry.GCR, ContainerRegistry.DockerHub)
      .find(image => image.regex.pattern.asPredicate().test(imageUrl))
      .map { dockerRegistry =>
        dockerRegistry match {
          case ContainerRegistry.GCR       => ContainerImage.GCR(imageUrl)
          case ContainerRegistry.DockerHub => ContainerImage.DockerHub(imageUrl)
        }
      }
}
