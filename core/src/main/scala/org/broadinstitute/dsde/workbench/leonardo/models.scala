package org.broadinstitute.dsde.workbench.leonardo

import java.net.URL
import java.time.Instant

import ca.mrvisser.sealerate
import cats.implicits._
import enumeratum.{Enum, EnumEntry}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{parseGcsPath, GcsPath}

import scala.collection.immutable
import scala.util.matching.Regex

final case class MachineType(value: String) extends AnyVal

sealed trait CloudService extends EnumEntry with Product with Serializable {
  def asString: String

  override def toString: String = asString //Enumeratum's withName function uses `toString` as key for lookup
}
object CloudService extends Enum[CloudService] {
  case object Dataproc extends CloudService {
    val asString: String = "DATAPROC"
  }
  case object GCE extends CloudService {
    val asString: String = "GCE"
  }

  override def values: immutable.IndexedSeq[CloudService] = findValues
}

final case class ClusterInternalId(asString: String) extends AnyVal

sealed trait RuntimeConfig extends Product with Serializable {
  def cloudService: CloudService
  def machineType: MachineType
  def diskSize: Int
}
object RuntimeConfig {
  final case class GceConfig(
    machineType: MachineType,
    diskSize: Int
  ) extends RuntimeConfig {
    val cloudService: CloudService = CloudService.GCE
  }

  final case class DataprocConfig(numberOfWorkers: Int,
                                  masterMachineType: String,
                                  masterDiskSize: Int, //min 10
                                  // worker settings are None when numberOfWorkers is 0
                                  workerMachineType: Option[String] = None,
                                  workerDiskSize: Option[Int] = None, //min 10
                                  numberOfWorkerLocalSSDs: Option[Int] = None, //min 0 max 8
                                  numberOfPreemptibleWorkers: Option[Int] = None)
      extends RuntimeConfig {
    val cloudService: CloudService = CloudService.Dataproc
    val machineType: MachineType = MachineType(masterMachineType)
    val diskSize: Int = masterDiskSize
  }
}

// Information about service accounts used by the cluster
final case class ServiceAccountInfo(clusterServiceAccount: Option[WorkbenchEmail],
                                    notebookServiceAccount: Option[WorkbenchEmail])

final case class ClusterError(errorMessage: String, errorCode: Int, timestamp: Instant)

sealed trait UserScriptPath extends Product with Serializable {
  def asString: String
}
object UserScriptPath {
  final case class Http(url: URL) extends UserScriptPath {
    val asString: String = url.toString
  }
  final case class Gcs(gcsPath: GcsPath) extends UserScriptPath {
    val asString: String = gcsPath.toUri
  }

  def stringToUserScriptPath(string: String): Either[Throwable, UserScriptPath] =
    parseGcsPath(string) match {
      case Right(value) => Right(Gcs(value))
      case Left(_)      => Either.catchNonFatal(new URL(string)).map(url => Http(url))
    }
}

final case class UserJupyterExtensionConfig(nbExtensions: Map[String, String] = Map.empty,
                                            serverExtensions: Map[String, String] = Map.empty,
                                            combinedExtensions: Map[String, String] = Map.empty,
                                            labExtensions: Map[String, String] = Map.empty) {

  def asLabels: Map[String, String] =
    nbExtensions ++ serverExtensions ++ combinedExtensions ++ labExtensions
}

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

sealed trait ClusterImageType extends EnumEntry with Serializable with Product
object ClusterImageType extends Enum[ClusterImageType] {
  val values = findValues

  case object Jupyter extends ClusterImageType
  case object RStudio extends ClusterImageType
  case object Welder extends ClusterImageType
  case object CustomDataProc extends ClusterImageType

  def stringToClusterImageType: Map[String, ClusterImageType] = values.map(c => c.toString -> c).toMap
}

final case class ClusterImage(imageType: ClusterImageType, imageUrl: String, timestamp: Instant)

final case class AuditInfo(creator: WorkbenchEmail,
                           createdDate: Instant,
                           destroyedDate: Option[Instant],
                           dateAccessed: Instant,
                           kernelFoundBusyDate: Option[Instant])
