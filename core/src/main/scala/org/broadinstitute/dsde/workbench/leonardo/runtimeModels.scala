package org.broadinstitute.dsde.workbench.leonardo

import cats.implicits._
import enumeratum.{Enum, EnumEntry}
import monocle.Prism
import org.broadinstitute.dsde.workbench.google2.{MachineTypeName, OperationName, RegionName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.RuntimeContainerServiceType.JupyterService
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{BootSource, Jupyter, RStudio, Welder}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId._
import org.broadinstitute.dsde.workbench.model.google.{parseGcsPath, GcsBucketName, GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{IP, TraceId, ValueObject, WorkbenchEmail}
import org.http4s.Uri

import java.net.URL
import java.nio.file.Path
import java.time.Instant
import scala.collection.immutable

/**
 * This file contains models for Leonardo runtimes.
 */
/** The runtime itself */
final case class Runtime(id: Long,
                         workspaceId: Option[WorkspaceId],
                         samResource: RuntimeSamResourceId,
                         runtimeName: RuntimeName,
                         cloudContext: CloudContext,
                         serviceAccount: WorkbenchEmail,
                         asyncRuntimeFields: Option[AsyncRuntimeFields],
                         auditInfo: AuditInfo,
                         kernelFoundBusyDate: Option[Instant],
                         proxyUrl: URL,
                         status: RuntimeStatus,
                         labels: LabelMap,
                         userScriptUri: Option[UserScriptPath],
                         startUserScriptUri: Option[UserScriptPath],
                         errors: List[RuntimeError],
                         userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                         autopauseThreshold: Int,
                         defaultClientId: Option[String],
                         allowStop: Boolean,
                         runtimeImages: Set[RuntimeImage],
                         scopes: Set[String],
                         welderEnabled: Boolean,
                         customEnvironmentVariables: Map[String, String],
                         runtimeConfigId: RuntimeConfigId,
                         patchInProgress: Boolean
) {
  def projectNameString: String = s"${cloudContext.asStringWithProvider}/${runtimeName.asString}"
}

object Runtime {
  def getProxyUrl(urlBase: String,
                  cloudContext: CloudContext,
                  runtimeName: RuntimeName,
                  runtimeImages: Set[RuntimeImage],
                  hostIp: Option[IP],
                  labels: Map[String, String]
  ): URL = {
    val tool = runtimeImages
      .map(_.imageType)
      .filterNot(Set(Welder, BootSource).contains)
      .headOption
      .orElse(labels.get("tool").flatMap(RuntimeImageType.withNameInsensitiveOption))
      .flatMap(t => RuntimeContainerServiceType.imageTypeToRuntimeContainerServiceType.get(t))
      .headOption
      .getOrElse(JupyterService)

    cloudContext match {
      case _: CloudContext.Gcp =>
        new URL(
          urlBase + cloudContext.asString + "/" + runtimeName.asString + "/" + tool.proxySegment
        )
      case _: CloudContext.Azure =>
        hostIp.fold(new URL("https://relay-not-defined-yet"))(s =>
          new URL(s"https://${s.asString}/${runtimeName.asString}")
        )
    }
  }
}

/** Runtime status enum */
sealed trait RuntimeStatus extends EnumEntry with Product with Serializable {
  // end status if it is a transition status. For instance, terminalStatus for `Starting` is `Running`
  def terminalStatus: Option[RuntimeStatus]
}
object RuntimeStatus extends Enum[RuntimeStatus] {
  val values = findValues
  // Leonardo defined runtime statuses.

  // These statuses exist when we save a runtime during an API request, but we haven't published the event to back leo
  case object PreCreating extends RuntimeStatus {
    override def terminalStatus: Option[RuntimeStatus] = Some(Running)
  }
  case object PreDeleting extends RuntimeStatus {
    override def terminalStatus: Option[RuntimeStatus] = Some(Deleted)
  }
  case object PreStarting extends RuntimeStatus {
    override def terminalStatus: Option[RuntimeStatus] = Some(Running)
  }
  case object PreStopping extends RuntimeStatus {
    override def terminalStatus: Option[RuntimeStatus] = Some(Stopped)
  }

  // NOTE: Remember to update the definition of this enum in Swagger when you add new ones
  case object Creating extends RuntimeStatus {
    override def terminalStatus: Option[RuntimeStatus] = Some(Running)
  }
  case object Updating extends RuntimeStatus {
    override def terminalStatus: Option[RuntimeStatus] = Some(Running)
  } // only for dataproc status
  case object Deleting extends RuntimeStatus {
    override def terminalStatus: Option[RuntimeStatus] = Some(Deleted)
  }
  case object Starting extends RuntimeStatus {
    override def terminalStatus: Option[RuntimeStatus] = Some(Running)
  }
  case object Stopping extends RuntimeStatus {
    override def terminalStatus: Option[RuntimeStatus] = Some(Stopped)
  }

  case object Running extends RuntimeStatus {
    override def terminalStatus: Option[RuntimeStatus] = None
  }
  case object Error extends RuntimeStatus {
    override def terminalStatus: Option[RuntimeStatus] = None
  }
  case object Unknown extends RuntimeStatus {
    override def terminalStatus: Option[RuntimeStatus] = None
  }
  case object Stopped extends RuntimeStatus {
    override def terminalStatus: Option[RuntimeStatus] = None
  }
  case object Deleted extends RuntimeStatus {
    override def terminalStatus: Option[RuntimeStatus] = None
  }

  def fromDataprocClusterStatus(dataprocClusterStatus: DataprocClusterStatus): RuntimeStatus =
    dataprocClusterStatus match {
      case DataprocClusterStatus.Creating => Creating
      case DataprocClusterStatus.Deleting => Deleting
      case DataprocClusterStatus.Error    => Error
      case DataprocClusterStatus.Running  => Running
      case DataprocClusterStatus.Unknown  => Unknown
      case DataprocClusterStatus.Updating => Updating
      case DataprocClusterStatus.Stopped  => Stopped
      case DataprocClusterStatus.Starting => Starting
      case DataprocClusterStatus.Stopping => Stopping
    }

  def fromGceInstanceStatus(gceInstanceStatus: GceInstanceStatus): RuntimeStatus =
    gceInstanceStatus match {
      case GceInstanceStatus.Provisioning => Creating
      case GceInstanceStatus.Staging      => Creating
      case GceInstanceStatus.Running      => Running
      case GceInstanceStatus.Stopping     => Stopping
      case GceInstanceStatus.Stopped      => Stopped
      case GceInstanceStatus.Suspending   => Stopping
      case GceInstanceStatus.Suspended    => Stopped
      case GceInstanceStatus.Terminated   => Stopped
    }

  // A user might need to connect to this notebook in the future. Keep it warm in the DNS cache.
  val activeStatuses: Set[RuntimeStatus] =
    Set(Unknown, Creating, Running, Updating, Stopping, Stopped, Starting)

  // Can a user delete this runtime? Contains everything except Creating, Deleting, Deleted.
  val deletableStatuses: Set[RuntimeStatus] =
    Set(Unknown, Running, Updating, Error, Stopping, Stopped, Starting)

  // Non-terminal statuses. Requires monitoring via ClusterMonitorActor.
  val monitoredStatuses: Set[RuntimeStatus] = Set(Unknown, Creating, Updating, Deleting, Stopping, Starting)

  // Can a user stop this runtime?
  val stoppableStatuses: Set[RuntimeStatus] = Set(Unknown, Running, Updating, Starting)

  // Can a user start this runtime?
  val startableStatuses: Set[RuntimeStatus] = Set(Stopped, Stopping)

  // A runtime transitioning to Stopped
  val stoppingStatuses: Set[RuntimeStatus] = Set(PreStopping, Stopping, Stopped)

  // Can a user update (i.e. resize) this runtime?
  val updatableStatuses: Set[RuntimeStatus] = Set(Running, Stopped)

  implicit class EnrichedRuntimeStatus(status: RuntimeStatus) {
    def isActive: Boolean = activeStatuses contains status
    def isDeletable: Boolean = deletableStatuses contains status
    def isMonitored: Boolean = monitoredStatuses contains status
    def isStoppable: Boolean = stoppableStatuses contains status
    def isStartable: Boolean = startableStatuses contains status
    def isUpdatable: Boolean = updatableStatuses contains status
    def isStopping: Boolean = stoppingStatuses contains status
  }
}

/** Fields that are populated asynchronous to the runtime's creation */
case class AsyncRuntimeFields(proxyHostName: ProxyHostName,
                              operationName: OperationName,
                              stagingBucket: GcsBucketName,
                              hostIp: Option[IP]
)

/** The cloud environment of the runtime, e.g. Dataproc, GCE. */
sealed trait CloudService extends EnumEntry with Product with Serializable {
  def asString: String

  override def toString: String = asString // Enumeratum's withName function uses `toString` as key for lookup
}
object CloudService extends Enum[CloudService] {
  case object Dataproc extends CloudService {
    val asString: String = "DATAPROC"
  }
  case object GCE extends CloudService {
    val asString: String = "GCE"
  }

  case object AzureVm extends CloudService {
    val asString: String = "AZURE_VM"
  }

  override def values: immutable.IndexedSeq[CloudService] = findValues
}

sealed trait CustomImage extends Product with Serializable {
  def asString: String
  def cloudService: CloudService

  override val toString = asString
}
object CustomImage {
  final case class DataprocCustomImage(asString: String) extends CustomImage {
    val cloudService = CloudService.Dataproc
  }
  final case class GceCustomImage(asString: String) extends CustomImage {
    val cloudService = CloudService.GCE
  }
}

/** Configuration of the runtime such as machine types, disk size, etc */
sealed trait RuntimeConfigType extends EnumEntry with Product with Serializable {
  def asString: String

  override def toString: String = asString // Enumeratum's withName function uses `toString` as key for lookup
}
object RuntimeConfigType extends Enum[RuntimeConfigType] {
  case object Dataproc extends RuntimeConfigType {
    val asString = "Dataproc"
  }

  case object GceConfig extends RuntimeConfigType {
    val asString = "GceConfig"
  }

  case object GceWithPdConfig extends RuntimeConfigType {
    val asString = "GceWithPdConfig"
  }

  case object AzureVmConfig extends RuntimeConfigType {
    val asString = "AzureVmConfig"
  }

  override def values: immutable.IndexedSeq[RuntimeConfigType] = findValues
}
final case class RuntimeConfigId(id: Long) extends AnyVal
sealed trait RuntimeConfig extends Product with Serializable {
  def cloudService: CloudService
  def machineType: MachineTypeName

  def configType: RuntimeConfigType
}
object RuntimeConfig {
  final case class GceConfig(
    machineType: MachineTypeName,
    diskSize: DiskSize,
    bootDiskSize: Option[
      DiskSize
    ], // This is optional for supporting old runtimes which only have 1 disk. All new runtime will have a boot disk
    zone: ZoneName,
    gpuConfig: Option[GpuConfig] // This is optional since not all runtimes use gpus
  ) extends RuntimeConfig {
    val cloudService: CloudService = CloudService.GCE
    val configType: RuntimeConfigType = RuntimeConfigType.GceConfig
  }

  // When persistentDiskId is None, then we don't have any disk attached to the runtime
  final case class GceWithPdConfig(machineType: MachineTypeName,
                                   persistentDiskId: Option[DiskId],
                                   bootDiskSize: DiskSize,
                                   zone: ZoneName,
                                   gpuConfig: Option[GpuConfig]
  ) extends RuntimeConfig {
    val cloudService: CloudService = CloudService.GCE
    val configType: RuntimeConfigType = RuntimeConfigType.GceWithPdConfig
  }

  final case class DataprocConfig(numberOfWorkers: Int,
                                  masterMachineType: MachineTypeName,
                                  masterDiskSize: DiskSize, // min 10
                                  // worker settings are None when numberOfWorkers is 0
                                  workerMachineType: Option[MachineTypeName] = None,
                                  workerDiskSize: Option[DiskSize] = None, // min 10
                                  numberOfWorkerLocalSSDs: Option[Int] = None, // min 0 max 8
                                  numberOfPreemptibleWorkers: Option[Int] = None,
                                  properties: Map[String, String],
                                  region: RegionName,
                                  componentGatewayEnabled: Boolean,
                                  workerPrivateAccess: Boolean
  ) extends RuntimeConfig {
    val cloudService: CloudService = CloudService.Dataproc
    val machineType: MachineTypeName = masterMachineType
    val diskSize: DiskSize = masterDiskSize
    val configType: RuntimeConfigType = RuntimeConfigType.Dataproc
  }

  // Azure machineType maps to `com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes`
  final case class AzureConfig(machineType: MachineTypeName,
                               persistentDiskId: Option[DiskId],
                               region: Option[RegionName]
  ) extends RuntimeConfig {
    val cloudService: CloudService = CloudService.AzureVm
    val configType: RuntimeConfigType = RuntimeConfigType.AzureVmConfig
  }
}

/** Runtime user script */
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

  val gcsPrism = Prism[UserScriptPath, UserScriptPath.Gcs] {
    case x: UserScriptPath.Gcs  => Some(x)
    case UserScriptPath.Http(_) => None
  }(identity)

  /**
   * @param additionalValidate: checking if the string is a valid URI with org.http4s.Uri.fromString
   */
  def stringToUserScriptPath(string: String, additionalValidate: Boolean = true): Either[Throwable, UserScriptPath] =
    parseGcsPath(string) match {
      case Right(value) => Right(Gcs(value))
      case Left(_) =>
        for {
          _ <- if (additionalValidate) Uri.fromString(string) else Right(())
          res <- Either.catchNonFatal(new URL(string))
        } yield UserScriptPath.Http(res)
    }
}

/** Jupyter extension configuration */
final case class UserJupyterExtensionConfig(nbExtensions: Map[String, String] = Map.empty,
                                            serverExtensions: Map[String, String] = Map.empty,
                                            combinedExtensions: Map[String, String] = Map.empty,
                                            labExtensions: Map[String, String] = Map.empty
) {

  def asLabels: Map[String, String] =
    nbExtensions ++ serverExtensions ++ combinedExtensions ++ labExtensions
}

/** Types of Jupyter extensions */
sealed trait ExtensionType extends EnumEntry
object ExtensionType extends Enum[ExtensionType] {
  val values = findValues

  case object NBExtension extends ExtensionType
  case object ServerExtension extends ExtensionType
  case object CombinedExtension extends ExtensionType
  case object LabExtension extends ExtensionType
}

/** Types of images that can be deployed to a runtime */
sealed trait RuntimeImageType extends EnumEntry with Serializable with Product
object RuntimeImageType extends Enum[RuntimeImageType] {
  val values = findValues

  case object Jupyter extends RuntimeImageType
  case object RStudio extends RuntimeImageType
  case object Welder extends RuntimeImageType
  case object Listener extends RuntimeImageType
  // This is not strictly an image type. It can either be a custom VM image for dataproc,
  // or boot disk snapshot for GCE VMs
  case object BootSource extends RuntimeImageType
  case object Proxy extends RuntimeImageType
  case object CryptoDetector extends RuntimeImageType

  case object Azure extends RuntimeImageType

  def stringToRuntimeImageType: Map[String, RuntimeImageType] = values.map(c => c.toString -> c).toMap
}

/** Types of container services that can be deployed to a runtime */
sealed trait RuntimeContainerServiceType extends EnumEntry with Serializable with Product {
  def imageType: RuntimeImageType
  def proxySegment: String
}
object RuntimeContainerServiceType extends Enum[RuntimeContainerServiceType] {
  val values = findValues
  val imageTypeToRuntimeContainerServiceType: Map[RuntimeImageType, RuntimeContainerServiceType] =
    values.toList.map(v => v.imageType -> v).toMap ++ Map(RuntimeImageType.Azure -> JupyterService)
  case object JupyterService extends RuntimeContainerServiceType {
    override def imageType: RuntimeImageType = Jupyter
    override def proxySegment: String = "jupyter"
  }
  case object RStudioService extends RuntimeContainerServiceType {
    override def imageType: RuntimeImageType = RStudio
    override def proxySegment: String = "rstudio"
  }
  case object WelderService extends RuntimeContainerServiceType {
    override def imageType: RuntimeImageType = Welder
    override def proxySegment: String = "welder"
  }
}

/** Information about an image running on a runtime */
final case class RuntimeImage(imageType: RuntimeImageType,
                              imageUrl: String,
                              homeDirectory: Option[Path], // this is only used and populated for jupyter image
                              timestamp: Instant
) {
  def hash: Option[String] = {
    val splitUrl = imageUrl.split(":")
    if (splitUrl isDefinedAt 1) Some(splitUrl(1)) else None
  }
  def registry: Option[ContainerRegistry] = ContainerRegistry.inferRegistry(imageUrl)
}

/** Audit information about a runtime */
final case class AuditInfo(creator: WorkbenchEmail,
                           createdDate: Instant,
                           destroyedDate: Option[Instant],
                           dateAccessed: Instant
)

/** UIs that can be used to access a runtime */
sealed trait RuntimeUI extends Product with Serializable {
  def asString: String
}
object RuntimeUI {
  final case object Terra extends RuntimeUI {
    override def asString: String = "Terra"
  }
  final case object AoU extends RuntimeUI {
    override def asString: String = "AoU"
  }
  final case object Other extends RuntimeUI {
    override def asString: String = "Other"
  }
}

//This ADT is how some of the UI logic is resolved. Only used in Labels table.
sealed trait Tool extends EnumEntry with Product with Serializable {
  def asString: String

  override def toString: String = asString
}
object Tool extends Enum[Tool] {
  case object RStudio extends Tool {
    val asString: String = "RStudio"
  }
  case object Jupyter extends Tool {
    val asString: String = "Jupyter"
  }
  case object JupyterLab extends Tool {
    val asString: String = "JupyterLab"
  }

  override def values: immutable.IndexedSeq[Tool] = findValues
}

/** Default runtime labels */
case class DefaultRuntimeLabels(runtimeName: RuntimeName,
                                googleProject: Option[GoogleProject],
                                cloudContext: CloudContext,
                                creator: WorkbenchEmail,
                                serviceAccount: Option[WorkbenchEmail],
                                userScript: Option[UserScriptPath],
                                startUserScript: Option[UserScriptPath],
                                tool: Option[Tool]
) {
  def toMap: LabelMap =
    Map(
      "runtimeName" -> runtimeName.asString,
      "clusterName" -> runtimeName.asString, // TODO: potentially deprecate this once clients moves away from using this label (deprecated 3/5/2020)
      "googleProject" -> googleProject
        .map(_.value)
        .getOrElse(
          null
        ), // TODO: potentially deprecate this once clients moves away from using this label (deprecated 3/5/2020)
      "cloudContext" -> cloudContext.asStringWithProvider,
      "creator" -> creator.value,
      "clusterServiceAccount" -> serviceAccount.map(_.value).getOrElse(null),
      "userScript" -> userScript.map(_.asString).getOrElse(null),
      "startUserScript" -> startUserScript.map(_.asString).getOrElse(null),
      "tool" -> tool.map(_.toString).getOrElse(null)
    ).filterNot(_._2 == null)
}

object DefaultRuntimeLabels {
  // Creating a dummy instance to obtain the default label keys
  val defaultLabelKeys = DefaultRuntimeLabels(RuntimeName(""),
                                              Some(GoogleProject("")),
                                              CloudContext.Gcp(GoogleProject("")),
                                              WorkbenchEmail(""),
                                              Some(WorkbenchEmail("")),
                                              None,
                                              None,
                                              None
  ).toMap.keySet
}

/** Welder operations */
sealed trait WelderAction extends EnumEntry
object WelderAction extends Enum[WelderAction] {
  val values = findValues

  case object UpdateWelder extends WelderAction
  case object DisableDelocalization extends WelderAction
}

final case class MemorySize(bytes: Long) extends AnyVal {
  override def toString: String = bytes.toString + "b"
}
object MemorySize {
  val kbInBytes = 1024
  val mbInBytes = 1048576
  val gbInBytes = 1073741824

  def fromKb(kb: Double): MemorySize = MemorySize((kb * kbInBytes).toLong)
  def fromMb(mb: Double): MemorySize = MemorySize((mb * mbInBytes).toLong)
  def fromGb(gb: Double): MemorySize = MemorySize((gb * gbInBytes).toLong)
}

/**
 * Resource constraints for a runtime.
 * See https://docs.docker.com/compose/compose-file/compose-file-v2/#cpu-and-other-resources
 * for other types of resources we may want to add here.
 *
 * driverMemory will be populated if it's Dataproc runtime.
 * 
 * Note that the memory limit includes all the sub-procesess of the Notebook server including the
 * Notebook kernel and the Spark driver process, if any.
 */
final case class RuntimeResourceConstraints(memoryLimit: MemorySize,
                                            totalMachineMemory: MemorySize,
                                            driverMemory: Option[MemorySize]
)

final case class RuntimeMetrics(cloudContext: CloudContext,
                                runtimeName: RuntimeName,
                                status: RuntimeStatus,
                                workspaceId: Option[WorkspaceId],
                                images: Set[RuntimeImage],
                                labels: LabelMap
)

final case class RuntimeName(asString: String) extends AnyVal
final case class RuntimeError(errorMessage: String,
                              errorCode: Option[Int],
                              timestamp: Instant,
                              traceId: Option[TraceId] = None
)

/**
 * @param longMessage: error message we use in logs to provide details for debugging purpose
 * @param code: error code that comes from GCP
 * @param shortMessage: This is mainly used for labels in runtime creation failure metrics today
 * @param labels: leonardo labels for a runtime
 */
final case class RuntimeErrorDetails(longMessage: String,
                                     code: Option[Int] = None,
                                     shortMessage: Option[String] = None,
                                     labels: Map[String, String] = Map.empty
)
final case class RuntimeResource(asString: String) extends AnyVal
final case class RuntimeProjectAndName(cloudContext: CloudContext, runtimeName: RuntimeName) {
  override def toString: String = s"${cloudContext.asString}/${runtimeName.asString}"
}
final case class RuntimeAndRuntimeConfig(runtime: Runtime, runtimeConfig: RuntimeConfig)
final case class IpRange(value: String) extends AnyVal
final case class NetworkTag(value: String) extends ValueObject
final case class GoogleOperation(name: OperationName, id: ProxyHostName)
final case class ProxyHostName(value: String) extends AnyVal

sealed trait RuntimeOperation extends Product with Serializable {
  def asString: String
  final override def toString = asString
}
object RuntimeOperation {
  final case object Creating extends RuntimeOperation {
    val asString = "creating"
  }
  final case object Restarting extends RuntimeOperation {
    val asString = "restarting"
  }
  final case object Stopping extends RuntimeOperation {
    val asString = "stopping"
  }
}
