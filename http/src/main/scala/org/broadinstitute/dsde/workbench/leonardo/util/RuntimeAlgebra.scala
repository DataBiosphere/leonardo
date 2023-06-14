package org.broadinstitute.dsde.workbench.leonardo
package util

import cats.mtl.Ask
import com.google.api.gax.longrunning.OperationFuture
import com.google.cloud.compute.v1.Operation
import monocle.Prism
import org.broadinstitute.dsde.workbench.google2.{DiskName, MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.CreateRuntimeMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.RuntimeConfigInCreateRuntimeMessage
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/**
 * Defines an algebra for manipulating Leo Runtimes.
 * Currently has interpreters for Dataproc and GCE.
 */
trait RuntimeAlgebra[F[_]] {
  def createRuntime(params: CreateRuntimeParams)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[CreateGoogleRuntimeResponse]]
  def deleteRuntime(params: DeleteRuntimeParams)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[OperationFuture[Operation, Operation]]]
  def finalizeDelete(params: FinalizeDeleteParams)(implicit ev: Ask[F, AppContext]): F[Unit]
  def stopRuntime(params: StopRuntimeParams)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[OperationFuture[Operation, Operation]]]
  def startRuntime(params: StartRuntimeParams)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[OperationFuture[Operation, Operation]]]
  def updateMachineType(params: UpdateMachineTypeParams)(implicit ev: Ask[F, AppContext]): F[Unit]
  def updateDiskSize(params: UpdateDiskSizeParams)(implicit ev: Ask[F, AppContext]): F[Unit]
  def resizeCluster(params: ResizeClusterParams)(implicit ev: Ask[F, AppContext]): F[Unit]
}

// Parameters
final case class CreateRuntimeParams(id: Long,
                                     runtimeProjectAndName: RuntimeProjectAndName,
                                     serviceAccountInfo: WorkbenchEmail,
                                     asyncRuntimeFields: Option[AsyncRuntimeFields],
                                     auditInfo: AuditInfo,
                                     userScriptUri: Option[UserScriptPath],
                                     startUserScriptUri: Option[UserScriptPath],
                                     userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                                     defaultClientId: Option[String],
                                     runtimeImages: Set[RuntimeImage],
                                     scopes: Set[String],
                                     welderEnabled: Boolean,
                                     customEnvironmentVariables: Map[String, String],
                                     runtimeConfig: RuntimeConfigInCreateRuntimeMessage
)
object CreateRuntimeParams {
  def fromCreateRuntimeMessage(message: CreateRuntimeMessage): CreateRuntimeParams =
    CreateRuntimeParams(
      message.runtimeId,
      message.runtimeProjectAndName,
      message.serviceAccountInfo,
      message.asyncRuntimeFields,
      message.auditInfo,
      message.userScriptUri,
      message.startUserScriptUri,
      message.userJupyterExtensionConfig,
      message.defaultClientId,
      message.runtimeImages,
      message.scopes,
      message.welderEnabled,
      message.customEnvironmentVariables,
      message.runtimeConfig
    )
}

sealed trait BootSource extends Product with Serializable {
  def asString: String
}
object BootSource {
  final case class VmImage(customImage: CustomImage) extends BootSource {
    def asString: String = customImage.asString
  }
}
final case class CreateGoogleRuntimeResponse(asyncRuntimeFields: AsyncRuntimeFields,
                                             initBucket: GcsBucketName,
                                             bootSource: BootSource
)
final case class DeleteRuntimeParams(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                                     masterInstance: Option[DataprocInstance]
)
final case class FinalizeDeleteParams(runtime: Runtime)
final case class StopRuntimeParams(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                                   now: Instant,
                                   isDataprocFullStop: Boolean
)
final case class StartRuntimeParams(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig, initBucket: GcsBucketName)
final case class UpdateMachineTypeParams(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                                         machineType: MachineTypeName,
                                         now: Instant
)

sealed trait UpdateDiskSizeParams extends Product with Serializable
object UpdateDiskSizeParams {
  final case class Dataproc(diskSize: DiskSize, masterDataprocInstance: DataprocInstance) extends UpdateDiskSizeParams
  final case class Gce(googleProject: GoogleProject, diskName: DiskName, diskSize: DiskSize, zone: ZoneName)
      extends UpdateDiskSizeParams

  val dataprocPrism = Prism[UpdateDiskSizeParams, Dataproc] {
    case x: Dataproc => Some(x)
    case _           => None
  }(identity)

  val gcePrism = Prism[UpdateDiskSizeParams, Gce] {
    case x: Gce => Some(x)
    case _      => None
  }(identity)
}

final case class ResizeClusterParams(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                                     numWorkers: Option[Int],
                                     numPreemptibles: Option[Int]
)

// Configurations
sealed trait RuntimeInterpreterConfig {
  def welderConfig: WelderConfig
  def imageConfig: ImageConfig
  def proxyConfig: ProxyConfig
  def clusterResourcesConfig: ClusterResourcesConfig
  def clusterFilesConfig: SecurityFilesConfig
  def runtimeCreationTimeout: FiniteDuration
}
object RuntimeInterpreterConfig {
  final case class DataprocInterpreterConfig(dataprocConfig: DataprocConfig,
                                             groupsConfig: GoogleGroupsConfig,
                                             welderConfig: WelderConfig,
                                             imageConfig: ImageConfig,
                                             proxyConfig: ProxyConfig,
                                             vpcConfig: VPCConfig,
                                             clusterResourcesConfig: ClusterResourcesConfig,
                                             clusterFilesConfig: SecurityFilesConfig,
                                             runtimeCreationTimeout: FiniteDuration
  ) extends RuntimeInterpreterConfig

  final case class GceInterpreterConfig(gceConfig: GceConfig,
                                        welderConfig: WelderConfig,
                                        imageConfig: ImageConfig,
                                        proxyConfig: ProxyConfig,
                                        vpcConfig: VPCConfig,
                                        clusterResourcesConfig: ClusterResourcesConfig,
                                        clusterFilesConfig: SecurityFilesConfig,
                                        runtimeCreationTimeout: FiniteDuration
  ) extends RuntimeInterpreterConfig
}
