package org.broadinstitute.dsde.workbench.leonardo
package util

import java.time.Instant

import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.Operation
import org.broadinstitute.dsde.workbench.google2.{DiskName, MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.CreateRuntimeMessage
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject, ServiceAccountKey}
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}

import scala.concurrent.duration.FiniteDuration

/**
 * Defines an algebra for manipulating Leo Runtimes.
 * Currently has interpreters for Dataproc and GCE.
 */
trait RuntimeAlgebra[F[_]] {
  def createRuntime(params: CreateRuntimeParams)(implicit ev: ApplicativeAsk[F, AppContext]): F[CreateRuntimeResponse]
  def getRuntimeStatus(params: GetRuntimeStatusParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[RuntimeStatus]
  def deleteRuntime(params: DeleteRuntimeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Option[Operation]]
  def finalizeDelete(params: FinalizeDeleteParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
  def stopRuntime(params: StopRuntimeParams)(implicit ev: ApplicativeAsk[F, AppContext]): F[Option[Operation]]
  def startRuntime(params: StartRuntimeParams)(implicit ev: ApplicativeAsk[F, AppContext]): F[Unit]
  def updateMachineType(params: UpdateMachineTypeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
  def updateDiskSize(params: UpdateDiskSizeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
  def resizeCluster(params: ResizeClusterParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
}

// Parameters
final case class CreateRuntimeParams(id: Long,
                                     runtimeProjectAndName: RuntimeProjectAndName,
                                     serviceAccountInfo: WorkbenchEmail,
                                     asyncRuntimeFields: Option[AsyncRuntimeFields],
                                     auditInfo: AuditInfo,
                                     jupyterUserScriptUri: Option[UserScriptPath],
                                     jupyterStartUserScriptUri: Option[UserScriptPath],
                                     userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                                     defaultClientId: Option[String],
                                     runtimeImages: Set[RuntimeImage],
                                     scopes: Set[String],
                                     welderEnabled: Boolean,
                                     customEnvironmentVariables: Map[String, String],
                                     runtimeConfig: RuntimeConfig)
object CreateRuntimeParams {
  def fromCreateRuntimeMessage(message: CreateRuntimeMessage): CreateRuntimeParams =
    CreateRuntimeParams(
      message.runtimeId,
      message.runtimeProjectAndName,
      message.serviceAccountInfo,
      message.asyncRuntimeFields,
      message.auditInfo,
      message.jupyterUserScriptUri,
      message.jupyterStartUserScriptUri,
      message.userJupyterExtensionConfig,
      message.defaultClientId,
      message.runtimeImages,
      message.scopes,
      message.welderEnabled,
      message.customEnvironmentVariables,
      message.runtimeConfig
    )
}
final case class CreateRuntimeResponse(asyncRuntimeFields: AsyncRuntimeFields,
                                       initBucket: GcsBucketName,
                                       serviceAccountKey: Option[ServiceAccountKey],
                                       customImage: CustomImage)
final case class GetRuntimeStatusParams(googleProject: GoogleProject,
                                        runtimeName: RuntimeName,
                                        zoneName: Option[ZoneName]) // zoneName is only needed for GCE
final case class DeleteRuntimeParams(googleProject: GoogleProject,
                                     runtimeName: RuntimeName,
                                     isAsyncRuntimeFields: Boolean,
                                     autoDeletePersistentDisk: Option[DiskName])
final case class FinalizeDeleteParams(runtime: Runtime)
final case class StopRuntimeParams(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig, now: Instant)
final case class StartRuntimeParams(runtime: Runtime, now: Instant)
final case class UpdateMachineTypeParams(runtime: Runtime, machineType: MachineTypeName, now: Instant)
final case class UpdateDiskSizeParams(runtime: Runtime, diskSize: DiskSize)
final case class ResizeClusterParams(runtime: Runtime, numWorkers: Option[Int], numPreemptibles: Option[Int])

// Configurations
sealed trait RuntimeInterpreterConfig {
  def welderConfig: WelderConfig
  def imageConfig: ImageConfig
  def proxyConfig: ProxyConfig
  def clusterResourcesConfig: ClusterResourcesConfig
  def clusterFilesConfig: ClusterFilesConfig
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
                                             clusterFilesConfig: ClusterFilesConfig,
                                             runtimeCreationTimeout: FiniteDuration)
      extends RuntimeInterpreterConfig

  final case class GceInterpreterConfig(gceConfig: GceConfig,
                                        welderConfig: WelderConfig,
                                        imageConfig: ImageConfig,
                                        proxyConfig: ProxyConfig,
                                        vpcConfig: VPCConfig,
                                        clusterResourcesConfig: ClusterResourcesConfig,
                                        clusterFilesConfig: ClusterFilesConfig,
                                        runtimeCreationTimeout: FiniteDuration)
      extends RuntimeInterpreterConfig
}
