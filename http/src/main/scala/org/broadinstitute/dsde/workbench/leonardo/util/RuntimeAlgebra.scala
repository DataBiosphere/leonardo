package org.broadinstitute.dsde.workbench.leonardo
package util

import java.time.Instant

import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.google2.{MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.config.{
  ClusterFilesConfig,
  ClusterResourcesConfig,
  DataprocConfig,
  GceConfig,
  GoogleGroupsConfig,
  ImageConfig,
  MonitorConfig,
  ProxyConfig,
  VPCConfig,
  WelderConfig
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.CreateRuntimeMessage
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsPath, GoogleProject, ServiceAccountKey}

/**
 * Defines an algebra for manipulating Leo Runtimes.
 * Currently has interpreters for Dataproc and GCE.
 */
trait RuntimeAlgebra[F[_]] {
  def createRuntime(params: CreateRuntimeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[CreateRuntimeResponse]
  def getRuntimeStatus(params: GetRuntimeStatusParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[RuntimeStatus]
  def deleteRuntime(params: DeleteRuntimeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
  def finalizeDelete(params: FinalizeDeleteParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
  def stopRuntime(params: StopRuntimeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
  def startRuntime(params: StartRuntimeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
  def updateMachineType(params: UpdateMachineTypeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
  def updateDiskSize(params: UpdateDiskSizeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
  def resizeCluster(params: ResizeClusterParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
}

// Parameters
final case class CreateRuntimeParams(id: Long,
                                     runtimeProjectAndName: RuntimeProjectAndName,
                                     serviceAccountInfo: ServiceAccountInfo,
                                     asyncRuntimeFields: Option[AsyncRuntimeFields],
                                     auditInfo: AuditInfo,
                                     jupyterExtensionUri: Option[GcsPath],
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
      message.id,
      message.runtimeProjectAndName,
      message.serviceAccountInfo,
      message.asyncRuntimeFields,
      message.auditInfo,
      message.jupyterExtensionUri,
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
final case class DeleteRuntimeParams(runtime: Runtime)
final case class FinalizeDeleteParams(runtime: Runtime)
final case class StopRuntimeParams(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig, now: Instant)
final case class StartRuntimeParams(runtime: Runtime, now: Instant)
final case class UpdateMachineTypeParams(runtime: Runtime, machineType: MachineTypeName, now: Instant)
final case class UpdateDiskSizeParams(runtime: Runtime, diskSize: Int)
final case class ResizeClusterParams(runtime: Runtime, numWorkers: Option[Int], numPreemptibles: Option[Int])

// Configurations
sealed trait RuntimeInterpreterConfig {
  def welderConfig: WelderConfig
  def imageConfig: ImageConfig
  def proxyConfig: ProxyConfig
  def clusterResourcesConfig: ClusterResourcesConfig
  def clusterFilesConfig: ClusterFilesConfig
  def monitorConfig: MonitorConfig
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
                                             monitorConfig: MonitorConfig)
      extends RuntimeInterpreterConfig

  final case class GceInterpreterConfig(gceConfig: GceConfig,
                                        welderConfig: WelderConfig,
                                        imageConfig: ImageConfig,
                                        proxyConfig: ProxyConfig,
                                        vpcConfig: VPCConfig,
                                        clusterResourcesConfig: ClusterResourcesConfig,
                                        clusterFilesConfig: ClusterFilesConfig,
                                        monitorConfig: MonitorConfig)
      extends RuntimeInterpreterConfig
}
