package org.broadinstitute.dsde.workbench.leonardo.util

import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, GoogleGroupsConfig, ImageConfig, MonitorConfig, ProxyConfig, WelderConfig}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.CreateCluster
import org.broadinstitute.dsde.workbench.leonardo.{Runtime, RuntimeConfig}
import org.broadinstitute.dsde.workbench.model.TraceId

/**
 * Defines an algebra for manipulating Leo runtimes. Currently has interpreters for Dataproc and GCE.
 */
trait RuntimeAlgebra[F[_]] {
  def createRuntime(params: CreateCluster)(implicit ev: ApplicativeAsk[F, TraceId]): F[CreateClusterResponse]
  def deleteRuntime(params: DeleteRuntimeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
  def finalizeDelete(params: FinalizeDeleteParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
  def stopRuntime(params: StopRuntimeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
  def startRuntime(params: StartRuntimeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
  def updateMachineType(params: UpdateMachineTypeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
  def updateDiskSize(params: UpdateDiskSizeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
}

/**
 * Extends RuntimeAlgebra for Dataproc-specific functionality.
 */
trait DataprocAlgebra[F[_]] extends RuntimeAlgebra[F] {
  def resizeCluster(params: ResizeClusterParams): F[Unit]
}

// Parameters
final case class DeleteRuntimeParams(runtime: Runtime)
final case class FinalizeDeleteParams(runtime: Runtime)
final case class StopRuntimeParams(runtime: Runtime, runtimeConfig: RuntimeConfig)
final case class StartRuntimeParams(runtime: Runtime)
final case class UpdateMachineTypeParams(runtime: Runtime, machineType: MachineTypeName)
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
                                             clusterResourcesConfig: ClusterResourcesConfig,
                                             clusterFilesConfig: ClusterFilesConfig,
                                             monitorConfig: MonitorConfig) extends RuntimeInterpreterConfig
}