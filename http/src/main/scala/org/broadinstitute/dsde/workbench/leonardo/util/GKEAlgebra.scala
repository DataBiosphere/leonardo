package org.broadinstitute.dsde.workbench.leonardo.util

import cats.mtl.Ask
import com.google.cloud.compute.v1.Disk
import org.broadinstitute.dsde.workbench.google2.{DiskName, ZoneName}
import org.broadinstitute.dsde.workbench.google2.GKEModels.{
  KubernetesNetwork,
  KubernetesOperationId,
  KubernetesSubNetwork
}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.NamespaceName
import org.broadinstitute.dsde.workbench.leonardo.config.GalaxyDiskConfig
import org.broadinstitute.dsde.workbench.leonardo.{
  AppContext,
  AppId,
  AppMachineType,
  AppName,
  KubernetesClusterLeoId,
  NodepoolLeoId
}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsp.ChartVersion

trait GKEAlgebra[F[_]] {

  /** Creates a GKE cluster but doesn't wait for its completion. */
  def createCluster(params: CreateClusterParams)(implicit ev: Ask[F, AppContext]): F[Option[CreateClusterResult]]

  /**
   * Polls a creating GKE cluster for its completion and also does other cluster-wide set-up like
   * install nginx ingress controller.
   */
  def pollCluster(params: PollClusterParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  /**
   * Creates a GKE nodepool and polls it for completion.
   */
  def createAndPollNodepool(params: CreateNodepoolParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  /**
   * Creates an app and polls it for completion
   * Update startTime for app usage tracking for certain apps.
   */
  def createAndPollApp(params: CreateAppParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  /**
   * Deletes a cluster and polls for completion
   */
  def deleteAndPollCluster(params: DeleteClusterParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  /**
   * Deletes a nodepool and polls for completion
   */
  def deleteAndPollNodepool(params: DeleteNodepoolParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  /**
   * Deletes an app and polls for completion
   * Update stopTime for app usage tracking for certain apps.
   */
  def deleteAndPollApp(params: DeleteAppParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  /**
   * Stops an app and polls for completion
   * Update stopTime for app usage tracking for certain apps.
   */
  def stopAndPollApp(params: StopAppParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  /**
   * Starts an app and polls for completion
   * Update startTime for app usage tracking for certain apps.
   */
  def startAndPollApp(params: StartAppParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  def updateAndPollApp(params: UpdateAppParams)(implicit ev: Ask[F, AppContext]): F[Unit]
}

object GKEAlgebra {
  import scala.jdk.CollectionConverters._

  private[leonardo] def getOldStyleGalaxyPostgresDiskName(namespaceName: NamespaceName, suffix: String): DiskName =
    DiskName(s"${namespaceName.value}-${suffix}")

  private[leonardo] def getGalaxyPostgresDiskName(dataDiskName: DiskName, suffix: String): DiskName =
    DiskName(s"${dataDiskName.value}-${suffix}")

  private[leonardo] def buildGalaxyPostgresDisk(zone: ZoneName,
                                                dataDiskName: DiskName,
                                                galaxyDiskConfig: GalaxyDiskConfig
  ): Disk =
    Disk
      .newBuilder()
      .setName(getGalaxyPostgresDiskName(dataDiskName, galaxyDiskConfig.postgresDiskNameSuffix).value)
      .setZone(zone.value)
      .setSizeGb(galaxyDiskConfig.postgresDiskSizeGB.gb)
      .setPhysicalBlockSizeBytes(galaxyDiskConfig.postgresDiskBlockSize.bytes)
      .putAllLabels(Map("leonardo" -> "true").asJava)
      .build()

  // Utility for building staging bucket name for apps.
  // Staging bucket is used for storing state files for welder.
  def buildAppStagingBucketName(diskName: DiskName): GcsBucketName = GcsBucketName(
    s"leostaging-${diskName.value}"
  )
}

final case class CreateClusterParams(clusterId: KubernetesClusterLeoId,
                                     googleProject: GoogleProject,
                                     nodepoolsToCreate: List[NodepoolLeoId],
                                     enableIntraNodeVisibility: Boolean,
                                     autopilot: Boolean
)

final case class CreateClusterResult(op: KubernetesOperationId,
                                     network: KubernetesNetwork,
                                     subnetwork: KubernetesSubNetwork
)

final case class PollClusterParams(clusterId: KubernetesClusterLeoId,
                                   googleProject: GoogleProject,
                                   createResult: CreateClusterResult
)

final case class CreateNodepoolParams(nodepoolId: NodepoolLeoId, googleProject: GoogleProject)

final case class CreateAppParams(appId: AppId,
                                 googleProject: GoogleProject,
                                 appName: AppName,
                                 appMachineType: Option[AppMachineType],
                                 mountWorkspaceBucketName: Option[String]
)

final case class DeleteClusterParams(clusterId: KubernetesClusterLeoId, googleProject: GoogleProject)

final case class DeleteNodepoolParams(nodepoolId: NodepoolLeoId, googleProject: GoogleProject)

final case class DeleteAppParams(appId: AppId,
                                 googleProject: GoogleProject,
                                 appName: AppName,
                                 errorAfterDelete: Boolean
)

final case class StopAppParams(appId: AppId, appName: AppName, googleProject: GoogleProject)
object StopAppParams {
  def fromStartAppParams(params: StartAppParams): StopAppParams =
    StopAppParams(params.appId, params.appName, params.googleProject)
}

final case class StartAppParams(appId: AppId, appName: AppName, googleProject: GoogleProject)

final case class UpdateAppParams(appId: AppId,
                                 appName: AppName,
                                 appChartVersion: ChartVersion,
                                 googleProject: Option[GoogleProject],
                                 mountWorkspaceBucketName: Boolean
)
