package org.broadinstitute.dsde.workbench.leonardo.util

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.GKEModels.{
  KubernetesNetwork,
  KubernetesOperationId,
  KubernetesSubNetwork
}
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, AppId, AppName, KubernetesClusterLeoId, NodepoolLeoId}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

trait GKEAlgebra[F[_]] {

  /** Creates a GKE cluster but doesn't wait for its completion. */
  def createCluster(params: CreateClusterParams)(implicit ev: Ask[F, AppContext]): F[Option[CreateClusterResult]]

  /**
   * Polls a creating GKE cluster for its completion and also does other cluster-wide set-up like
   * install nginx ingress controller.
   */
  def pollCluster(params: PollClusterParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  /** Creates a GKE nodepool and polls it for completion */
  def createAndPollNodepool(params: CreateNodepoolParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  /** Creates an app and polls it for completion */
  def createAndPollApp(params: CreateAppParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  /** Deletes a cluster and polls for completion */
  def deleteAndPollCluster(params: DeleteClusterParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  /** Deletes a nodepool and polls for completion */
  def deleteAndPollNodepool(params: DeleteNodepoolParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  /** Deletes an app and polls for completion */
  def deleteAndPollApp(params: DeleteAppParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  /** Stops an app and polls for completion */
  def stopAndPollApp(params: StopAppParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  /** Starts an app and polls for completion */
  def startAndPollApp(params: StartAppParams)(implicit ev: Ask[F, AppContext]): F[Unit]
}

final case class CreateClusterParams(clusterId: KubernetesClusterLeoId,
                                     googleProject: GoogleProject,
                                     nodepoolsToCreate: List[NodepoolLeoId])

final case class CreateClusterResult(op: KubernetesOperationId,
                                     network: KubernetesNetwork,
                                     subnetwork: KubernetesSubNetwork)

final case class PollClusterParams(clusterId: KubernetesClusterLeoId,
                                   googleProject: GoogleProject,
                                   createResult: CreateClusterResult)

final case class CreateNodepoolParams(nodepoolId: NodepoolLeoId, googleProject: GoogleProject)

final case class CreateAppParams(appId: AppId, googleProject: GoogleProject, appName: AppName)

final case class DeleteClusterParams(clusterId: KubernetesClusterLeoId, googleProject: GoogleProject)

final case class DeleteNodepoolParams(nodepoolId: NodepoolLeoId, googleProject: GoogleProject)

final case class DeleteAppParams(appId: AppId,
                                 googleProject: GoogleProject,
                                 appName: AppName,
                                 errorAfterDelete: Boolean)

final case class StopAppParams(appId: AppId, appName: AppName, googleProject: GoogleProject)
object StopAppParams {
  def fromStartAppParams(params: StartAppParams): StopAppParams =
    StopAppParams(params.appId, params.appName, params.googleProject)
}

final case class StartAppParams(appId: AppId, appName: AppName, googleProject: GoogleProject)
