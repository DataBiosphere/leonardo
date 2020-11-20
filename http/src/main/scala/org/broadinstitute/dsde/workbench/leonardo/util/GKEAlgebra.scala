package org.broadinstitute.dsde.workbench.leonardo.util

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.GKEModels.{
  KubernetesClusterId,
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

  /** Creates a GKE nodepool but doesn't wait for its completion. */
  def createNodepool(params: CreateNodepoolParams)(implicit ev: Ask[F, AppContext]): F[Option[CreateNodepoolResult]]

  /** Polls a creating nodepool for its completion. */
  def pollNodepool(params: PollNodepoolParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  /** Creates an app and polls it for completion. */
  def createAndPollApp(params: CreateAppParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  /** Deletes a cluster and polls for completion */
  def deleteAndPollCluster(params: DeleteClusterParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  /** Deletes a nodepool and polls for completion */
  def deleteAndPollNodepool(params: DeleteNodepoolParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  /** Deletes an app and polls for completion */
  def deleteAndPollApp(params: DeleteAppParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  def stopAndPollApp(params: StopAppParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  def startAndPollApp(params: StartAppParams)(implicit ev: Ask[F, AppContext]): F[Unit]
}

final case class CreateClusterParams(clusterId: KubernetesClusterLeoId,
                                     googleProject: GoogleProject,
                                     nodepoolsToCreate: List[NodepoolLeoId],
                                     isNodepoolPrecreate: Boolean)

final case class CreateClusterResult(op: KubernetesOperationId,
                                     network: KubernetesNetwork,
                                     subnetwork: KubernetesSubNetwork)

final case class PollClusterParams(clusterId: KubernetesClusterLeoId,
                                   googleProject: GoogleProject,
                                   isNodepoolPrecreate: Boolean,
                                   createResult: CreateClusterResult)

final case class CreateNodepoolParams(nodepoolId: NodepoolLeoId, googleProject: GoogleProject)

final case class CreateNodepoolResult(op: KubernetesOperationId, clusterId: KubernetesClusterId)

final case class PollNodepoolParams(nodepoolId: NodepoolLeoId, createResult: CreateNodepoolResult)

final case class CreateAppParams(appId: AppId, googleProject: GoogleProject, appName: AppName)

final case class DeleteClusterParams(clusterId: KubernetesClusterLeoId, googleProject: GoogleProject)

final case class DeleteNodepoolParams(nodepoolId: NodepoolLeoId, googleProject: GoogleProject)

final case class DeleteAppParams(appId: AppId,
                                 googleProject: GoogleProject,
                                 appName: AppName,
                                 errorAfterDelete: Boolean)

final case class StopAppParams(appId: AppId, appName: AppName, googleProject: GoogleProject)

final case class StartAppParams(appId: AppId, appName: AppName, googleProject: GoogleProject)
