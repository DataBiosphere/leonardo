package org.broadinstitute.dsde.workbench.leonardo.util

import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, AppId, AppName, KubernetesClusterLeoId, NodepoolLeoId}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

trait GKEAlgebra[F[_]] {
  def createAndPollCluster(params: CreateClusterParams)(implicit ev: ApplicativeAsk[F, AppContext]): F[Unit]

  def createAndPollNodepool(params: CreateNodepoolParams)(implicit ev: ApplicativeAsk[F, AppContext]): F[Unit]

  def createAndPollApp(params: CreateAppParams)(implicit ev: ApplicativeAsk[F, AppContext]): F[Unit]

  def deleteAndPollCluster(params: DeleteClusterParams)(implicit ev: ApplicativeAsk[F, AppContext]): F[Unit]

  def deleteAndPollNodepool(params: DeleteNodepoolParams)(implicit ev: ApplicativeAsk[F, AppContext]): F[Unit]

  def deleteAndPollApp(params: DeleteAppParams)(implicit ev: ApplicativeAsk[F, AppContext]): F[Unit]
}

final case class CreateClusterParams(clusterId: KubernetesClusterLeoId,
                                     googleProject: GoogleProject,
                                     nodepoolsToCreate: List[NodepoolLeoId],
                                     isNodepoolPrecreate: Boolean)

final case class CreateNodepoolParams(nodepoolId: NodepoolLeoId, googleProject: GoogleProject)

final case class CreateAppParams(appId: AppId, googleProject: GoogleProject, appName: AppName)

final case class DeleteClusterParams(clusterId: KubernetesClusterLeoId, googleProject: GoogleProject)

final case class DeleteNodepoolParams(nodepoolId: NodepoolLeoId, googleProject: GoogleProject)

final case class DeleteAppParams(appId: AppId, googleProject: GoogleProject, appName: AppName)
