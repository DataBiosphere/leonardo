package org.broadinstitute.dsde.workbench.leonardo
package auth

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

class MockLeoAuthProvider(authConfig: Config, saProvider: ServiceAccountProvider[IO], notifySucceeds: Boolean = true)
    extends LeoAuthProvider[IO] {
  override def serviceAccountProvider: ServiceAccountProvider[IO] = saProvider
  //behaviour defined in test\...\reference.conf
  val projectPermissions: Map[ProjectActions.ProjectAction, Boolean] =
    (ProjectActions.allActions map (action => action -> authConfig.getBoolean(action.toString))).toMap
  val clusterPermissions: Map[NotebookClusterActions.NotebookClusterAction, Boolean] =
    (NotebookClusterActions.allActions map (action => action -> authConfig.getBoolean(action.toString))).toMap

  val canSeeClustersInAllProjects = authConfig.as[Option[Boolean]]("canSeeClustersInAllProjects").getOrElse(false)
  val canSeeAllClustersIn = authConfig.as[Option[Seq[String]]]("canSeeAllClustersIn").getOrElse(Seq.empty)

  override def hasProjectPermission(
    userInfo: UserInfo,
    action: ProjectActions.ProjectAction,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] =
    IO.pure(projectPermissions(action))

  override def hasNotebookClusterPermission(
    internalId: ClusterInternalId,
    userInfo: UserInfo,
    action: NotebookClusterActions.NotebookClusterAction,
    googleProject: GoogleProject,
    clusterName: ClusterName
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] =
    IO.pure(clusterPermissions(action))

  override def filterUserVisibleClusters(userInfo: UserInfo, clusters: List[(GoogleProject, ClusterInternalId)])(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[(GoogleProject, ClusterInternalId)]] =
    if (canSeeClustersInAllProjects) {
      IO.pure(clusters)
    } else {
      IO.pure(clusters.filter {
        case (googleProject, _) =>
          canSeeAllClustersIn.contains(googleProject.value) || clusterPermissions(
            NotebookClusterActions.GetClusterStatus
          )
      })
    }

  private def notifyInternal: IO[Unit] =
    if (notifySucceeds)
      IO.unit
    else
      IO.raiseError(new RuntimeException("boom"))

  override def notifyClusterCreated(internalId: ClusterInternalId,
                                    creatorEmail: WorkbenchEmail,
                                    googleProject: GoogleProject,
                                    clusterName: ClusterName)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    notifyInternal

  override def notifyClusterDeleted(internalId: ClusterInternalId,
                                    userEmail: WorkbenchEmail,
                                    creatorEmail: WorkbenchEmail,
                                    googleProject: GoogleProject,
                                    clusterName: ClusterName)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    notifyInternal

}
