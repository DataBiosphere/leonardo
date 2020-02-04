package org.broadinstitute.dsde.workbench.leonardo
package auth

import cats.effect.IO
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions.NotebookClusterAction
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions.ProjectAction
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

class WhitelistAuthProvider(config: Config, saProvider: ServiceAccountProvider[IO]) extends LeoAuthProvider[IO] {

  val whitelist = config.as[Set[String]]("whitelist").map(_.toLowerCase)

  protected def checkWhitelist(userInfo: UserInfo): IO[Boolean] =
    IO.pure(whitelist contains userInfo.userEmail.value.toLowerCase)

  /**
   * @param userInfo The user in question
   * @param action The project-level action (above) the user is requesting
   * @param googleProject The Google project to check in
   * @return If the given user has permissions in this project to perform the specified action.
   */
  override def hasProjectPermission(userInfo: UserInfo, action: ProjectAction, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Boolean] =
    checkWhitelist(userInfo)

  /**
   * @param internalId     The internal ID for the cluster (i.e. used for Sam resources)
   * @param userInfo The user in question
   * @param action   The cluster-level action (above) the user is requesting
   * @param runtimeName The user-provided name of the Dataproc cluster
   * @return If the userEmail has permission on this individual notebook cluster to perform this action
   */
  override def hasNotebookClusterPermission(
    internalId: RuntimeInternalId,
    userInfo: UserInfo,
    action: NotebookClusterAction,
    googleProject: GoogleProject,
    runtimeName: RuntimeName
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] =
    checkWhitelist(userInfo)

  /**
   * Leo calls this method when it receives a "list clusters" API call, passing in all non-deleted clusters from the database.
   * It should return a list of clusters that the user can see according to their authz.
   *
   * @param userInfo The user in question
   * @param clusters All non-deleted clusters from the database
   * @return         Filtered list of clusters that the user is allowed to see
   */
  override def filterUserVisibleClusters(userInfo: UserInfo, clusters: List[(GoogleProject, RuntimeInternalId)])(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[(GoogleProject, RuntimeInternalId)]] =
    clusters.traverseFilter { a =>
      checkWhitelist(userInfo).map {
        case true  => Some(a)
        case false => None
      }
    }

  //Notifications that Leo has created/destroyed clusters. Allows the auth provider to register things.

  /**
   * Leo calls this method to notify the auth provider that a new notebook cluster has been created.
   * The returned future should complete once the provider has finished doing any associated work.
   * Leo will wait, so be timely!
   *
   * @param internalId     The internal ID for the cluster (i.e. used for Sam resources)
   * @param creatorEmail     The email address of the user in question
   * @param googleProject The Google project the cluster was created in
   * @param runtimeName   The user-provided name of the Dataproc cluster
   * @return A Future that will complete when the auth provider has finished doing its business.
   */
  def notifyClusterCreated(internalId: RuntimeInternalId,
                           creatorEmail: WorkbenchEmail,
                           googleProject: GoogleProject,
                           runtimeName: RuntimeName)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit

  /**
   * Leo calls this method to notify the auth provider that a notebook cluster has been destroyed.
   * The returned future should complete once the provider has finished doing any associated work.
   * Leo will wait, so be timely!
   *
   * @param internalId     The internal ID for the cluster (i.e. used for Sam resources)
   * @param userEmail     The email address of the user in question
   * @param creatorEmail     The email address of the creator of the cluster
   * @param googleProject The Google project the cluster was created in
   * @param runtimeName   The user-provided name of the Dataproc cluster
   * @return A Future that will complete when the auth provider has finished doing its business.
   */
  def notifyClusterDeleted(internalId: RuntimeInternalId,
                           userEmail: WorkbenchEmail,
                           creatorEmail: WorkbenchEmail,
                           googleProject: GoogleProject,
                           runtimeName: RuntimeName)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit

  override def serviceAccountProvider: ServiceAccountProvider[IO] = saProvider
}
