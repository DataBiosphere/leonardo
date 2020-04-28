package org.broadinstitute.dsde.workbench.leonardo
package model

import ca.mrvisser.sealerate

import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

sealed trait LeoAuthAction extends Product with Serializable

sealed trait ProjectAction extends LeoAuthAction
object ProjectAction {
  case object CreateClusters extends ProjectAction
  case object CreatePersistentDisk extends ProjectAction
  val allActions = sealerate.values[ProjectAction]
}

sealed trait NotebookClusterAction extends LeoAuthAction
object NotebookClusterAction {
  case object GetClusterStatus extends NotebookClusterAction
  case object ConnectToCluster extends NotebookClusterAction
  case object SyncDataToCluster extends NotebookClusterAction
  case object DeleteCluster extends NotebookClusterAction
  case object ModifyCluster extends NotebookClusterAction
  case object StopStartCluster extends NotebookClusterAction
  val allActions = sealerate.values[NotebookClusterAction]
}

sealed trait PersistentDiskAction extends LeoAuthAction
object PersistentDiskAction {
  case object ReadPersistentDisk extends PersistentDiskAction
  case object AttachPersistentDisk extends PersistentDiskAction
  case object ModifyPersistentDisk extends PersistentDiskAction
  case object DeletePersistentDisk extends PersistentDiskAction
  val allActions = sealerate.values[PersistentDiskAction]
}

abstract class LeoAuthProvider[F[_]] {
  def serviceAccountProvider: ServiceAccountProvider[F]

  /**
   * @param userInfo The user in question
   * @param action The project-level action (above) the user is requesting
   * @param googleProject The Google project to check in
   * @return If the given user has permissions in this project to perform the specified action.
   */
  def hasProjectPermission(userInfo: UserInfo, action: ProjectAction, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Boolean]

  /**
   * Leo calls this method to verify if the user has permission to perform the given action on a specific notebook cluster.
   * It may call this method passing in a cluster that doesn't exist. Return Future.successful(false) if so.
   *
   * @param userInfo      The user in question
   * @param action        The cluster-level action (above) the user is requesting
   * @param googleProject The Google project the cluster was created in
   * @param runtimeName   The user-provided name of the Dataproc cluster
   * @return If the userEmail has permission on this individual notebook cluster to perform this action
   */
  def hasNotebookClusterPermission(internalId: RuntimeInternalId,
                                   userInfo: UserInfo,
                                   action: NotebookClusterAction,
                                   googleProject: GoogleProject,
                                   runtimeName: RuntimeName)(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean]

  /**
    * Leo calls this method to verify if the user has permission to perform the given action on a specific persistent disk.
    * Return Future.successful(false) if the specified persistent disk does not exist.
    *
    * @param userInfo       The user in question
    * @param action         The persistent-disk action (above) the user is requesting
    * @param googleProject  The Google project the persistent disk was created in
    * @return If the userEmail has permission on this individual notebook cluster to perform this action
    */
  def hasPersistentDiskPermission(internalId: PersistentDiskInternalId,
                                  userInfo: UserInfo,
                                  action: PersistentDiskAction,
                                  googleProject: GoogleProject)(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean]

  /**
   * Leo calls this method when it receives a "list clusters" API call, passing in all non-deleted clusters from the database.
   * It should return a list of clusters that the user can see according to their authz.
   *
   * @param userInfo The user in question
   * @param clusters All non-deleted clusters from the database
   * @return         Filtered list of clusters that the user is allowed to see
   */
  def filterUserVisibleClusters(userInfo: UserInfo, clusters: List[(GoogleProject, RuntimeInternalId)])(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[List[(GoogleProject, RuntimeInternalId)]]

  /**
    * Leo calls this method when it receives a "list persistent disks" API call, passing in all non-deleted disks from the database.
    * It should return a list of disks that the user can see according to their authz.
    *
    * @param userInfo The user in question
    * @param disks    All non-deleted disks from the database
    * @return         Filtered list of disks that the user is allowed to see
    */
  def filterUserVisiblePersistentDisks(userInfo: UserInfo, disks: List[(GoogleProject, PersistentDiskInternalId)])(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[List[(GoogleProject, PersistentDiskInternalId)]]

  //Notifications that Leo has created/destroyed clusters. Allows the auth provider to register things.

  /**
   * Leo calls this method to notify the auth provider that a new notebook cluster has been created.
   * The returned future should complete once the provider has finished doing any associated work.
   * Returning a failed Future will prevent the cluster from being created, and will call notifyClusterDeleted for the same cluster.
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
                           runtimeName: RuntimeName)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]

  /**
   * Leo calls this method to notify the auth provider that a notebook cluster has been deleted.
   * The returned future should complete once the provider has finished doing any associated work.
   * Leo will wait, so be timely!
   *
   * @param internalId    The internal ID for the cluster (i.e. used for Sam resources)
   * @param userEmail     The email address of the user in question
   * @param creatorEmail  The email address of the creator of the cluster
   * @param googleProject The Google project the cluster was created in
   * @param runtimeName   The user-provided name of the Dataproc cluster
   * @return A Future that will complete when the auth provider has finished doing its business.
   */
  def notifyClusterDeleted(internalId: RuntimeInternalId,
                           userEmail: WorkbenchEmail,
                           creatorEmail: WorkbenchEmail,
                           googleProject: GoogleProject,
                           runtimeName: RuntimeName)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]

  /**
    * Leo calls this method to notify the auth provider that a new persistent disk has been created.
    * The returned future should complete once the provider has finished doing any associated work.
    * Returning a failed Future will prevent the disk from being created, and will call notifyPersistentDiskDeleted for the same disk.
    * Leo will wait, so be timely!
    *
    * @param internalId     The internal ID for the persistent disk (i.e. used for Sam resources)
    * @param creatorEmail   The email address of the user in question
    * @param googleProject  The Google project the disk was created in
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  def notifyPersistentDiskCreated(internalId: PersistentDiskInternalId,
                                  creatorEmail: WorkbenchEmail,
                                  googleProject: GoogleProject)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]

  /**
    * Leo calls this method to notify the auth provider that a persistent disk has been deleted.
    * The returned future should complete once the provider has finished doing any associated work.
    * Leo will wait, so be timely!
    *
    * @param internalId    The internal ID for the persistent disk (i.e. used for Sam resources)
    * @param userEmail     The email address of the user in question
    * @param creatorEmail  The email address of the creator of the disk
    * @param googleProject The Google project the disk was created in
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  def notifyPersistentDiskDeleted(internalId: PersistentDiskInternalId,
                                  userEmail: WorkbenchEmail,
                                  creatorEmail: WorkbenchEmail,
                                  googleProject: GoogleProject)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
}
