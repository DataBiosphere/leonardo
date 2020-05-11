package org.broadinstitute.dsde.workbench.leonardo
package auth

import cats.effect.IO
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterAction
import org.broadinstitute.dsde.workbench.leonardo.model.PersistentDiskAction
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectAction
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

class WhitelistAuthProvider(config: Config, saProvider: ServiceAccountProvider[IO]) extends LeoAuthProvider[IO] {

  val whitelist = config.as[Set[String]]("whitelist").map(_.toLowerCase)

  protected def checkWhitelist(userInfo: UserInfo): IO[Boolean] =
    IO.pure(whitelist contains userInfo.userEmail.value.toLowerCase)

  override def hasProjectPermission(userInfo: UserInfo, action: ProjectAction, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Boolean] =
    checkWhitelist(userInfo)

  override def hasNotebookClusterPermission(
    internalId: RuntimeInternalId,
    userInfo: UserInfo,
    action: NotebookClusterAction,
    googleProject: GoogleProject,
    runtimeName: RuntimeName
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] =
    checkWhitelist(userInfo)

  override def hasPersistentDiskPermission(
    internalId: DiskSamResourceId,
    userInfo: UserInfo,
    action: PersistentDiskAction,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] =
    checkWhitelist(userInfo)

  override def filterUserVisibleClusters(userInfo: UserInfo, clusters: List[(GoogleProject, RuntimeInternalId)])(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[(GoogleProject, RuntimeInternalId)]] =
    clusters.traverseFilter { a =>
      checkWhitelist(userInfo).map {
        case true  => Some(a)
        case false => None
      }
    }

  override def filterUserVisiblePersistentDisks(userInfo: UserInfo, disks: List[(GoogleProject, DiskSamResourceId)])(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[(GoogleProject, DiskSamResourceId)]] =
    disks.traverseFilter { a =>
      checkWhitelist(userInfo).map {
        case true  => Some(a)
        case false => None
      }
    }

  def notifyClusterCreated(internalId: RuntimeInternalId,
                           creatorEmail: WorkbenchEmail,
                           googleProject: GoogleProject,
                           runtimeName: RuntimeName)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit

  def notifyClusterDeleted(internalId: RuntimeInternalId,
                           userEmail: WorkbenchEmail,
                           creatorEmail: WorkbenchEmail,
                           googleProject: GoogleProject,
                           runtimeName: RuntimeName)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit

  override def notifyPersistentDiskCreated(
    internalId: DiskSamResourceId,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit

  override def notifyPersistentDiskDeleted(
    internalId: DiskSamResourceId,
    userEmail: WorkbenchEmail,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit

  override def serviceAccountProvider: ServiceAccountProvider[IO] = saProvider
}
