package org.broadinstitute.dsde.workbench.leonardo.auth

import cats.effect.IO
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.SamResource
import org.broadinstitute.dsde.workbench.leonardo.SamResource.{PersistentDiskSamResource, RuntimeSamResource}
import org.broadinstitute.dsde.workbench.leonardo.model._
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

  override def hasRuntimePermission(
    samResource: RuntimeSamResource,
    userInfo: UserInfo,
    action: RuntimeAction,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] =
    checkWhitelist(userInfo)

  override def hasPersistentDiskPermission(
    samResource: PersistentDiskSamResource,
    userInfo: UserInfo,
    action: PersistentDiskAction,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] =
    checkWhitelist(userInfo)

  override def filterUserVisibleRuntimes(userInfo: UserInfo, clusters: List[(GoogleProject, RuntimeSamResource)])(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[(GoogleProject, RuntimeSamResource)]] =
    clusters.traverseFilter { a =>
      checkWhitelist(userInfo).map {
        case true  => Some(a)
        case false => None
      }
    }

  override def filterUserVisiblePersistentDisks(userInfo: UserInfo,
                                                disks: List[(GoogleProject, PersistentDiskSamResource)])(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[(GoogleProject, PersistentDiskSamResource)]] =
    disks.traverseFilter { a =>
      checkWhitelist(userInfo).map {
        case true  => Some(a)
        case false => None
      }
    }

  def notifyResourceCreated(samResource: SamResource, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] = IO.unit

  def notifyResourceDeleted(samResource: SamResource,
                            userEmail: WorkbenchEmail,
                            creatorEmail: WorkbenchEmail,
                            googleProject: GoogleProject)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit

  override def serviceAccountProvider: ServiceAccountProvider[IO] = saProvider

  override def getRuntimeActionsWithProjectFallback(googleProject: GoogleProject,
                                                    samResource: RuntimeSamResource,
                                                    userInfo: UserInfo)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[LeoAuthAction]] =
    if (checkWhitelist(userInfo) == IO.pure(true)) IO.pure(RuntimeAction.allActions.toList)
    else IO.pure(List.empty)

}
