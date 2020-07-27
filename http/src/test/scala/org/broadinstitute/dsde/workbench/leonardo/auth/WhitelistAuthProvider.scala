package org.broadinstitute.dsde.workbench.leonardo.auth

import cats.effect.IO
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.{LeoAuthAction, ProjectAction, SamResource}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

class WhitelistAuthProvider(config: Config, saProvider: ServiceAccountProvider[IO]) extends LeoAuthProvider[IO] {

  val whitelist = config.as[Set[String]]("whitelist").map(_.toLowerCase)

  protected def checkWhitelist(userInfo: UserInfo): IO[Boolean] =
    IO.pure(whitelist contains userInfo.userEmail.value.toLowerCase)

  def hasPermission[R <: SamResource, A <: LeoAuthAction](samResource: R, action: A, userInfo: UserInfo)(
    implicit act: ActionCheckable[R, A],
    ev: ApplicativeAsk[IO, TraceId]
  ): IO[Boolean] = checkWhitelist(userInfo)

  def hasPermissionWithProjectFallback[R <: SamResource, A <: LeoAuthAction](
    samResource: R,
    action: A,
    projectAction: ProjectAction,
    userInfo: UserInfo,
    googleProject: GoogleProject
  )(implicit act: ActionCheckable[R, A], ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] = checkWhitelist(userInfo)

  def getActions[R <: SamResource, A <: LeoAuthAction](samResource: R, userInfo: UserInfo)(
    implicit act: ActionCheckable[R, A],
    ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[act.ActionCategory]] =
    checkWhitelist(userInfo).map {
      case true  => act.allActions
      case false => List.empty
    }

  def getActionsWithProjectFallback[R <: SamResource, A <: LeoAuthAction](samResource: R,
                                                                          googleProject: GoogleProject,
                                                                          userInfo: UserInfo)(
    implicit act: ActionCheckable[R, A],
    ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[LeoAuthAction]] =
    checkWhitelist(userInfo).map {
      case true  => act.allActions ++ ProjectAction.allActions
      case false => List.empty
    }

  def filterUserVisible[R <: SamResource](resources: List[R], userInfo: UserInfo)(
    implicit pol: PolicyCheckable[R],
    ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[R]] =
    resources.traverseFilter { a =>
      checkWhitelist(userInfo).map {
        case true  => Some(a)
        case false => None
      }
    }

  def filterUserVisibleWithProjectFallback[R <: SamResource](
    resources: List[(GoogleProject, R)],
    userInfo: UserInfo
  )(
    implicit pol: PolicyCheckable[R],
    ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[(GoogleProject, R)]] =
    resources.traverseFilter { a =>
      checkWhitelist(userInfo).map {
        case true  => Some(a)
        case false => None
      }
    }

  // Creates a resource in Sam
  def notifyResourceCreated[R <: SamResource](samResource: R,
                                              creatorEmail: WorkbenchEmail,
                                              googleProject: GoogleProject)(
    implicit pol: PolicyCheckable[R],
    ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] = IO.unit

  // Deletes a resource in Sam
  def notifyResourceDeleted[R <: SamResource](
    samResource: R,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject
  )(implicit pol: PolicyCheckable[R], ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit

  override def serviceAccountProvider: ServiceAccountProvider[IO] = saProvider
}
