package org.broadinstitute.dsde.workbench.leonardo.auth

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all._
import cats.mtl.Ask
import com.typesafe.config.Config
import io.circe.{Decoder, Encoder}
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{serviceAccountEmail, userEmail}
import org.broadinstitute.dsde.workbench.leonardo.{ProjectAction, WorkspaceId}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

class WhitelistAuthProvider(config: Config, saProvider: ServiceAccountProvider[IO]) extends LeoAuthProvider[IO] {

  val whitelist = config.as[Set[String]]("whitelist").map(_.toLowerCase)

  protected def checkWhitelist(userInfo: UserInfo): IO[Boolean] =
    IO.pure(whitelist contains userInfo.userEmail.value.toLowerCase)

  def hasPermission[R, A](samResource: R, action: A, userInfo: UserInfo)(
    implicit sr: SamResourceAction[R, A],
    ev: Ask[IO, TraceId]
  ): IO[Boolean] = checkWhitelist(userInfo)

  def hasPermissionWithProjectFallback[R, A](
    samResource: R,
    action: A,
    projectAction: ProjectAction,
    userInfo: UserInfo,
    googleProject: GoogleProject
  )(implicit sr: SamResourceAction[R, A], ev: Ask[IO, TraceId]): IO[Boolean] = checkWhitelist(userInfo)

  def getActions[R, A](samResource: R, userInfo: UserInfo)(
    implicit sr: SamResourceAction[R, A],
    ev: Ask[IO, TraceId]
  ): IO[List[sr.ActionCategory]] =
    checkWhitelist(userInfo).map {
      case true  => sr.allActions
      case false => List.empty
    }

  def getActionsWithProjectFallback[R, A](samResource: R, googleProject: GoogleProject, userInfo: UserInfo)(
    implicit sr: SamResourceAction[R, A],
    ev: Ask[IO, TraceId]
  ): IO[(List[sr.ActionCategory], List[ProjectAction])] =
    checkWhitelist(userInfo).map {
      case true  => (sr.allActions, ProjectAction.allActions.toList)
      case false => (List.empty, List.empty)
    }

  def filterUserVisible[R](resources: NonEmptyList[R], userInfo: UserInfo)(
    implicit sr: SamResource[R],
    decoder: Decoder[R],
    ev: Ask[IO, TraceId]
  ): IO[List[R]] =
    resources.toList.traverseFilter { a =>
      checkWhitelist(userInfo).map {
        case true  => Some(a)
        case false => None
      }
    }

  def filterUserVisibleWithProjectFallback[R](
    resources: NonEmptyList[(GoogleProject, R)],
    userInfo: UserInfo
  )(
    implicit sr: SamResource[R],
    decoder: Decoder[R],
    ev: Ask[IO, TraceId]
  ): IO[List[(GoogleProject, R)]] =
    resources.toList.traverseFilter { a =>
      checkWhitelist(userInfo).map {
        case true  => Some(a)
        case false => None
      }
    }

  // Creates a resource in Sam
  def notifyResourceCreated[R](samResource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit sr: SamResource[R],
    encoder: Encoder[R],
    ev: Ask[IO, TraceId]
  ): IO[Unit] = IO.unit

  // Deletes a resource in Sam
  def notifyResourceDeleted[R](
    samResource: R,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject
  )(implicit sr: SamResource[R], ev: Ask[IO, TraceId]): IO[Unit] = IO.unit

  override def serviceAccountProvider: ServiceAccountProvider[IO] = saProvider

  override def filterUserVisibleWithWorkspaceFallback[R](
    resources: NonEmptyList[(WorkspaceId, R)],
    userInfo: UserInfo
  )(implicit sr: SamResource[R], decoder: Decoder[R], ev: Ask[IO, TraceId]): IO[List[(WorkspaceId, R)]] =
    resources.toList.traverseFilter { a =>
      checkWhitelist(userInfo).map {
        case true  => Some(a)
        case false => None
      }
    }

  override def isUserWorkspaceOwner[R](
    workspaceId: WorkspaceId,
    workspaceResource: R,
    userInfo: UserInfo
  )(implicit sr: SamResource[R], decoder: Decoder[R], ev: Ask[IO, TraceId]): IO[Boolean] =
    checkWhitelist(userInfo)

  override def lookupOriginatingUserEmail[R](petOrUserInfo: UserInfo)(
    implicit ev: Ask[IO, TraceId]
  ): IO[WorkbenchEmail] = petOrUserInfo.userEmail.value match {
    case serviceAccountEmail.value => IO(userEmail)
    case _                         => IO(petOrUserInfo.userEmail)
  }
}
