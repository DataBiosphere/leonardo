package org.broadinstitute.dsde.workbench.leonardo.auth

import cats.effect.IO
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.typesafe.config.Config
import io.circe.{Decoder, Encoder}
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.ProjectAction
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

class WhitelistAuthProvider(config: Config, saProvider: ServiceAccountProvider[IO]) extends LeoAuthProvider[IO] {

  val whitelist = config.as[Set[String]]("whitelist").map(_.toLowerCase)

  protected def checkWhitelist(userInfo: UserInfo): IO[Boolean] =
    IO.pure(whitelist contains userInfo.userEmail.value.toLowerCase)

  def hasPermission[R, A](samResource: R, action: A, userInfo: UserInfo)(
    implicit sr: SamResourceAction[R, A],
    ev: ApplicativeAsk[IO, TraceId]
  ): IO[Boolean] = checkWhitelist(userInfo)

  def hasPermissionWithProjectFallback[R, A](
    samResource: R,
    action: A,
    projectAction: ProjectAction,
    userInfo: UserInfo,
    googleProject: GoogleProject
  )(implicit sr: SamResourceAction[R, A], ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] = checkWhitelist(userInfo)

  def getActions[R, A](samResource: R, userInfo: UserInfo)(
    implicit sr: SamResourceAction[R, A],
    ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[sr.ActionCategory]] =
    checkWhitelist(userInfo).map {
      case true  => sr.allActions
      case false => List.empty
    }

  def getActionsWithProjectFallback[R, A](samResource: R, googleProject: GoogleProject, userInfo: UserInfo)(
    implicit sr: SamResourceAction[R, A],
    ev: ApplicativeAsk[IO, TraceId]
  ): IO[(List[sr.ActionCategory], List[ProjectAction])] =
    checkWhitelist(userInfo).map {
      case true  => (sr.allActions, ProjectAction.allActions.toList)
      case false => (List.empty, List.empty)
    }

  def filterUserVisible[R](resources: List[R], userInfo: UserInfo)(
    implicit sr: SamResource[R],
    decoder: Decoder[R],
    ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[R]] =
    resources.traverseFilter { a =>
      checkWhitelist(userInfo).map {
        case true  => Some(a)
        case false => None
      }
    }

  def filterUserVisibleWithProjectFallback[R](
    resources: List[(GoogleProject, R)],
    userInfo: UserInfo
  )(
    implicit sr: SamResource[R],
    decoder: Decoder[R],
    ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[(GoogleProject, R)]] =
    resources.traverseFilter { a =>
      checkWhitelist(userInfo).map {
        case true  => Some(a)
        case false => None
      }
    }

  // Creates a resource in Sam
  def notifyResourceCreated[R](samResource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit sr: SamResource[R],
    encoder: Encoder[R],
    ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] = IO.unit

  // Deletes a resource in Sam
  def notifyResourceDeleted[R](
    samResource: R,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject
  )(implicit sr: SamResource[R], ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit

  override def serviceAccountProvider: ServiceAccountProvider[IO] = saProvider
}
