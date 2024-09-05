package org.broadinstitute.dsde.workbench.leonardo.auth

import akka.http.scaladsl.model.StatusCodes
import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all._
import cats.mtl.Ask
import com.typesafe.config.Config
import io.circe.{Decoder, Encoder}
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.dao.AuthProviderException
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{serviceAccountEmail, userEmail}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.{AppSamResourceId, WorkspaceResourceSamResourceId}
import org.broadinstitute.dsde.workbench.leonardo.{CloudContext, ProjectAction, SamResourceId, WorkspaceId}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

class AllowlistAuthProvider(config: Config) extends LeoAuthProvider[IO] {

  val allowlist = config.as[Set[String]]("allowlist").map(_.toLowerCase)

  protected def checkAllowlist(userInfo: UserInfo): IO[Boolean] =
    IO.pure(allowlist contains userInfo.userEmail.value.toLowerCase)

  def hasPermission[R, A](samResource: R, action: A, userInfo: UserInfo)(implicit
    sr: SamResourceAction[R, A],
    ev: Ask[IO, TraceId]
  ): IO[Boolean] = checkAllowlist(userInfo)

  def hasPermissionWithProjectFallback[R, A](
    samResource: R,
    action: A,
    projectAction: ProjectAction,
    userInfo: UserInfo,
    googleProject: GoogleProject
  )(implicit sr: SamResourceAction[R, A], ev: Ask[IO, TraceId]): IO[Boolean] = checkAllowlist(userInfo)

  def getActions[R, A](samResource: R, userInfo: UserInfo)(implicit
    sr: SamResourceAction[R, A],
    ev: Ask[IO, TraceId]
  ): IO[List[A]] =
    checkAllowlist(userInfo).map {
      case true  => sr.allActions
      case false => List.empty
    }

  def getActionsWithProjectFallback[R, A](samResource: R, googleProject: GoogleProject, userInfo: UserInfo)(implicit
    sr: SamResourceAction[R, A],
    ev: Ask[IO, TraceId]
  ): IO[(List[A], List[ProjectAction])] =
    checkAllowlist(userInfo).map {
      case true  => (sr.allActions, ProjectAction.allActions.toList)
      case false => (List.empty, List.empty)
    }

  def listResourceIds[R <: SamResourceId](
    hasOwnerRole: Boolean,
    userInfo: UserInfo
  )(implicit
    resourceDefinition: SamResource[R],
    appDefinition: SamResource[AppSamResourceId],
    resourceIdDecoder: Decoder[R],
    ev: Ask[IO, TraceId]
  ): IO[Set[R]] =
    checkAllowlist(userInfo).map(_ => Set.empty)

  def filterUserVisible[R](resources: NonEmptyList[R], userInfo: UserInfo)(implicit
    sr: SamResource[R],
    decoder: Decoder[R],
    ev: Ask[IO, TraceId]
  ): IO[List[R]] =
    resources.toList.traverseFilter { a =>
      checkAllowlist(userInfo).map {
        case true  => Some(a)
        case false => None
      }
    }

  def filterResourceProjectVisible[R](
    resources: NonEmptyList[(GoogleProject, R)],
    userInfo: UserInfo
  )(implicit
    sr: SamResource[R],
    decoder: Decoder[R],
    ev: Ask[IO, TraceId]
  ): IO[List[(GoogleProject, R)]] =
    resources.toList.traverseFilter { a =>
      checkAllowlist(userInfo).map {
        case true  => Some(a)
        case false => None
      }
    }

  override def isUserProjectReader(
    cloudContext: CloudContext,
    userInfo: UserInfo
  )(implicit ev: Ask[IO, TraceId]): IO[Boolean] =
    checkAllowlist(userInfo)

  // Creates a resource in Sam
  def notifyResourceCreated[R](samResource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit
    sr: SamResource[R],
    encoder: Encoder[R],
    ev: Ask[IO, TraceId]
  ): IO[Unit] = IO.unit

  // Deletes a resource in Sam
  def notifyResourceDeleted[R](
    samResource: R,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject
  )(implicit sr: SamResource[R], ev: Ask[IO, TraceId]): IO[Unit] = IO.unit

  override def isUserWorkspaceOwner(
    workspaceResource: WorkspaceResourceSamResourceId,
    userInfo: UserInfo
  )(implicit ev: Ask[IO, TraceId]): IO[Boolean] =
    checkAllowlist(userInfo)

  override def isUserWorkspaceReader(
    workspaceResource: WorkspaceResourceSamResourceId,
    userInfo: UserInfo
  )(implicit ev: Ask[IO, TraceId]): IO[Boolean] =
    checkAllowlist(userInfo)

  override def lookupOriginatingUserEmail[R](petOrUserInfo: UserInfo)(implicit
    ev: Ask[IO, TraceId]
  ): IO[WorkbenchEmail] = petOrUserInfo.userEmail.value match {
    case serviceAccountEmail.value => IO(userEmail)
    case _                         => IO(petOrUserInfo.userEmail)
  }

  override def checkUserEnabled(petOrUserInfo: UserInfo)(implicit ev: Ask[IO, TraceId]): IO[Unit] = for {
    traceId: TraceId <- ev.ask
    _ <- checkAllowlist(petOrUserInfo).map {
      case true => IO.unit
      case false =>
        IO.raiseError(
          AuthProviderException(
            traceId,
            s"[AllowlistAuthProvider.checkUserEnabled] User ${petOrUserInfo.userEmail.value} is disabled",
            StatusCodes.Unauthorized
          )
        )
    }
  } yield ()

  override def isCustomAppAllowed(userEmail: WorkbenchEmail)(implicit ev: Ask[IO, TraceId]): IO[Boolean] = IO.pure(true)

  override def notifyResourceCreatedV2[R](samResource: R,
                                          creatorEmail: WorkbenchEmail,
                                          cloudContext: CloudContext,
                                          workspaceId: WorkspaceId,
                                          userInfo: UserInfo
  )(implicit sr: SamResource[R], encoder: Encoder[R], ev: Ask[IO, TraceId]): IO[Unit] = IO.unit

  override def notifyResourceDeletedV2[R](samResource: R, userInfo: UserInfo)(implicit
    sr: SamResource[R],
    ev: Ask[IO, TraceId]
  ): IO[Unit] = IO.unit

  override def filterWorkspaceOwner(resources: NonEmptyList[WorkspaceResourceSamResourceId], userInfo: UserInfo)(
    implicit ev: Ask[IO, TraceId]
  ): IO[Set[WorkspaceResourceSamResourceId]] = IO.pure(resources.toList.toSet)

  override def filterWorkspaceReader(resources: NonEmptyList[WorkspaceResourceSamResourceId], userInfo: UserInfo)(
    implicit ev: Ask[IO, TraceId]
  ): IO[Set[WorkspaceResourceSamResourceId]] = for {
    filteredResources <- resources.toList.traverseFilter { resource =>
      isUserWorkspaceReader(resource, userInfo).map {
        case true  => Some(resource)
        case false => None
      }
    }
  } yield filteredResources.toSet

  def filterUserVisibleWithProjectFallback[R](
    resources: NonEmptyList[(GoogleProject, R)],
    userInfo: UserInfo
  )(implicit
    sr: SamResource[R],
    decoder: Decoder[R],
    ev: Ask[IO, TraceId]
  ): IO[List[(GoogleProject, R)]] =
    resources.toList.traverseFilter { a =>
      checkAllowlist(userInfo).map {
        case true  => Some(a)
        case false => None
      }
    }

  override def isAdminUser(userInfo: UserInfo)(implicit ev: Ask[IO, TraceId]): IO[Boolean] = ???

  override def isSasAppAllowed(userEmail: WorkbenchEmail)(implicit ev: Ask[IO, TraceId]): IO[Boolean] = IO.pure(true)

  override def getLeoAuthToken: IO[String] = ???
}
