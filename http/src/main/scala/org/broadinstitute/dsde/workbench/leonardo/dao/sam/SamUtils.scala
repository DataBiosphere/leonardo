package org.broadinstitute.dsde.workbench.leonardo.dao.sam

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.Async
import cats.implicits.{catsSyntaxApplicativeError, toFlatMapOps}
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.model.{
  ForbiddenError,
  LeoException,
  RuntimeNotFoundByWorkspaceIdException,
  RuntimeNotFoundException
}
import org.broadinstitute.dsde.workbench.leonardo.{
  AppContext,
  CloudContext,
  RuntimeAction,
  RuntimeName,
  SamResourceId,
  WorkspaceId
}
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}

trait SamUtils[F[_]] {
  val samService: SamService[F]

  def checkRuntimeAction(userInfo: UserInfo,
                         cloudContext: CloudContext,
                         runtimeName: RuntimeName,
                         samResourceId: SamResourceId,
                         action: RuntimeAction,
                         userEmail: Option[WorkbenchEmail] = None
  )(implicit F: Async[F], as: Ask[F, AppContext]): F[Unit] =
    checkRuntimeActionInternal(
      userInfo.accessToken,
      userEmail.getOrElse(userInfo.userEmail),
      samResourceId,
      action,
      RuntimeNotFoundException(cloudContext, runtimeName, "Not found in database")
    )

  def checkRuntimeAction(userInfo: UserInfo,
                         workspaceId: WorkspaceId,
                         runtimeName: RuntimeName,
                         samResourceId: SamResourceId,
                         action: RuntimeAction
  )(implicit F: Async[F], as: Ask[F, AppContext]): F[Unit] =
    checkRuntimeActionInternal(
      userInfo.accessToken,
      userInfo.userEmail,
      samResourceId,
      action,
      RuntimeNotFoundByWorkspaceIdException(workspaceId, runtimeName, "Not found in database")
    )

  private def checkRuntimeActionInternal(userToken: OAuth2BearerToken,
                                         userEmail: WorkbenchEmail,
                                         samResourceId: SamResourceId,
                                         action: RuntimeAction,
                                         notFoundException: LeoException
  )(implicit F: Async[F], as: Ask[F, AppContext]): F[Unit] =
    samService
      .checkAuthorized(userToken.token, samResourceId, action)
      .handleErrorWith {
        // If we've already checked read access and the user doesn't have it, pretend the runtime doesn't exist to avoid leaking its existence
        case e: SamException if e.statusCode == StatusCodes.Forbidden && action == RuntimeAction.GetRuntimeStatus =>
          F.raiseError(notFoundException)
        // Check if the user can read the runtime to determine which error to raise
        case e: SamException if e.statusCode == StatusCodes.Forbidden =>
          samService
            .checkAuthorized(userToken.token, samResourceId, RuntimeAction.GetRuntimeStatus)
            .attempt
            .flatMap {
              // The user can read the runtime, but they don't have the required action. Raise the original Forbidden action from Sam
              case Right(_) => F.raiseError(ForbiddenError(userEmail))
              // The user can't read the runtime, pretend it doesn't exist to avoid leaking its existence
              case Left(_) => F.raiseError(notFoundException)
            }
      }
}
