package org.broadinstitute.dsde.workbench.leonardo.dao.sam

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.Async
import cats.implicits.{catsSyntaxApplicativeError, toFlatMapOps}
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.http.service.{DiskNotFoundByIdException, DiskNotFoundException}
import org.broadinstitute.dsde.workbench.leonardo.model.{
  ForbiddenError,
  LeoException,
  RuntimeNotFoundByWorkspaceIdException,
  RuntimeNotFoundException
}
import org.broadinstitute.dsde.workbench.leonardo.{
  AppContext,
  CloudContext,
  DiskId,
  PersistentDiskAction,
  RuntimeAction,
  RuntimeName,
  SamResourceAction,
  SamResourceId,
  WorkspaceId
}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

trait SamUtils[F[_]] {
  val samService: SamService[F]

  def checkRuntimeAction(userInfo: UserInfo,
                         cloudContext: CloudContext,
                         runtimeName: RuntimeName,
                         samResourceId: SamResourceId,
                         action: RuntimeAction,
                         userEmail: Option[WorkbenchEmail] = None
  )(implicit F: Async[F], as: Ask[F, AppContext]): F[Unit] =
    checkActionInternal(
      userInfo.accessToken,
      userEmail.getOrElse(userInfo.userEmail),
      samResourceId,
      action,
      RuntimeAction.GetRuntimeStatus,
      RuntimeNotFoundException(cloudContext, runtimeName, "Not found in database")
    )

  def checkRuntimeAction(userInfo: UserInfo,
                         workspaceId: WorkspaceId,
                         runtimeName: RuntimeName,
                         samResourceId: SamResourceId,
                         action: RuntimeAction
  )(implicit F: Async[F], as: Ask[F, AppContext]): F[Unit] =
    checkActionInternal(
      userInfo.accessToken,
      userInfo.userEmail,
      samResourceId,
      action,
      RuntimeAction.GetRuntimeStatus,
      RuntimeNotFoundByWorkspaceIdException(workspaceId, runtimeName, "Not found in database")
    )

  def checkDiskAction(userInfo: UserInfo,
                      cloudContext: CloudContext,
                      diskName: DiskName,
                      samResourceId: SamResourceId,
                      action: SamResourceAction,
                      traceId: TraceId
  )(implicit F: Async[F], as: Ask[F, AppContext]): F[Unit] =
    checkActionInternal(
      userInfo.accessToken,
      userInfo.userEmail,
      samResourceId,
      action,
      PersistentDiskAction.ReadPersistentDisk,
      DiskNotFoundException(cloudContext, diskName, traceId)
    )

  def checkDiskAction(userInfo: UserInfo,
                      diskId: DiskId,
                      samResourceId: SamResourceId,
                      action: SamResourceAction,
                      traceId: TraceId
  )(implicit F: Async[F], as: Ask[F, AppContext]): F[Unit] =
    checkActionInternal(
      userInfo.accessToken,
      userInfo.userEmail,
      samResourceId,
      action,
      PersistentDiskAction.ReadPersistentDisk,
      DiskNotFoundByIdException(diskId, traceId)
    )

  private def checkActionInternal(userToken: OAuth2BearerToken,
                                  userEmail: WorkbenchEmail,
                                  samResourceId: SamResourceId,
                                  actionToCheck: SamResourceAction,
                                  resourceReadAction: SamResourceAction,
                                  notFoundException: LeoException
  )(implicit F: Async[F], as: Ask[F, AppContext]): F[Unit] =
    samService
      .checkAuthorized(userToken.token, samResourceId, actionToCheck)
      .handleErrorWith {
        // If we've already checked read access and the user doesn't have it, pretend the resource doesn't exist to avoid leaking its existence
        case e: SamException if e.statusCode == StatusCodes.Forbidden && actionToCheck == resourceReadAction =>
          F.raiseError(notFoundException)
        // Check if the user can read the resource to determine which error to raise
        case e: SamException if e.statusCode == StatusCodes.Forbidden =>
          samService
            .checkAuthorized(userToken.token, samResourceId, resourceReadAction)
            .attempt
            .flatMap {
              // The user can read the resource, but they don't have the required action. Raise the original Forbidden action from Sam
              case Right(_) => F.raiseError(ForbiddenError(userEmail))
              // The user can't read the resource, pretend it doesn't exist to avoid leaking its existence
              case Left(_) => F.raiseError(notFoundException)
            }
      }
}
