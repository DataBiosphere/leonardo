package org.broadinstitute.dsde.workbench.leonardo
package http

import cats.data.NonEmptyList
import cats.effect.IO
import cats.mtl.Ask
import com.google.cloud.compute.v1.Operation
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.leonardo.algebra.{SamResource, SamResourceAction}
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

object MockRuntimeAlgebra extends RuntimeAlgebra[IO] {
  override def createRuntime(params: CreateRuntimeParams)(
    implicit ev: Ask[IO, AppContext]
  ): IO[CreateGoogleRuntimeResponse] = ???

  override def getRuntimeStatus(params: GetRuntimeStatusParams)(
    implicit ev: Ask[IO, AppContext]
  ): IO[RuntimeStatus] = ???

  override def deleteRuntime(params: DeleteRuntimeParams)(
    implicit ev: Ask[IO, AppContext]
  ): IO[Option[Operation]] = IO.pure(None)

  override def finalizeDelete(params: FinalizeDeleteParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = ???

  override def stopRuntime(params: StopRuntimeParams)(
    implicit ev: Ask[IO, AppContext]
  ): IO[Option[Operation]] = ???

  override def startRuntime(params: StartRuntimeParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = ???

  override def updateMachineType(params: UpdateMachineTypeParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] =
    ???

  override def updateDiskSize(params: UpdateDiskSizeParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = ???

  override def resizeCluster(params: ResizeClusterParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = ???
}

object MockAuthProvider extends LeoAuthProvider[IO] {
  override def serviceAccountProvider: ServiceAccountProvider[IO] = ???

  override def hasPermission[R, A](samResource: R, action: A, userInfo: UserInfo)(
    implicit sr: SamResourceAction[R, A],
    ev: Ask[IO, TraceId]
  ): IO[Boolean] = ???

  override def hasPermissionWithProjectFallback[R, A](
    samResource: R,
    action: A,
    projectAction: ProjectAction,
    userInfo: UserInfo,
    googleProject: GoogleProject
  )(implicit sr: SamResourceAction[R, A], ev: Ask[IO, TraceId]): IO[Boolean] = ???

  override def getActions[R, A](samResource: R, userInfo: UserInfo)(
    implicit sr: SamResourceAction[R, A],
    ev: Ask[IO, TraceId]
  ): IO[List[sr.ActionCategory]] = ???

  override def getActionsWithProjectFallback[R, A](samResource: R, googleProject: GoogleProject, userInfo: UserInfo)(
    implicit sr: SamResourceAction[R, A],
    ev: Ask[IO, TraceId]
  ): IO[(List[sr.ActionCategory], List[ProjectAction])] = ???

  override def filterUserVisible[R](
    resources: NonEmptyList[R],
    userInfo: UserInfo
  )(implicit sr: SamResource[R], decoder: Decoder[R], ev: Ask[IO, TraceId]): IO[List[R]] = ???

  override def filterUserVisibleWithProjectFallback[R](
    resources: NonEmptyList[(GoogleProject, R)],
    userInfo: UserInfo
  )(implicit sr: SamResource[R], decoder: Decoder[R], ev: Ask[IO, TraceId]): IO[List[(GoogleProject, R)]] =
    ???

  override def notifyResourceCreated[R](samResource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit sr: SamResource[R],
    encoder: Encoder[R],
    ev: Ask[IO, TraceId]
  ): IO[Unit] = IO.unit

  override def notifyResourceDeleted[R](samResource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit sr: SamResource[R],
    ev: Ask[IO, TraceId]
  ): IO[Unit] = IO.unit
}
