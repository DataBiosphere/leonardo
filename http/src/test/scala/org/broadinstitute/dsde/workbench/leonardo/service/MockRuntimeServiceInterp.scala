package org.broadinstitute.dsde.workbench.leonardo.service

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.http.api.{
  CreateRuntime2Request,
  RuntimeServiceContext,
  UpdateRuntimeRequest
}
import org.broadinstitute.dsde.workbench.leonardo.http.service.{GetRuntimeResponse, ListRuntimeResponse, RuntimeService}
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, RuntimeName}
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

object MockRuntimeServiceInterp extends RuntimeService[IO] {
  override def createRuntime(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    req: CreateRuntime2Request
  )(implicit as: ApplicativeAsk[IO, RuntimeServiceContext]): IO[Unit] =
    IO.unit

  override def getRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit as: ApplicativeAsk[IO, RuntimeServiceContext]
  ): IO[GetRuntimeResponse] =
    IO.pure(GetRuntimeResponse.fromRuntime(CommonTestData.testCluster, CommonTestData.defaultRuntimeConfig))

  override def listRuntimes(userInfo: UserInfo, googleProject: Option[GoogleProject], params: Map[String, String])(
    implicit as: ApplicativeAsk[IO, RuntimeServiceContext]
  ): IO[Vector[ListRuntimeResponse]] =
    IO.pure(Vector(ListRuntimeResponse.fromRuntime(CommonTestData.testCluster, CommonTestData.defaultRuntimeConfig)))

  override def deleteRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit as: ApplicativeAsk[IO, RuntimeServiceContext]
  ): IO[Unit] =
    IO.unit

  override def stopRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit as: ApplicativeAsk[IO, RuntimeServiceContext]
  ): IO[Unit] =
    IO.unit

  override def startRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit as: ApplicativeAsk[IO, RuntimeServiceContext]
  ): IO[Unit] =
    IO.unit

  override def updateRuntime(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    req: UpdateRuntimeRequest
  )(implicit as: ApplicativeAsk[IO, RuntimeServiceContext]): IO[Unit] = IO.unit
}
