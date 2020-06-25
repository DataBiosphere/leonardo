package org.broadinstitute.dsde.workbench.leonardo
package service

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.http.CreateRuntime2Request
import org.broadinstitute.dsde.workbench.leonardo.http.api.{ListRuntimeResponse2, UpdateRuntimeRequest}
import org.broadinstitute.dsde.workbench.leonardo.http.service.{
  DeleteRuntimeRequest,
  GetRuntimeResponse,
  RuntimeService
}
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

class BaseMockRuntimeServiceInterp extends RuntimeService[IO] {
  override def createRuntime(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    req: CreateRuntime2Request
  )(implicit as: ApplicativeAsk[IO, AppContext]): IO[Unit] =
    IO.unit

  override def getRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit as: ApplicativeAsk[IO, AppContext]
  ): IO[GetRuntimeResponse] =
    IO.pure(
      GetRuntimeResponse.fromRuntime(CommonTestData.testCluster, CommonTestData.defaultDataprocRuntimeConfig, None)
    )

  override def listRuntimes(userInfo: UserInfo, googleProject: Option[GoogleProject], params: Map[String, String])(
    implicit as: ApplicativeAsk[IO, AppContext]
  ): IO[Vector[ListRuntimeResponse2]] =
    IO.pure(
      Vector(
        ListRuntimeResponse2(
          CommonTestData.testCluster.id,
          CommonTestData.testCluster.samResource,
          CommonTestData.testCluster.runtimeName,
          CommonTestData.testCluster.googleProject,
          CommonTestData.testCluster.auditInfo,
          CommonTestData.defaultGceRuntimeConfig,
          CommonTestData.testCluster.proxyUrl,
          CommonTestData.testCluster.status,
          CommonTestData.testCluster.labels,
          CommonTestData.testCluster.patchInProgress
        )
      )
    )

  override def deleteRuntime(deleteRuntimeRequest: DeleteRuntimeRequest)(
    implicit as: ApplicativeAsk[IO, AppContext]
  ): IO[Unit] =
    IO.unit

  override def stopRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit as: ApplicativeAsk[IO, AppContext]
  ): IO[Unit] =
    IO.unit

  override def startRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit as: ApplicativeAsk[IO, AppContext]
  ): IO[Unit] =
    IO.unit

  override def updateRuntime(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    req: UpdateRuntimeRequest
  )(implicit as: ApplicativeAsk[IO, AppContext]): IO[Unit] = IO.unit
}

object MockRuntimeServiceInterp extends BaseMockRuntimeServiceInterp
