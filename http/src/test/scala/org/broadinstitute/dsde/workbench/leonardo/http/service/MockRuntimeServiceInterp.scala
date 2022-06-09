package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

class BaseMockRuntimeServiceInterp extends RuntimeService[IO] {
  override def createRuntime(
    userInfo: UserInfo,
    cloudContext: CloudContext,
    runtimeName: RuntimeName,
    req: CreateRuntime2Request
  )(implicit as: Ask[IO, AppContext]): IO[Unit] =
    IO.unit

  override def getRuntime(userInfo: UserInfo, cloudContext: CloudContext, runtimeName: RuntimeName)(implicit
    as: Ask[IO, AppContext]
  ): IO[GetRuntimeResponse] =
    IO.pure(
      GetRuntimeResponse.fromRuntime(CommonTestData.testCluster, CommonTestData.defaultDataprocRuntimeConfig, None)
    )

  override def listRuntimes(userInfo: UserInfo, cloudContext: Option[CloudContext], params: Map[String, String])(
    implicit as: Ask[IO, AppContext]
  ): IO[Vector[ListRuntimeResponse2]] =
    IO.pure(
      Vector(
        ListRuntimeResponse2(
          CommonTestData.testCluster.id,
          None,
          CommonTestData.testCluster.samResource,
          CommonTestData.testCluster.runtimeName,
          CommonTestData.testCluster.cloudContext,
          CommonTestData.testCluster.auditInfo,
          CommonTestData.defaultGceRuntimeConfig,
          CommonTestData.testCluster.proxyUrl,
          CommonTestData.testCluster.status,
          CommonTestData.testCluster.labels,
          CommonTestData.testCluster.patchInProgress
        )
      )
    )

  override def deleteRuntime(deleteRuntimeRequest: DeleteRuntimeRequest)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] =
    IO.unit

  override def stopRuntime(userInfo: UserInfo, cloudContext: CloudContext, runtimeName: RuntimeName)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] =
    IO.unit

  override def startRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] =
    IO.unit

  override def updateRuntime(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    req: UpdateRuntimeRequest
  )(implicit as: Ask[IO, AppContext]): IO[Unit] = IO.unit
}

object MockRuntimeServiceInterp extends BaseMockRuntimeServiceInterp
