package org.broadinstitute.dsde.workbench.leonardo
package service

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.http.service.{
  CreateAppRequest,
  DeleteAppParams,
  GetAppResponse,
  ListAppResponse
}
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

class MockKubernetesServiceInterp extends KubernetesService[IO] {
  override def createApp(userInfo: UserInfo, googleProject: GoogleProject, appName: AppName, req: CreateAppRequest)(
    implicit as: ApplicativeAsk[IO, AppContext]
  ): IO[Unit] =
    IO.unit

  override def getApp(userInfo: UserInfo, googleProject: GoogleProject, appName: AppName)(
    implicit as: ApplicativeAsk[IO, AppContext]
  ): IO[GetAppResponse] =
    IO.pure(getAppResponse)

  override def listApp(userInfo: UserInfo,
                       googleProject: Option[GoogleProject],
                       params: Map[String, String]): IO[Vector[ListAppResponse]] =
    IO.pure(listAppResponse)

  override def deleteApp(params: DeleteAppParams)(
    implicit as: ApplicativeAsk[IO, AppContext]
  ): IO[Unit] =
    IO.unit
}

object MockKubernetesServiceInterp extends MockKubernetesServiceInterp
