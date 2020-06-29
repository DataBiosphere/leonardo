package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.http.api.{ListRuntimeResponse2, UpdateRuntimeRequest}
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

trait RuntimeService[F[_]] {
  def createRuntime(userInfo: UserInfo,
                    googleProject: GoogleProject,
                    runtimeName: RuntimeName,
                    req: CreateRuntime2Request)(implicit as: ApplicativeAsk[F, AppContext]): F[Unit]

  def getRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[GetRuntimeResponse]

  def listRuntimes(userInfo: UserInfo, googleProject: Option[GoogleProject], params: Map[String, String])(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[Vector[ListRuntimeResponse2]]

  def deleteRuntime(deleteRuntimeRequest: DeleteRuntimeRequest)(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[Unit]

  def stopRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[Unit]

  def startRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[Unit]

  def updateRuntime(userInfo: UserInfo,
                    googleProject: GoogleProject,
                    runtimeName: RuntimeName,
                    req: UpdateRuntimeRequest)(implicit as: ApplicativeAsk[F, AppContext]): F[Unit]
}

final case class DeleteRuntimeRequest(userInfo: UserInfo,
                                      googleProject: GoogleProject,
                                      runtimeName: RuntimeName,
                                      deleteDisk: Boolean)
