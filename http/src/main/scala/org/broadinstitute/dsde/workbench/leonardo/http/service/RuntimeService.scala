package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.Parallel
import cats.effect.Async
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.{ComputePollOperation, GoogleComputeService, GoogleStorageService}
import org.broadinstitute.dsde.workbench.leonardo.config.PersistentDiskConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.DockerDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.http.api.ListRuntimeResponse2
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.ExecutionContext

trait RuntimeService[F[_]] {
  def createRuntime(userInfo: UserInfo,
                    googleProject: GoogleProject,
                    runtimeName: RuntimeName,
                    req: CreateRuntime2Request)(implicit as: Ask[F, AppContext]): F[Unit]

  def getRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit as: Ask[F, AppContext]
  ): F[GetRuntimeResponse]

  def listRuntimes(userInfo: UserInfo, googleProject: Option[GoogleProject], params: Map[String, String])(
    implicit as: Ask[F, AppContext]
  ): F[Vector[ListRuntimeResponse2]]

  def deleteRuntime(deleteRuntimeRequest: DeleteRuntimeRequest)(
    implicit as: Ask[F, AppContext]
  ): F[Unit]

  def stopRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit as: Ask[F, AppContext]
  ): F[Unit]

  def startRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit as: Ask[F, AppContext]
  ): F[Unit]

  def updateRuntime(userInfo: UserInfo,
                    googleProject: GoogleProject,
                    runtimeName: RuntimeName,
                    req: UpdateRuntimeRequest)(implicit as: Ask[F, AppContext]): F[Unit]
}

object RuntimeService {
  def apply[F[_]: Parallel](config: RuntimeServiceConfig,
                            diskConfig: PersistentDiskConfig,
                            authProvider: LeoAuthProvider[F],
                            serviceAccountProvider: ServiceAccountProvider[F],
                            dockerDAO: DockerDAO[F],
                            googleStorageService: GoogleStorageService[F],
                            googleComputeService: GoogleComputeService[F],
                            computePollOperation: ComputePollOperation[F],
                            publisherQueue: fs2.concurrent.Queue[F, LeoPubsubMessage])(
    implicit F: Async[F],
    log: StructuredLogger[F],
    dbReference: DbReference[F],
    ec: ExecutionContext,
    metrics: OpenTelemetryMetrics[F]
  ): RuntimeService[F] =
    new RuntimeServiceInterp(
      config,
      diskConfig,
      authProvider,
      serviceAccountProvider,
      dockerDAO,
      googleStorageService,
      googleComputeService,
      computePollOperation,
      publisherQueue
    )
}

final case class DeleteRuntimeRequest(userInfo: UserInfo,
                                      googleProject: GoogleProject,
                                      runtimeName: RuntimeName,
                                      deleteDisk: Boolean)
