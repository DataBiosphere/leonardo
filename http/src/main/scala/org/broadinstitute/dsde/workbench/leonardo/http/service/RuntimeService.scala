package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.Parallel
import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.{GoogleComputeService, GoogleStorageService}
import org.broadinstitute.dsde.workbench.leonardo.config.PersistentDiskConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.DockerDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.ExecutionContext

trait RuntimeService[F[_]] {
  def createRuntime(userInfo: UserInfo,
                    cloudContext: CloudContext,
                    runtimeName: RuntimeName,
                    req: CreateRuntimeRequest
  )(implicit as: Ask[F, AppContext]): F[CreateRuntimeResponse]

  def getRuntime(userInfo: UserInfo, cloudContext: CloudContext, runtimeName: RuntimeName)(implicit
    as: Ask[F, AppContext]
  ): F[GetRuntimeResponse]

  def listRuntimes(userInfo: UserInfo, cloudContext: Option[CloudContext], params: Map[String, String])(implicit
    as: Ask[F, AppContext]
  ): F[Vector[ListRuntimeResponse2]]

  def deleteRuntime(deleteRuntimeRequest: DeleteRuntimeRequest)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def deleteAllRuntimes(userInfo: UserInfo, cloudContext: CloudContext.Gcp, deleteDisk: Boolean)(implicit
    as: Ask[F, AppContext]
  ): F[Option[Vector[DiskId]]]

  def deleteRuntimeRecords(userInfo: UserInfo, cloudContext: CloudContext.Gcp, runtime: ListRuntimeResponse2)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def deleteAllRuntimesRecords(userInfo: UserInfo, cloudContext: CloudContext.Gcp)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def stopRuntime(userInfo: UserInfo, cloudContext: CloudContext, runtimeName: RuntimeName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def startRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def updateRuntime(userInfo: UserInfo,
                    googleProject: GoogleProject,
                    runtimeName: RuntimeName,
                    req: UpdateRuntimeRequest
  )(implicit as: Ask[F, AppContext]): F[Unit]
}

object RuntimeService {
  def apply[F[_]: Parallel](config: RuntimeServiceConfig,
                            diskConfig: PersistentDiskConfig,
                            authProvider: LeoAuthProvider[F],
                            serviceAccountProvider: ServiceAccountProvider[F],
                            dockerDAO: DockerDAO[F],
                            googleStorageService: Option[GoogleStorageService[F]],
                            googleComputeService: Option[GoogleComputeService[F]],
                            publisherQueue: Queue[F, LeoPubsubMessage]
  )(implicit
    F: Async[F],
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
      publisherQueue
    )
}

final case class DeleteRuntimeRequest(userInfo: UserInfo,
                                      googleProject: GoogleProject,
                                      runtimeName: RuntimeName,
                                      deleteDisk: Boolean
)
