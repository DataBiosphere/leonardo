package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.Parallel
import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.RuntimeSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.dao.sam.{SamService, SamUtils}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model.{
  RuntimeCannotBeStartedException,
  RuntimeCannotBeStoppedException
}
import org.broadinstitute.dsde.workbench.leonardo.model.SamResource.RuntimeSamResource
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{StartRuntimeMessage, StopRuntimeMessage}
import org.broadinstitute.dsde.workbench.model.UserInfo

import scala.concurrent.ExecutionContext

class RuntimeV2ServiceInterp[F[_]: Parallel](
  publisherQueue: Queue[F, LeoPubsubMessage],
  samService: SamService[F]
)(implicit F: Async[F], dbReference: DbReference[F], ec: ExecutionContext)
    extends RuntimeV2Service[F] {

  override def listRuntimes(
    userInfo: UserInfo,
    workspaceId: Option[WorkspaceId],
    cloudProvider: Option[CloudProvider],
    params: Map[String, String]
  )(implicit as: Ask[F, AppContext]): F[Vector[ListRuntimeResponse2]] =
    for {
      ctx <- as.ask

      // Parameters: parse search filters from request
      (labelMap, _, _) <- F.fromEither(processListParameters(params))
      excludeStatuses = List(RuntimeStatus.Deleted)
      creatorEmail <- F.fromEither(processCreatorOnlyParameter(userInfo.userEmail, params, ctx.traceId))

      samResources <- samService.listResources(userInfo.accessToken.token, RuntimeSamResource.resourceType)
      runtimes <- RuntimeServiceDbQueries
        .listRuntimes(
          runtimeIds = samResources.map(RuntimeSamResourceId).toSet,
          cloudProvider = cloudProvider,
          creatorEmail = creatorEmail,
          excludeStatuses = excludeStatuses,
          labelMap = labelMap,
          workspaceId = workspaceId
        )
        .map(_.toList)
        .transaction

    } yield runtimes.toVector

  def startRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] = for {
    ctx <- as.ask
    runtime <- getClusterRecordWithRequiredAction(userInfo, workspaceId, runtimeName, RuntimeAction.StopStartRuntime)
    _ <-
      if (runtime.status.isStartable) F.unit
      else
        F.raiseError[Unit](RuntimeCannotBeStartedException(runtime.cloudContext, runtime.runtimeName, runtime.status))
    _ <- clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.PreStarting, ctx.now).transaction
    _ <- publisherQueue.offer(StartRuntimeMessage(runtime.id, Some(ctx.traceId)))
  } yield ()

  def stopRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] = for {
    ctx <- as.ask

    runtime <- getClusterRecordWithRequiredAction(userInfo, workspaceId, runtimeName, RuntimeAction.StopStartRuntime)
    _ <-
      if (runtime.status.isStoppable) F.unit
      else
        F.raiseError[Unit](RuntimeCannotBeStoppedException(runtime.cloudContext, runtime.runtimeName, runtime.status))
    _ <- clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.PreStopping, ctx.now).transaction
    _ <- publisherQueue.offer(StopRuntimeMessage(runtime.id, Some(ctx.traceId)))
  } yield ()

  private def getClusterRecordWithRequiredAction(
    userInfo: UserInfo,
    workspaceId: WorkspaceId,
    runtimeName: RuntimeName,
    action: RuntimeAction
  )(implicit as: Ask[F, AppContext]): F[ClusterRecord] =
    for {
      runtime <- RuntimeServiceDbQueries.getActiveRuntimeRecord(workspaceId, runtimeName).transaction
      _ <- SamUtils.checkRuntimeAction(samService,
                                       userInfo,
                                       workspaceId,
                                       runtimeName,
                                       RuntimeSamResourceId(runtime.internalId),
                                       action
      )
    } yield runtime
}
