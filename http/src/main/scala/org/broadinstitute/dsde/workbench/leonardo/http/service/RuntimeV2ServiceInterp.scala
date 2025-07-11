package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.Parallel
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.RuntimeSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.dao.sam.SamService
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model.SamResource.RuntimeSamResource
import org.broadinstitute.dsde.workbench.model.UserInfo

import scala.concurrent.ExecutionContext

class RuntimeV2ServiceInterp[F[_]: Parallel](
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
}
