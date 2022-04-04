package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.Parallel
import cats.effect.Async
import cats.mtl.Ask
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.{RuntimeName, WorkspaceId, AppContext}
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.dao.{WsmDao, SamDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, RuntimeServiceDbQueries}
import org.broadinstitute.dsde.workbench.leonardo.http.api.ListRuntimeResponse2
import org.broadinstitute.dsde.workbench.model.UserInfo

import scala.concurrent.ExecutionContext

class RuntimeV2ServiceInterp[F[_]: Parallel](config: RuntimeServiceConfig,
                             authProvider: LeoAuthProvider[F],
                             wsmDao: WsmDao[F],
                             samDAO: SamDAO[F])(implicit F: Async[F],
dbReference: DbReference[F],
ec: ExecutionContext
) extends RuntimeV2Service[F] {

  override def listRuntimes(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: Option[WorkspaceId], params: Map[String, String])(implicit as: Ask[F, AppContext]): F[Vector[ListRuntimeResponse2]] = {

    for {
      leoAuth <- samDAO.getLeoAuthToken
      context <- as.ask
      paramMap <- F.fromEither(processListParameters(params))
      workspaceDesc <- workspaceId.traverse(id => wsmDao.getWorkspace(id, leoAuth))

      azureRuntimes <- workspaceDesc.traverse(desc =>
        RuntimeServiceDbQueries.listRuntimes(paramMap._1, paramMap._2, desc.azureContext.map(CloudContext.Azure)).transaction
      )
      googleRuntimes <- workspaceDesc.traverse(desc =>
        RuntimeServiceDbQueries.listRuntimes(paramMap._1, paramMap._2, desc.gcpContext.map(CloudContext.Gcp)).transaction
      )

      allRuntimes = azureRuntimes.getOrElse(List.empty) ++ googleRuntimes.getOrElse(List.empty)

      //TODO: sam permissions filter


    } yield (allRuntimes)
  }
}
