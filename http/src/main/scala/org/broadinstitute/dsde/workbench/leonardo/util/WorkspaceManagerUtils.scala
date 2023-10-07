package org.broadinstitute.dsde.workbench.leonardo.util

import bio.terra.workspace.model.{AccessScope, ResourceType, StewardshipType}
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.dao.{SamDAO, WsmApiClientProvider}
import org.broadinstitute.dsde.workbench.leonardo.{
  AppContext,
  StorageContainer,
  StorageContainerName,
  WorkspaceId,
  WsmControlledResourceId
}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import scala.jdk.CollectionConverters._
import org.broadinstitute.dsde.workbench.leonardo.http._

class WorkspaceManagerUtils[F[_]](wsmClientProvider: WsmApiClientProvider[F], samDAO: SamDAO[F])(implicit F: Async[F]) {

  private[util] def getWorkspaceSharedStorageContainer(
    workspaceId: WorkspaceId,
    userEmail: WorkbenchEmail
  )(implicit ev: Ask[F, AppContext]): F[Option[StorageContainer]] =
    for {
      ctx <- ev.ask
      tokenOpt <- samDAO.getCachedArbitraryPetAccessToken(userEmail)
      token <- F.fromOption(tokenOpt, AppCreationException(s"Pet not found for user ${userEmail}", Some(ctx.traceId)))
      wsmResourceApi <- wsmClientProvider.getResourceApi(token)
      wsmResp <- F.blocking(
        wsmResourceApi
          .enumerateResources(workspaceId.value,
                              0,
                              100,
                              ResourceType.AZURE_STORAGE_CONTAINER,
                              StewardshipType.CONTROLLED
          )
          .getResources
          .asScala
          .toList
      )
      storageContainerOpt = wsmResp
        .find { r =>
          r.getMetadata.getControlledResourceMetadata.getAccessScope == AccessScope.SHARED_ACCESS && r.getMetadata.getResourceType == ResourceType.AZURE_STORAGE_CONTAINER
        }
        .map { r =>
          StorageContainer(
            StorageContainerName(r.getResourceAttributes.getAzureStorageContainer.getStorageContainerName),
            WsmControlledResourceId(r.getMetadata.getResourceId)
          )
        }
    } yield storageContainerOpt

}
