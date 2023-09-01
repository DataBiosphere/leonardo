package org.broadinstitute.dsde.workbench.leonardo.app
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName
import org.broadinstitute.dsde.workbench.leonardo
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.broadinstitute.dsde.workbench.leonardo.dao.HailBatchDAO
import org.broadinstitute.dsde.workbench.leonardo.util.{AKSInterpreterConfig, AppCreationException, CreateAKSAppParams}
import org.broadinstitute.dsp.Values
import org.http4s.Uri
import org.http4s.headers.Authorization

class HailBatchAppInstall[F[_]](hailBatchDao: HailBatchDAO[F])(implicit F: Async[F]) extends AppInstall[F] {
  override def databases: List[AppInstall.Database] = List.empty

  override def helmValues(params: CreateAKSAppParams,
                          config: AKSInterpreterConfig,
                          app: leonardo.App,
                          relayPath: Uri,
                          ksaName: KubernetesSerializableName.ServiceAccountName,
                          databaseNames: List[String]
  )(implicit ev: Ask[F, AppContext]): F[Values] =
    for {
      ctx <- ev.ask
      // Storage container is required for Cromwell app
      storageContainer <- F.fromOption(
        params.storageContainer,
        AppCreationException("Storage container required for Hail Batch app", Some(ctx.traceId))
      )
      values =
        List(
          raw"persistence.storageAccount=${params.landingZoneResources.storageAccountName.value}",
          raw"persistence.blobContainer=${storageContainer.name.value}",
          raw"persistence.workspaceManager.url=${config.wsmConfig.uri.renderString}",
          raw"persistence.workspaceManager.workspaceId=${params.workspaceId.value}",
          raw"persistence.workspaceManager.containerResourceId=${storageContainer.resourceId.value.toString}",
          raw"persistence.workspaceManager.storageContainerUrl=https://${params.landingZoneResources.storageAccountName.value}.blob.core.windows.net/${storageContainer.name.value}",
          raw"persistence.leoAppName=${app.appName.value}",

          // identity configs
          // TODO: can Hail Batch chart migrate to Workfload Identity?
//          raw"identity.name=${petManagedIdentity.map(_.name).getOrElse("none")}",
//          raw"identity.resourceId=${petManagedIdentity.map(_.id).getOrElse("none")}",
//          raw"identity.clientId=${petManagedIdentity.map(_.clientId).getOrElse("none")}",

          // relay configs
          raw"relay.domain=${relayPath.authority.getOrElse("none")}",
          raw"relay.subpath=/${relayPath.path.segments.last.toString}"
        )
    } yield values.mkString(",")

  override def checkStatus(baseUri: Uri, authHeader: Authorization)(implicit ev: Ask[F, AppContext]): F[Boolean] =
    hailBatchDao.getStatus(baseUri, authHeader)
}
