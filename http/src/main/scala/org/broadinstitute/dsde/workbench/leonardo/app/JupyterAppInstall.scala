package org.broadinstitute.dsde.workbench.leonardo.app
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.broadinstitute.dsde.workbench.leonardo.config.JupyterAppConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.JupyterDAO
import org.broadinstitute.dsde.workbench.leonardo.util.AppCreationException
import org.broadinstitute.dsp.Values
import org.http4s.Uri
import org.http4s.headers.Authorization

/**
 * Jupyter app.
 */
class JupyterAppInstall[F[_]](config: JupyterAppConfig, jupyterDao: JupyterDAO[F])(implicit F: Async[F])
    extends AppInstall[F] {
  override def databases: List[Database] = List.empty

  override def buildHelmOverrideValues(
    params: BuildHelmOverrideValuesParams
  )(implicit ev: Ask[F, AppContext]): F[Values] =
    for {
      ctx <- ev.ask
      // Storage container is required for Cromwell app
      storageContainer <- F.fromOption(
        params.storageContainer,
        AppCreationException("Storage container required for Jupyter app", Some(ctx.traceId))
      )

      disk <- F.fromOption(
        params.app.appResources.disk,
        AppCreationException("Disk required for Jupyter app", Some(ctx.traceId))
      )

//      diskResourceId <- F.fromOption(
//        params.diskWsmResourceId,
//        AppCreationException("Disk required for Jupyter app", Some(ctx.traceId))
//      )

      values =
        List(
          // workspace configs
          raw"workspace.id=${params.workspaceId.value.toString}",
          raw"workspace.name=${params.workspaceName}",
          raw"workspace.storageContainer.url=https://${params.landingZoneResources.storageAccountName.value}.blob.core.windows.net/${storageContainer.name.value}",
          raw"workspace.storageContainer.resourceId=${storageContainer.resourceId.value.toString}",
          raw"workspace.cloudProvider=Azure",

          // persistent disk configs
          raw"persistence.diskName=${disk.name.value}",
          raw"persistence.diskSize=${disk.size.gb}",
          // raw"persistence.diskResourceId=${diskResourceId.value.toString}",
          raw"persistence.subscriptionId=${params.cloudContext.subscriptionId.value}",
          raw"persistence.resourceGroupName=${params.cloudContext.managedResourceGroupName.value}",

          // misc
          raw"serviceAccount.name=${params.ksaName.value}",
          raw"relay.connectionName=${params.app.appName.value}"
        )
    } yield Values(values.mkString(","))

  override def checkStatus(baseUri: Uri, authHeader: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] =
    jupyterDao.getStatus(baseUri, authHeader)
}
