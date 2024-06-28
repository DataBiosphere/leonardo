package org.broadinstitute.dsde.workbench.leonardo.app

import cats.effect.Async
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, CloudContext, RuntimeName}
import org.broadinstitute.dsde.workbench.leonardo.config.JupyterAppConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.JupyterDAO
import org.broadinstitute.dsde.workbench.leonardo.util.AppCreationException
import org.broadinstitute.dsp.Values
import org.http4s.Uri
import org.http4s.headers.Authorization

/**
 * Jupyter app.
 */
class JupyterAppInstall[F[_]](config: JupyterAppConfig, JupyterDao: JupyterDAO[F])(implicit F: Async[F])
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
        AppCreationException("Storage container required for Hail Batch app", Some(ctx.traceId))
      )
      values =
        List(
          raw"persistence.storageAccount=${params.landingZoneResources.storageAccountName.value}",
          raw"persistence.blobContainer=${storageContainer.name.value}",
          raw"persistence.workspaceManager.url=${params.config.wsmConfig.uri.renderString}",
          raw"persistence.workspaceManager.workspaceId=${params.workspaceId.value}",
          raw"persistence.workspaceManager.containerResourceId=${storageContainer.resourceId.value.toString}",
          raw"persistence.workspaceManager.storageContainerUrl=https://${params.landingZoneResources.storageAccountName.value}.blob.core.windows.net/${storageContainer.name.value}",
          raw"persistence.leoAppName=${params.app.appName.value}",

          // identity configs
          raw"workloadIdentity.serviceAccountName=${params.ksaName.value}",

          // relay configs
          raw"relay.domain=${params.relayPath.authority.getOrElse("none")}",
          raw"relay.subpath=/${params.relayPath.path.segments.last.toString}"
        )
    } yield Values(values.mkString(","))

  override def checkStatus(baseUri: Uri, authHeader: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] =
    F.pure(true) // TODO (LM)

  def isProxyAvailable(cloudContext: CloudContext, runtimeName: RuntimeName)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] =
    JupyterDao.isProxyAvailable(cloudContext, runtimeName)
}
