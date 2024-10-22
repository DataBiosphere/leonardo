package org.broadinstitute.dsde.workbench
package leonardo
package app

import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.azure.AzureApplicationInsightsService
import org.broadinstitute.dsde.workbench.leonardo.app.Database.ControlledDatabase
import org.broadinstitute.dsde.workbench.leonardo.auth.SamAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.config.WdsAppConfig
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.util.AppCreationException
import org.broadinstitute.dsp.Values
import org.http4s.Uri
import org.http4s.headers.Authorization

/**
 * WDS app.
 * Helm chart: https://github.com/broadinstitute/terra-helmfile/tree/master/charts/wds
 */
class WdsAppInstall[F[_]](config: WdsAppConfig,
                          tdrConfig: TdrConfig,
                          samDao: SamDAO[F],
                          wdsDao: WdsDAO[F],
                          azureApplicationInsightsService: AzureApplicationInsightsService[F],
                          authProvider: SamAuthProvider[F]
)(implicit
  F: Async[F]
) extends AppInstall[F] {
  override def databases: List[Database] =
    List(ControlledDatabase("wds"))

  override def buildHelmOverrideValues(
    params: BuildHelmOverrideValuesParams
  )(implicit ev: Ask[F, AppContext]): F[Values] =
    for {
      ctx <- ev.ask

      // Resolve Application Insights in Azure
      applicationInsightsComponent <- azureApplicationInsightsService.getApplicationInsights(
        params.landingZoneResources.applicationInsightsName,
        params.cloudContext
      )

      // Database required for WDS App
      dbName <- F.fromOption(params.databaseNames.headOption.map(_.azureDatabaseName),
                             AppCreationException("Database names required for WDS app", Some(ctx.traceId))
      )

      // Postgres server required for WDS App
      postgresServer <- F.fromOption(
        params.landingZoneResources.postgresServer,
        AppCreationException("Postgres server required for WDS app", Some(ctx.traceId))
      )

      // Get the pet userToken

      // Get Vpa enabled tag
      vpaEnabled <- F.pure(params.landingZoneResources.aksCluster.tags.getOrElse("aks-cost-vpa-enabled", false))

      valuesList =
        List(
          // pass enviiroment information to wds so it can properly pick its config
          raw"wds.environment=${config.environment}",
          raw"wds.environmentBase=${config.environmentBase}",

          // azure resources configs
          raw"config.resourceGroup=${params.cloudContext.managedResourceGroupName.value}",
          raw"config.applicationInsightsConnectionString=${applicationInsightsComponent.connectionString()}",
          raw"config.aks.vpaEnabled=${vpaEnabled}",

          // Azure subscription configs currently unused
          raw"config.subscriptionId=${params.cloudContext.subscriptionId.value}",
          raw"config.region=${params.landingZoneResources.region}",

          // persistence configs
          raw"general.leoAppInstanceName=${params.app.appName.value}",
          raw"general.workspaceManager.workspaceId=${params.workspaceId.value}",

          // identity configs
          raw"identity.enabled=false",
          raw"workloadIdentity.enabled=true",
          raw"workloadIdentity.serviceAccountName=${params.ksaName.value}",

          // general configs
          raw"fullnameOverride=wds-${params.app.release.asString}",
          raw"instrumentationEnabled=${config.instrumentationEnabled}",

          // provenance (app-cloning) configs
          raw"provenance.userAccessToken=",
          raw"provenance.sourceWorkspaceId=${params.app.sourceWorkspaceId.map(_.value).getOrElse("")}",

          // database configs
          raw"postgres.host=${postgresServer.name}.postgres.database.azure.com",
          raw"postgres.pgbouncer.enabled=${postgresServer.pgBouncerEnabled}",
          raw"postgres.dbname=$dbName",
          // convention is that the database user is the same as the service account name
          raw"postgres.user=${params.ksaName.value}"
        )
    } yield Values(valuesList.mkString(","))

  override def checkStatus(baseUri: Uri, authHeader: Authorization)(implicit ev: Ask[F, AppContext]): F[Boolean] =
    wdsDao.getStatus(baseUri, authHeader).handleError(_ => false)
}
