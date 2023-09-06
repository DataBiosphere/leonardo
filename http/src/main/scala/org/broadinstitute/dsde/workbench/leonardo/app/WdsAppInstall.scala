package org.broadinstitute.dsde.workbench
package leonardo
package app

import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.azure.AzureApplicationInsightsService
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
                          azureApplicationInsightsService: AzureApplicationInsightsService[F]
)(implicit
  F: Async[F]
) extends AppInstall[F] {
  override def databases: List[Database] =
    List(
      Database("wds",
               // The WDS database should only be accessed by WDS-app
               allowAccessForAllWorkspaceUsers = false
      )
    )

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
      dbName <- F.fromOption(params.databaseNames.headOption,
                             AppCreationException("Database names required for WDS app", Some(ctx.traceId))
      )

      // Postgres server required for WDS App
      postgresServer <- F.fromOption(
        params.landingZoneResources.postgresServer,
        AppCreationException("Postgres server required for WDS app", Some(ctx.traceId))
      )

      // Get the pet userToken
      tokenOpt <- samDao.getCachedArbitraryPetAccessToken(params.app.auditInfo.creator)
      userToken <- F.fromOption(
        tokenOpt,
        AppCreationException(s"Pet not found for user ${params.app.auditInfo.creator}", Some(ctx.traceId))
      )

      valuesList =
        List(
          // azure resources configs
          raw"config.resourceGroup=${params.cloudContext.managedResourceGroupName.value}",
          raw"config.applicationInsightsConnectionString=${applicationInsightsComponent.connectionString()}",

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

          // Sam configs
          raw"sam.url=${params.config.samConfig.server}",

          // Leo configs
          raw"leonardo.url=${params.config.leoUrlBase}",

          // workspace manager
          raw"workspacemanager.url=${params.config.wsmConfig.uri.renderString}",

          // general configs
          raw"fullnameOverride=wds-${params.app.release.asString}",
          raw"instrumentationEnabled=${config.instrumentationEnabled}",

          // import configs
          raw"import.dataRepoUrl=${tdrConfig.url}",

          // provenance (app-cloning) configs
          raw"provenance.userAccessToken=${userToken}",
          raw"provenance.sourceWorkspaceId=${params.app.sourceWorkspaceId.map(_.value).getOrElse("")}",

          // database configs
          raw"postgres.podLocalDatabaseEnabled=false",
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
