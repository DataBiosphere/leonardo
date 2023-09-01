package org.broadinstitute.dsde.workbench
package leonardo
package app

import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.azure.AzureApplicationInsightsService
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsde.workbench.leonardo.app.AppInstall.Database
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.util.{AKSInterpreterConfig, AppUpdateException, CreateAKSAppParams}
import org.broadinstitute.dsp.Values
import org.http4s.Uri
import org.http4s.headers.Authorization

class WdsAppInstall[F[_]](samDao: SamDAO[F],
                          wdsDao: WdsDAO[F],
                          azureApplicationInsightsService: AzureApplicationInsightsService[F]
)(implicit
  F: Async[F]
) extends AppInstall[F] {
  override def databases: List[AppInstall.Database] =
    List(
      Database("wds",
               // The WDS database should only be accessed by WDS-app
               allowAccessForAllWorkspaceUsers = false
      )
    )

  override def helmValues(params: CreateAKSAppParams,
                          config: AKSInterpreterConfig,
                          app: App,
                          relayPath: Uri,
                          ksaName: ServiceAccountName,
                          databaseNames: List[String]
  )(implicit ev: Ask[F, AppContext]): F[Values] =
    for {
      ctx <- ev.ask

      // Resolve Application Insights in Azure
      applicationInsightsComponent <- azureApplicationInsightsService.getApplicationInsights(
        params.landingZoneResources.applicationInsightsName,
        params.cloudContext
      )

      // Get the pet userToken
      tokenOpt <- samDao.getCachedArbitraryPetAccessToken(app.auditInfo.creator)
      userToken <- F.fromOption(
        tokenOpt,
        AppUpdateException(s"Pet not found for user ${app.auditInfo.creator}", Some(ctx.traceId))
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
          raw"general.leoAppInstanceName=${app.appName.value}",
          raw"general.workspaceManager.workspaceId=${params.workspaceId.value}",

          // identity configs
          raw"identity.enabled=false",
          raw"workloadIdentity.enabled=true",
          raw"workloadIdentity.serviceAccountName=${ksaName.value}",

          // Sam configs
          raw"sam.url=${config.samConfig.server}",

          // Leo configs
          raw"leonardo.url=${config.leoUrlBase}",

          // workspace manager
          raw"workspacemanager.url=${config.wsmConfig.uri.renderString}",

          // general configs
          raw"fullnameOverride=wds-${app.release.asString}",
          raw"instrumentationEnabled=${config.wdsAppConfig.instrumentationEnabled}",

          // import configs
          raw"import.dataRepoUrl=${config.tdr.url}",

          // provenance (app-cloning) configs
          raw"provenance.userAccessToken=${userToken}",
          raw"provenance.sourceWorkspaceId=${app.sourceWorkspaceId.map(_.value).getOrElse("")}"
        )

      postgresConfig = (databaseNames.headOption, params.landingZoneResources.postgresServer) match {
        case (Some(db), Some(PostgresServer(dbServerName, pgBouncerEnabled))) =>
          List(
            raw"postgres.podLocalDatabaseEnabled=false",
            raw"postgres.host=$dbServerName.postgres.database.azure.com",
            raw"postgres.pgbouncer.enabled=$pgBouncerEnabled",
            raw"postgres.dbname=$db",
            // convention is that the database user is the same as the service account name
            raw"postgres.user=${ksaName.value}"
          )
        case _ => List.empty
      }

    } yield Values((valuesList ++ postgresConfig).mkString(","))

  override def checkStatus(baseUri: Uri, authHeader: Authorization)(implicit ev: Ask[F, AppContext]): F[Boolean] =
    wdsDao.getStatus(baseUri, authHeader)
}
