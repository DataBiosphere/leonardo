package org.broadinstitute.dsde.workbench.leonardo.app

import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.azure.{AzureApplicationInsightsService, AzureBatchService}
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, WsmControlledDatabaseResource}
import org.broadinstitute.dsde.workbench.leonardo.app.AppInstall.getAzureDatabaseName
import org.broadinstitute.dsde.workbench.leonardo.app.Database.{ControlledDatabase, ReferenceDatabase}
import org.broadinstitute.dsde.workbench.leonardo.config.CromwellRunnerAppConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.{CromwellDAO, SamDAO}
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.util.AppCreationException
import org.broadinstitute.dsp.Values
import org.http4s.Uri
import org.http4s.headers.Authorization

/**
 * Cromwell runner app type.
 * Helm chart: https://github.com/broadinstitute/terra-helmfile/tree/master/charts/cromwell-runner-app
 */
class CromwellRunnerAppInstall[F[_]](config: CromwellRunnerAppConfig,
                                     drsConfig: DrsConfig,
                                     samDao: SamDAO[F],
                                     cromwellDao: CromwellDAO[F],
                                     azureBatchService: AzureBatchService[F],
                                     azureApplicationInsightsService: AzureApplicationInsightsService[F]
)(implicit
  F: Async[F]
) extends AppInstall[F] {
  override def databases: List[Database] =
    List(
      ControlledDatabase("cromwell"),
      ControlledDatabase("tes"),
      ReferenceDatabase("cromwellmetadata")
    )

  override def buildHelmOverrideValues(params: BuildHelmOverrideValuesParams)(implicit
    ev: Ask[F, AppContext]
  ): F[Values] =
    for {
      ctx <- ev.ask
      // Resolve batch account in Azure
      batchAccount <- azureBatchService.getBatchAccount(params.landingZoneResources.batchAccountName,
                                                        params.cloudContext
      )

      // Resolve application insights in Azure
      applicationInsightsComponent <- azureApplicationInsightsService.getApplicationInsights(
        params.landingZoneResources.applicationInsightsName,
        params.cloudContext
      )

      // Storage container is required for Cromwell app
      storageContainer <- F.fromOption(
        params.storageContainer,
        AppCreationException("Storage container required for Cromwell Runner app", Some(ctx.traceId))
      )

      // Databases required for Cromwell App
      dbNames <- F.fromOption(
        toCromwellRunnerAppDatabaseNames(params.databaseNames),
        AppCreationException(s"Database names required for Cromwell Runner app: ${params.databaseNames}",
                             Some(ctx.traceId)
        )
      )

      // Postgres server required for Cromwell App
      postgresServer <- F.fromOption(
        params.landingZoneResources.postgresServer,
        AppCreationException("Postgres server required for Cromwell Runner app", Some(ctx.traceId))
      )

      // Get the pet userToken
      tokenOpt <- samDao.getCachedArbitraryPetAccessToken(params.app.auditInfo.creator)
      userToken <- F.fromOption(
        tokenOpt,
        AppCreationException(s"Pet not found for user ${params.app.auditInfo.creator}", Some(ctx.traceId))
      )

      values = List(
        // azure resources configs
        raw"config.resourceGroup=${params.cloudContext.managedResourceGroupName.value}",
        raw"config.batchAccountKey=${batchAccount.getKeys().primary}",
        raw"config.batchAccountName=${params.landingZoneResources.batchAccountName.value}",
        raw"config.batchNodesSubnetId=${params.landingZoneResources.batchNodesSubnetName.value}",
        raw"config.drsUrl=${drsConfig.url}",
        raw"config.landingZoneId=${params.landingZoneResources.landingZoneId}",
        raw"config.subscriptionId=${params.cloudContext.subscriptionId.value}",
        raw"config.region=${params.landingZoneResources.region}",
        raw"config.applicationInsightsConnectionString=${applicationInsightsComponent.connectionString()}",
        raw"config.limit-true",

        // relay configs
        raw"relay.path=${params.relayPath.renderString}",

        // persistence configs
        raw"persistence.storageAccount=${params.landingZoneResources.storageAccountName.value}",
        raw"persistence.blobContainer=${storageContainer.name.value}",
        raw"persistence.leoAppInstanceName=${params.app.appName.value}",
        raw"persistence.workspaceManager.url=${params.config.wsmConfig.uri.renderString}",
        raw"persistence.workspaceManager.workspaceId=${params.workspaceId.value}",
        raw"persistence.workspaceManager.containerResourceId=${storageContainer.resourceId.value.toString}",

        // identity configs
        raw"workloadIdentity.serviceAccountName=${params.ksaName.value}",
        raw"identity.name=${params.managedIdentityName.value}",

        // Enabled services configs
        raw"cromwell.enabled=${config.enabled}",

        // general configs
        raw"fullnameOverride=cra-${params.app.release.asString}",
        raw"instrumentationEnabled=${config.instrumentationEnabled}",

        // provenance (app-cloning) configs
        raw"provenance.userAccessToken=${userToken}",

        // database configs
        raw"postgres.podLocalDatabaseEnabled=false",
        raw"postgres.host=${postgresServer.name}.postgres.database.azure.com",
        raw"postgres.pgbouncer.enabled=${postgresServer.pgBouncerEnabled}",
        // convention is that the database user is the same as the service account name
        raw"postgres.user=${params.ksaName.value}",
        raw"postgres.dbnames.cromwell=${dbNames.cromwell}",
        raw"postgres.dbnames.tes=${dbNames.tes}",
        raw"postgres.dbnames.cromwellMetadata=${dbNames.cromwellMetadata}",

        // ECM configs
        raw"ecm.baseUri=${config.ecmBaseUri}"
      )
    } yield Values(values.mkString(","))

  override def checkStatus(baseUri: Uri, authHeader: Authorization)(implicit ev: Ask[F, AppContext]): F[Boolean] =
    cromwellDao.getStatus(baseUri, authHeader).handleError(_ => false)

  def toCromwellRunnerAppDatabaseNames(
    dbResources: List[WsmControlledDatabaseResource]
  ): Option[CromwellRunnerAppDatabaseNames] =
    (dbResources
       .find(db => db.wsmDatabaseName.startsWith("cromwell") && !db.wsmDatabaseName.startsWith("cromwellmetadata"))
       .map(_.azureDatabaseName),
     getAzureDatabaseName(dbResources, "tes"),
     getAzureDatabaseName(dbResources, "cromwellmetadata")
    ).mapN(CromwellRunnerAppDatabaseNames)
}

final case class CromwellRunnerAppDatabaseNames(cromwell: String, tes: String, cromwellMetadata: String)
