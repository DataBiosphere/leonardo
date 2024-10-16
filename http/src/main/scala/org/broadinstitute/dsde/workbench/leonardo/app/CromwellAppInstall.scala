package org.broadinstitute.dsde.workbench.leonardo.app

import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.azure.{AzureApplicationInsightsService, AzureBatchService}
import org.broadinstitute.dsde.workbench.leonardo.app.AppInstall.getAzureDatabaseName
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, WsmControlledDatabaseResource}
import org.broadinstitute.dsde.workbench.leonardo.app.Database.ControlledDatabase
import org.broadinstitute.dsde.workbench.leonardo.auth.SamAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.config.CoaAppConfig
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.util.AppCreationException
import org.broadinstitute.dsp.Values
import org.http4s.Uri
import org.http4s.headers.Authorization

/**
 * Legacy Cromwell-as-an-app. Replaced by WorkflowApp and CromwellRunner app types.
 * Helm chart: https://github.com/broadinstitute/cromwhelm/tree/main/coa-helm
 */
class CromwellAppInstall[F[_]](config: CoaAppConfig,
                               drsConfig: DrsConfig,
                               samDao: SamDAO[F],
                               cromwellDao: CromwellDAO[F],
                               cbasDao: CbasDAO[F],
                               azureBatchService: AzureBatchService[F],
                               azureApplicationInsightsService: AzureApplicationInsightsService[F],
                               authProvider: SamAuthProvider[F]
)(implicit
  F: Async[F]
) extends AppInstall[F] {

  override def databases: List[Database] =
    List(
      ControlledDatabase("cromwell"),
      ControlledDatabase("cbas"),
      ControlledDatabase("tes")
    )

  override def buildHelmOverrideValues(
    params: BuildHelmOverrideValuesParams
  )(implicit ev: Ask[F, AppContext]): F[Values] = for {
    ctx <- ev.ask

    // Resolve batch account in Azure
    batchAccount <- azureBatchService.getBatchAccount(params.landingZoneResources.batchAccountName, params.cloudContext)

    // Resolve application insights in Azure
    applicationInsightsComponent <- azureApplicationInsightsService.getApplicationInsights(
      params.landingZoneResources.applicationInsightsName,
      params.cloudContext
    )

    // Storage container is required for Cromwell app
    storageContainer <- F.fromOption(
      params.storageContainer,
      AppCreationException("Storage container required for Cromwell app", Some(ctx.traceId))
    )

    // Databases required for Cromwell App
    dbNames <- F.fromOption(toCromwellAppDatabaseNames(params.databaseNames),
                            AppCreationException("Database names required for Cromwell app", Some(ctx.traceId))
    )

    // Postgres server required for Cromwell App
    postgresServer <- F.fromOption(params.landingZoneResources.postgresServer,
                                   AppCreationException("Postgres server required for Cromwell app", Some(ctx.traceId))
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

      // relay configs
      raw"relay.path=${params.relayPath.renderString}",

      // persistence configs
      raw"persistence.storageResourceGroup=${params.cloudContext.managedResourceGroupName.value}",
      raw"persistence.storageAccount=${params.landingZoneResources.storageAccountName.value}",
      raw"persistence.blobContainer=${storageContainer.name.value}",
      raw"persistence.leoAppInstanceName=${params.app.appName.value}",
      raw"persistence.workspaceManager.url=${params.config.wsmConfig.uri.renderString}",
      raw"persistence.workspaceManager.workspaceId=${params.workspaceId.value}",
      raw"persistence.workspaceManager.containerResourceId=${storageContainer.resourceId.value.toString}",

      // identity configs
      raw"identity.enabled=false",
      raw"workloadIdentity.enabled=true",
      raw"workloadIdentity.serviceAccountName=${params.ksaName.value}",
      raw"identity.name=${params.managedIdentityName.value}",

      // Sam configs
      raw"sam.url=${params.config.samConfig.server}",

      // Leo configs
      raw"leonardo.url=${params.config.leoUrlBase}",

      // Enabled services configs
      raw"cbas.enabled=true",
      raw"cromwell.enabled=true",
      raw"dockstore.baseUrl=${config.dockstoreBaseUrl}",

      // general configs
      raw"fullnameOverride=coa-${params.app.release.asString}",
      raw"instrumentationEnabled=${config.instrumentationEnabled}",

      // provenance (app-cloning) configs
      raw"provenance.userAccessToken=",

      // Database configs
      raw"postgres.podLocalDatabaseEnabled=false",
      raw"postgres.host=${postgresServer.name}.postgres.database.azure.com",
      raw"postgres.pgbouncer.enabled=${postgresServer.pgBouncerEnabled}",
      // convention is that the database user is the same as the service account name
      raw"postgres.user=${params.ksaName.value}",
      raw"postgres.dbnames.cromwell=${dbNames.cromwell}",
      raw"postgres.dbnames.cbas=${dbNames.cbas}",
      raw"postgres.dbnames.tes=${dbNames.tes}"
    )
  } yield Values(values.mkString(","))

  override def checkStatus(baseUri: Uri, authHeader: Authorization)(implicit ev: Ask[F, AppContext]): F[Boolean] =
    List(
      cromwellDao.getStatus(baseUri, authHeader).handleError(_ => false),
      cbasDao.getStatus(baseUri, authHeader).handleError(_ => false)
    ).sequence.map(_.forall(identity))

  private def toCromwellAppDatabaseNames(
    dbResources: List[WsmControlledDatabaseResource]
  ): Option[CromwellAppDatabaseNames] =
    (getAzureDatabaseName(dbResources, "cromwell"),
     getAzureDatabaseName(dbResources, "cbas"),
     getAzureDatabaseName(dbResources, "tes")
    ).mapN(CromwellAppDatabaseNames)
}

final case class CromwellAppDatabaseNames(cromwell: String, cbas: String, tes: String)
