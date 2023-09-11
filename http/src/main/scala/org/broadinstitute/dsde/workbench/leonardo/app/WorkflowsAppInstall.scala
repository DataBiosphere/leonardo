package org.broadinstitute.dsde.workbench.leonardo.app

import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.azure.{AzureApplicationInsightsService, AzureBatchService}
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.broadinstitute.dsde.workbench.leonardo.config.WorkflowsAppConfig
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.util.AppCreationException
import org.broadinstitute.dsp.Values
import org.http4s.Uri
import org.http4s.headers.Authorization

/**
 * Workflows app.
 * Helm chart: https://github.com/broadinstitute/terra-helmfile/tree/master/charts/workflows-app
 */
class WorkflowsAppInstall[F[_]](config: WorkflowsAppConfig,
                                drsConfig: DrsConfig,
                                samDao: SamDAO[F],
                                cromwellDao: CromwellDAO[F],
                                cbasDao: CbasDAO[F],
                                azureBatchService: AzureBatchService[F],
                                azureApplicationInsightsService: AzureApplicationInsightsService[F]
)(implicit
  F: Async[F]
) extends AppInstall[F] {

  override def databases: List[Database] =
    List(
      // CBAS database is only accessed by CBAS-app
      Database("cbas", allowAccessForAllWorkspaceUsers = false),
      // Cromwell metadata database is also accessed by the cromwell-runner app
      Database("cromwellmetadata", allowAccessForAllWorkspaceUsers = true)
    )

  override def buildHelmOverrideValues(
    params: BuildHelmOverrideValuesParams
  )(implicit ev: Ask[F, AppContext]): F[Values] =
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

      // Storage container is required for Workflows app
      storageContainer <- F.fromOption(
        params.storageContainer,
        AppCreationException("Storage container required for Workflows app", Some(ctx.traceId))
      )

      // Databases required for Workflows App
      dbNames <- F.fromOption(toWorkflowsAppDatabaseNames(params.databaseNames),
                              AppCreationException("Database names required for Workflows app", Some(ctx.traceId))
      )

      // Postgres server required for Workflows App
      postgresServer <- F.fromOption(
        params.landingZoneResources.postgresServer,
        AppCreationException("Postgres server required for Workflows app", Some(ctx.traceId))
      )

      // Get the pet userToken
      tokenOpt <- samDao.getCachedArbitraryPetAccessToken(params.app.auditInfo.creator)
      userToken <- F.fromOption(
        tokenOpt,
        AppCreationException(s"Pet not found for user ${params.app.auditInfo.creator}", Some(ctx.traceId))
      )

      values =
        List(
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
          raw"workloadIdentity.serviceAccountName=${params.ksaName.value}",

          // Sam configs
          raw"sam.url=${params.config.samConfig.server}",

          // Leo configs
          raw"leonardo.url=${params.config.leoUrlBase}",

          // Enabled services configs
          raw"dockstore.baseUrl=${config.dockstoreBaseUrl}",

          // general configs
          raw"fullnameOverride=wfa-${params.app.release.asString}",
          raw"instrumentationEnabled=${config.instrumentationEnabled}",

          // provenance (app-cloning) configs
          raw"provenance.userAccessToken=${userToken}",

          // database configs
          raw"postgres.podLocalDatabaseEnabled=false",
          raw"postgres.host=${postgresServer.name}.postgres.database.azure.com",
          raw"postgres.pgbouncer.enabled=${postgresServer.pgBouncerEnabled}",
          // convention is that the database user is the same as the service account name
          raw"postgres.user=${params.ksaName.value}",
          raw"postgres.dbnames.cromwellMetadata=${dbNames.cromwellMetadata}",
          raw"postgres.dbnames.cbas=${dbNames.cbas}"
        )
    } yield Values(values.mkString(","))

  override def checkStatus(baseUri: Uri, authHeader: Authorization)(implicit ev: Ask[F, AppContext]): F[Boolean] =
    List(cromwellDao.getStatus(baseUri, authHeader).handleError(_ => false),
         cbasDao.getStatus(baseUri, authHeader).handleError(_ => false)
    ).sequence
      .map(_.forall(identity))

  private def toWorkflowsAppDatabaseNames(dbNames: List[String]): Option[WorkflowsAppDatabaseNames] =
    (dbNames.find(_.startsWith("cromwellmetadata")), dbNames.find(_.startsWith("cbas")))
      .mapN(WorkflowsAppDatabaseNames)
}

final case class WorkflowsAppDatabaseNames(cromwellMetadata: String, cbas: String)
