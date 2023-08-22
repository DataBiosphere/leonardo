package org.broadinstitute.dsde.workbench
package leonardo
package util

import akka.http.scaladsl.model.StatusCodes
import bio.terra.workspace.api.ControlledAzureResourceApi
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model._
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import com.azure.core.management.AzureEnvironment
import com.azure.core.management.exception.ManagementException
import com.azure.core.management.profile.AzureProfile
import com.azure.identity.ClientSecretCredentialBuilder
import com.azure.resourcemanager.compute.ComputeManager
import com.azure.resourcemanager.compute.models.{
  ResourceIdentityType,
  VirtualMachineIdentityUserAssignedIdentities,
  VirtualMachineScaleSetIdentity,
  VirtualMachineScaleSetUpdate
}
import com.azure.resourcemanager.msi.MsiManager
import com.azure.resourcemanager.msi.models.Identity
import org.broadinstitute.dsde.workbench.DoneCheckableSyntax._
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.{KubernetesNamespace, PodStatus}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, streamUntilDoneOrTimeout, tracedRetryF}
import org.broadinstitute.dsde.workbench.leonardo.config.Config.refererConfig
import org.broadinstitute.dsde.workbench.leonardo.config.WorkflowsAppService.{Cbas, CbasUI, Cromwell}
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.http.service.{AppNotFoundException, WorkspaceNotFoundException}
import org.broadinstitute.dsde.workbench.leonardo.util.IdentityType.{NoIdentity, PodIdentity, WorkloadIdentity}
import org.broadinstitute.dsde.workbench.model.{IP, WorkbenchEmail}
import org.broadinstitute.dsp.{Release, _}
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials, Uri}
import org.typelevel.log4cats.StructuredLogger

import java.net.URL
import java.util.{Base64, UUID}
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

class AKSInterpreter[F[_]](config: AKSInterpreterConfig,
                           helmClient: HelmAlgebra[F],
                           azureBatchService: AzureBatchService[F],
                           azureContainerService: AzureContainerService[F],
                           azureApplicationInsightsService: AzureApplicationInsightsService[F],
                           azureRelayService: AzureRelayService[F],
                           samDao: SamDAO[F],
                           cromwellDao: CromwellDAO[F],
                           cbasDao: CbasDAO[F],
                           cbasUiDao: CbasUiDAO[F],
                           wdsDao: WdsDAO[F],
                           hailBatchDao: HailBatchDAO[F],
                           wsmDao: WsmDao[F],
                           kubeAlg: KubernetesAlgebra[F],
                           wsmClientProvider: WsmApiClientProvider
)(implicit
  executionContext: ExecutionContext,
  logger: StructuredLogger[F],
  dbRef: DbReference[F],
  F: Async[F]
) extends AKSAlgebra[F] {
  implicit private def booleanDoneCheckable: DoneCheckable[Boolean] = identity[Boolean]

  implicit private def listDoneCheckable[A: DoneCheckable]: DoneCheckable[List[A]] = as => as.forall(_.isDone)

  private[util] def isPodDone(podStatus: PodStatus): Boolean =
    podStatus == PodStatus.Failed || podStatus == PodStatus.Succeeded

  implicit private def podDoneCheckable: DoneCheckable[List[PodStatus]] =
    (ps: List[PodStatus]) => ps.forall(isPodDone)

  implicit private def createDatabaseDoneCheckable: DoneCheckable[CreatedControlledAzureDatabaseResult] =
    _.getJobReport.getStatus != JobReport.StatusEnum.RUNNING

  private def getListenerReleaseName(appReleaseName: Release): Release =
    Release(s"${appReleaseName.asString}-listener-rls")

  /** Creates an app and polls it for completion */
  override def createAndPollApp(params: CreateAKSAppParams)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask

      // Grab records from the database
      dbAppOpt <- KubernetesServiceDbQueries
        .getFullAppById(CloudContext.Azure(params.cloudContext), params.appId)
        .transaction
      dbApp <- F.fromOption(dbAppOpt,
                            AppNotFoundException(CloudContext.Azure(params.cloudContext),
                                                 params.appName,
                                                 ctx.traceId,
                                                 "No active app found in DB"
                            )
      )
      app = dbApp.app
      namespaceName = app.appResources.namespace.name
      kubernetesNamespace = KubernetesNamespace(namespaceName)

      _ <- logger.info(ctx.loggingCtx)(
        s"Begin app creation for app ${params.appName.value} in cloud context ${params.cloudContext.asString}"
      )

      // Create kubernetes client
      kubeClient <- kubeAlg.createAzureClient(params.cloudContext, params.landingZoneResources.clusterName)

      // Create namespace
      _ <- kubeAlg.createNamespace(kubeClient, kubernetesNamespace)

      // If configured for the app type, call WSM to create a managed identity and postgres database.
      // This returns a KSA authorized to access the database.
      (maybeWdsKSAFromDatabaseCreation, maybeDbName) <- maybeCreateWsmIdentityAndDatabase(app,
                                                                                          params.workspaceId,
                                                                                          params.landingZoneResources,
                                                                                          kubernetesNamespace
      )

      maybeCromwellDatabaseNames <- maybeCreateCromwellDatabases(app,
                                                                 params.workspaceId,
                                                                 params.landingZoneResources,
                                                                 kubernetesNamespace
      )

      (maybeWorkflowsAppKSA, maybeWorkflowsAppDatabaseNames) <- maybeCreateWorkflowsAppDatabases(
        app,
        params.workspaceId,
        params.landingZoneResources,
        kubernetesNamespace
      )

      // Determine which type of identity to link to the app: pod identity, workload identity, or nothing.
      identityType = (maybeWdsKSAFromDatabaseCreation,
                      app.samResourceId.resourceType,
                      maybeCromwellDatabaseNames,
                      maybeWorkflowsAppKSA
      ) match {
        case (Some(_), _, _, _)                      => WorkloadIdentity
        case (None, SamResourceType.SharedApp, _, _) => NoIdentity
        case (None, _, Some(_), _)                   => WorkloadIdentity
        case (None, _, _, Some(_))                   => WorkloadIdentity
        case (None, _, _, _)                         => PodIdentity
      }

      _ <- logger.info(ctx.loggingCtx)(s"Creating ${app.appType} with identity $identityType")

      // Authenticate helm client
      authContext <- getHelmAuthContext(params.landingZoneResources.clusterName, params.cloudContext, namespaceName)

      // Deploy aad-pod-identity chart
      // This only needs to be done once per cluster, but multiple helm installs have no effect.
      // See https://broadworkbench.atlassian.net/browse/IA-3804 for tracking migration to AKS Workload Identity.
      _ <- identityType match {
        case PodIdentity =>
          helmClient
            .installChart(
              config.aadPodIdentityConfig.release,
              config.aadPodIdentityConfig.chartName,
              config.aadPodIdentityConfig.chartVersion,
              config.aadPodIdentityConfig.values,
              true
            )
            .run(authContext.copy(namespace = config.aadPodIdentityConfig.namespace))
        case _ => F.unit
      }

      // Create relay hybrid connection pool
      hcName = RelayHybridConnectionName(s"${params.appName.value}-${params.workspaceId.value}")
      relayPrimaryKey <- azureRelayService.createRelayHybridConnection(params.landingZoneResources.relayNamespace,
                                                                       hcName,
                                                                       params.cloudContext
      )
      relayDomain = s"${params.landingZoneResources.relayNamespace.value}.servicebus.windows.net"
      relayEndpoint = s"https://${relayDomain}/"
      relayPath = Uri.unsafeFromString(relayEndpoint) / hcName.value

      values = BuildHelmChartValues.buildListenerChartOverrideValuesString(
        app.release,
        app.samResourceId,
        params.landingZoneResources.relayNamespace,
        hcName,
        relayPrimaryKey,
        app.appType,
        params.workspaceId,
        app.appName,
        refererConfig.validHosts + relayDomain,
        config.samConfig,
        config.listenerImage,
        config.leoUrlBase
      )

      _ <- logger.info(ctx.loggingCtx)(
        s"Relay listener values for app ${params.appName.value} are ${values.asString}"
      )

      _ <- helmClient
        .installChart(
          getListenerReleaseName(app.release),
          config.listenerChartConfig.chartName,
          config.listenerChartConfig.chartVersion,
          values,
          true
        )
        .run(authContext)

      // get the pet userToken
      tokenOpt <- samDao.getCachedArbitraryPetAccessToken(app.auditInfo.creator)
      userToken <- F.fromOption(
        tokenOpt,
        AppCreationException(s"Pet not found for user ${app.auditInfo.creator}", Some(ctx.traceId))
      )

      // private apps run as the pet managed identity, get it
      petMi <- app.samResourceId.resourceType match {
        case SamResourceType.App =>
          for {
            msi <- buildMsiManager(params.cloudContext)
            petMi <- F.delay(
              msi.identities().getById(app.googleServiceAccount.value)
            )
          } yield Some(petMi)
        case _ => F.pure(None)
      }

      // If we're configured to use pod identity with the pet for this app assign pet to the VM scale set.
      // See https://broadworkbench.atlassian.net/browse/IA-3804 for tracking migration to AKS Workload Identity
      // for all app types.
      _ <- (identityType, petMi) match {
        case (PodIdentity, Some(identity)) =>
          assignVmScaleSet(params.landingZoneResources.clusterName, params.cloudContext, identity)
        case _ => F.pure(None)
      }

      // Resolve Application Insights resource in Azure to pass to the helm chart.
      applicationInsightsComponent <- azureApplicationInsightsService.getApplicationInsights(
        params.landingZoneResources.applicationInsightsName,
        params.cloudContext
      )

      // Deploy app chart
      _ <- app.appType match {
        case AppType.Cromwell =>
          for {
            // Get the batch account key
            batchAccount <- azureBatchService.getBatchAccount(params.landingZoneResources.batchAccountName,
                                                              params.cloudContext
            )
            batchAccountKey = batchAccount.getKeys().primary

            // Storage container is required for Cromwell app
            storageContainer <- F.fromOption(
              params.storageContainer,
              AppCreationException("Storage container required for Cromwell app", Some(ctx.traceId))
            )

            _ <- helmClient
              .installChart(
                app.release,
                app.chart.name,
                app.chart.version,
                buildCromwellChartOverrideValues(
                  app.release,
                  params.appName,
                  params.cloudContext,
                  params.workspaceId,
                  params.landingZoneResources,
                  relayPath,
                  petMi,
                  storageContainer,
                  BatchAccountKey(batchAccountKey),
                  applicationInsightsComponent.connectionString(),
                  app.sourceWorkspaceId,
                  userToken,
                  identityType,
                  maybeCromwellDatabaseNames
                ),
                createNamespace = true
              )
              .run(authContext)
          } yield ()
        case AppType.Wds =>
          for {
            _ <- helmClient
              .installChart(
                app.release,
                app.chart.name,
                app.chart.version,
                buildWdsChartOverrideValues(
                  app.release,
                  params.appName,
                  params.cloudContext,
                  params.workspaceId,
                  params.landingZoneResources,
                  petMi,
                  applicationInsightsComponent.connectionString(),
                  app.sourceWorkspaceId,
                  userToken, // TODO: Remove once permanent solution utilizing the multi-user sam app identity has been implemented
                  identityType,
                  maybeWdsKSAFromDatabaseCreation,
                  maybeDbName
                ),
                createNamespace = true
              )
              .run(authContext)
          } yield ()
        case AppType.HailBatch =>
          for {
            // Storage container is required for HailBatch app
            storageContainer <- F.fromOption(
              params.storageContainer,
              AppCreationException("Storage container required for Hail Batch app", Some(ctx.traceId))
            )
            _ <- helmClient
              .installChart(
                app.release,
                app.chart.name,
                app.chart.version,
                buildHailBatchChartOverrideValues(
                  params.appName,
                  params.workspaceId,
                  params.landingZoneResources,
                  petMi,
                  storageContainer,
                  relayDomain,
                  hcName
                ),
                createNamespace = true
              )
              .run(authContext)
          } yield ()
        case AppType.WorkflowsApp =>
          for {
            // Get the batch account key
            batchAccount <- azureBatchService.getBatchAccount(params.landingZoneResources.batchAccountName,
                                                              params.cloudContext
            )

            batchAccountKey = batchAccount.getKeys().primary

            // Storage container is required for Workflows app
            storageContainer <- F.fromOption(
              params.storageContainer,
              AppCreationException("Storage container required for Workflows app", Some(ctx.traceId))
            )
            _ <- helmClient
              .installChart(
                app.release,
                app.chart.name,
                app.chart.version,
                buildWorkflowsAppChartOverrideValues(
                  app.release,
                  params.appName,
                  params.cloudContext,
                  params.workspaceId,
                  params.landingZoneResources,
                  relayPath,
                  petMi,
                  storageContainer,
                  BatchAccountKey(batchAccountKey),
                  applicationInsightsComponent.connectionString(),
                  app.sourceWorkspaceId,
                  userToken,
                  identityType,
                  maybeWorkflowsAppKSA,
                  maybeWorkflowsAppDatabaseNames
                ),
                createNamespace = true
              )
              .run(authContext)
          } yield ()
        case _ => F.raiseError(AppCreationException(s"App type ${app.appType} not supported on Azure"))
      }

      appOk <- pollAppCreation(app.auditInfo.creator, relayPath, app.appType)
      _ <-
        if (appOk)
          F.unit
        else
          F.raiseError[Unit](
            AppCreationException(
              s"App ${params.appName.value} failed to start in cluster ${params.landingZoneResources.clusterName.value} in cloud context ${params.cloudContext.asString}",
              Some(ctx.traceId)
            )
          )

      // Populate async fields in the KUBERNETES_CLUSTER table.
      // For Azure we don't need each field, but we do need the relay https endpoint.
      _ <- kubernetesClusterQuery
        .updateAsyncFields(
          dbApp.cluster.id,
          KubernetesClusterAsyncFields(
            IP(relayEndpoint),
            IP("[unset]"),
            NetworkFields(
              params.landingZoneResources.vnetName,
              params.landingZoneResources.aksSubnetName,
              IpRange("[unset]")
            )
          )
        )
        .transaction

      // If we've got here, update the App status to Running.
      _ <- appQuery.updateStatus(params.appId, AppStatus.Running).transaction

      _ <- logger.info(ctx.loggingCtx)(
        s"Finished app creation for app ${params.appName.value} in cluster ${params.landingZoneResources.clusterName.value} in cloud context ${params.cloudContext.asString}"
      )

    } yield ()

  override def updateAndPollApp(params: UpdateAKSAppParams)(implicit ev: Ask[F, AppContext]): F[Unit] = {
    for {
      ctx <- ev.ask

      workspaceId <- F.fromOption(
        params.workspaceId,
        AppUpdateException(
          s"${params.appName} must have a Workspace in the Azure cloud context",
          Some(ctx.traceId)
        )
      )

      // Grab records from the database
      dbAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(CloudContext.Azure(params.cloudContext), params.appName)
        .transaction
      dbApp <- F.fromOption(
        dbAppOpt,
        AppNotFoundException(CloudContext.Azure(params.cloudContext),
                             params.appName,
                             ctx.traceId,
                             "No active app found in DB"
        )
      )
      _ <- logger.info(ctx.loggingCtx)(s"Updating app ${params.appName} in workspace ${params.workspaceId}")

      app = dbApp.app
      namespaceName = app.appResources.namespace.name
      kubernetesNamespace = KubernetesNamespace(namespaceName)

      // Grab the LZ and storage container information associated with the workspace
      leoAuth <- samDao.getLeoAuthToken
      workspaceDescOpt <- wsmDao.getWorkspace(workspaceId, leoAuth)
      workspaceDesc <- F.fromOption(workspaceDescOpt, WorkspaceNotFoundException(workspaceId, ctx.traceId))
      landingZoneResources <- wsmDao.getLandingZoneResources(workspaceDesc.spendProfile, leoAuth)

      // Get the optional storage container for the workspace
      storageContainer <- wsmDao.getWorkspaceStorageContainer(workspaceId, leoAuth)

      // Build WSM client
      token <- leoAuth.credentials match {
        case org.http4s.Credentials.Token(_, token) => F.pure(token)
        case _ => F.raiseError(new RuntimeException("Could not obtain Leo auth token"))
      }
      wsmApi = wsmClientProvider.getControlledAzureResourceApi(token)

      // Resolve pet managed identity in Azure
      // Only do this for user-private apps; do not assign any identity for shared apps.
      // In the future we may use a shared identity instead.
      petMi <- app.samResourceId.resourceType match {
        case SamResourceType.SharedApp => F.pure(None)
        case _ =>
          for {
            msi <- buildMsiManager(params.cloudContext)
            petMi <- F.delay(
              msi.identities().getById(app.googleServiceAccount.value)
            )
          } yield Some(petMi)
      }

      // Get relay hybrid connection information
      hcName = RelayHybridConnectionName(s"${params.appName.value}-${workspaceId.value}")
      relayDomain = s"${landingZoneResources.relayNamespace.value}.servicebus.windows.net"
      relayEndpoint = s"https://${relayDomain}/"
      relayPath = Uri.unsafeFromString(relayEndpoint) / hcName.value
      relayPrimaryKey <- azureRelayService.getRelayHybridConnectionKey(landingZoneResources.relayNamespace,
                                                                       hcName,
                                                                       params.cloudContext
      )

      // Authenticate helm client
      authContext <- getHelmAuthContext(landingZoneResources.clusterName, params.cloudContext, namespaceName)

      // Update the relay listener deployment
      _ <- updateListener(authContext,
                          app,
                          landingZoneResources,
                          workspaceId,
                          hcName,
                          relayPrimaryKey,
                          relayDomain,
                          config.listenerChartConfig
      )

      // Generate the app values to pass to helm at the upgrade chart step
      chartOverrideValues <- app.appType match {
        case AppType.Cromwell =>
          for {
            // Get the batch account key
            batchAccount <- azureBatchService.getBatchAccount(landingZoneResources.batchAccountName,
                                                              params.cloudContext
            )
            batchAccountKey = batchAccount.getKeys().primary
            // Storage container is required for Cromwell app
            storageContainer <- F.fromOption(
              storageContainer,
              AppUpdateException("Storage container required for Cromwell app", Some(ctx.traceId))
            )
            // Resolve Application Insights resource in Azure to pass to the helm chart.
            applicationInsightsComponent <- azureApplicationInsightsService.getApplicationInsights(
              landingZoneResources.applicationInsightsName,
              params.cloudContext
            )
            // get the pet userToken
            tokenOpt <- samDao.getCachedArbitraryPetAccessToken(app.auditInfo.creator)
            userToken <- F.fromOption(
              tokenOpt,
              AppUpdateException(s"Pet not found for user ${app.auditInfo.creator}", Some(ctx.traceId))
            )

            // Call WSM to get the managed identity if it exists
            wsmIdentities <- appControlledResourceQuery
              .getAllForAppByType(app.id.id, WsmResourceType.AzureManagedIdentity)
              .transaction
            maybeKsaFromDatabaseCreation <- wsmIdentities.headOption.traverse { wsmIdentity =>
              F.delay(wsmApi.getAzureManagedIdentity(workspaceId.value, wsmIdentity.resourceId.value)).map { resource =>
                ServiceAccountName(resource.getMetadata.getName)
              }
            }

            // Call WSM to get the postgres databases if they exist
            wsmDatabases <- appControlledResourceQuery
              .getAllForAppByType(app.id.id, WsmResourceType.AzureDatabase)
              .transaction
            wsmDbNames <- wsmDatabases.traverse { wsmDatabase =>
              F.delay(wsmApi.getAzureDatabase(workspaceId.value, wsmDatabase.resourceId.value))
                .map(_.getMetadata.getName)
            }
            maybeDbNames = (wsmDbNames.find(_.startsWith("cromwell")),
                            wsmDbNames.find(_.startsWith("cbas")),
                            wsmDbNames.find(_.startsWith("tes"))
            ).mapN(CromwellDatabaseNames)

            // Determine which type of identity to link to the app: pod identity, workload identity, or nothing.
            identityType = (maybeKsaFromDatabaseCreation, app.samResourceId.resourceType, maybeDbNames) match {
              case (Some(_), _, _)                      => WorkloadIdentity
              case (None, SamResourceType.SharedApp, _) => NoIdentity
              case (None, _, Some(_))                   => WorkloadIdentity
              case (None, _, _)                         => PodIdentity
            }

          } yield buildCromwellChartOverrideValues(
            app.release,
            params.appName,
            params.cloudContext,
            workspaceId,
            landingZoneResources,
            relayPath,
            petMi,
            storageContainer,
            BatchAccountKey(batchAccountKey),
            applicationInsightsComponent.connectionString(),
            app.sourceWorkspaceId,
            userToken,
            identityType,
            maybeDbNames
          )
        case AppType.Wds =>
          for {
            // Resolve Application Insights resource in Azure to pass to the helm chart.
            applicationInsightsComponent <- azureApplicationInsightsService.getApplicationInsights(
              landingZoneResources.applicationInsightsName,
              params.cloudContext
            )
            // get the pet userToken
            tokenOpt <- samDao.getCachedArbitraryPetAccessToken(app.auditInfo.creator)
            userToken <- F.fromOption(
              tokenOpt,
              AppUpdateException(s"Pet not found for user ${app.auditInfo.creator}", Some(ctx.traceId))
            )

            // Call WSM to get the managed identity if it exists
            wsmIdentities <- appControlledResourceQuery
              .getAllForAppByType(app.id.id, WsmResourceType.AzureManagedIdentity)
              .transaction
            maybeKsaFromDatabaseCreation <- wsmIdentities.headOption.traverse { wsmIdentity =>
              F.delay(wsmApi.getAzureManagedIdentity(workspaceId.value, wsmIdentity.resourceId.value)).map { resource =>
                ServiceAccountName(resource.getMetadata.getName)
              }
            }

            // Call WSM to get the postgres database if it exists
            wsmDatabases <- appControlledResourceQuery
              .getAllForAppByType(app.id.id, WsmResourceType.AzureDatabase)
              .transaction
            maybeDbName <- wsmDatabases.headOption.traverse { wsmDatabase =>
              F.delay(wsmApi.getAzureDatabase(workspaceId.value, wsmDatabase.resourceId.value))
                .map(_.getMetadata.getName)
            }

            // Determine which type of identity to link to the app: pod identity, workload identity, or nothing.
            identityType = (maybeKsaFromDatabaseCreation, app.samResourceId.resourceType) match {
              case (Some(_), _)                      => WorkloadIdentity
              case (None, SamResourceType.SharedApp) => NoIdentity
              case (None, _)                         => PodIdentity
            }
          } yield buildWdsChartOverrideValues(
            app.release,
            params.appName,
            params.cloudContext,
            workspaceId,
            landingZoneResources,
            petMi,
            applicationInsightsComponent.connectionString(),
            app.sourceWorkspaceId,
            userToken,
            identityType,
            maybeKsaFromDatabaseCreation,
            maybeDbName
          )
        case AppType.HailBatch =>
          for {
            // Storage container is required for HailBatch app
            storageContainer <- F.fromOption(
              storageContainer,
              AppUpdateException("Storage container required for Hail Batch app", Some(ctx.traceId))
            )
          } yield buildHailBatchChartOverrideValues(
            params.appName,
            workspaceId,
            landingZoneResources,
            petMi,
            storageContainer,
            relayDomain,
            hcName
          )
        case _ => F.raiseError(AppUpdateException(s"App type ${app.appType} not supported on Azure", Some(ctx.traceId)))
      }

      // Upgrade app chart version and explicitly pass the values
      _ <- helmClient
        .upgradeChart(
          app.release,
          app.chart.name,
          params.appChartVersion,
          chartOverrideValues
        )
        .run(authContext)

      // Poll until all pods in the app namespace are running
      appOk <- pollAppUpdate(app.auditInfo.creator, relayPath, app.appType)
      _ <-
        if (appOk)
          F.unit
        else
          F.raiseError[Unit](
            AppUpdateException(
              s"App ${params.appName.value} failed to update in cluster ${landingZoneResources.clusterName.value} in cloud context ${params.cloudContext.asString}",
              Some(ctx.traceId)
            )
          )

      _ <- logger.info(
        s"Update app operation has finished for app ${app.appName.value} in cluster ${landingZoneResources.clusterName}"
      )

      // Update app chart version in the DB
      _ <- appQuery.updateChart(app.id, Chart(app.chart.name, params.appChartVersion)).transaction
      // Put app status back to running
      _ <- appQuery.updateStatus(app.id, AppStatus.Running).transaction

      _ <- logger.info(s"Done updating app ${params.appName} in workspace ${params.workspaceId}")
    } yield ()
  }

  override def deleteApp(params: DeleteAKSAppParams)(implicit ev: Ask[F, AppContext]): F[Unit] = {
    val DeleteAKSAppParams(appName, workspaceId, landingZoneResources, cloudContext) = params
    for {
      ctx <- ev.ask

      // Grab records from the database
      dbAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(CloudContext.Azure(cloudContext), params.appName)
        .transaction
      dbApp <- F.fromOption(
        dbAppOpt,
        AppNotFoundException(CloudContext.Azure(cloudContext), params.appName, ctx.traceId, "No active app found in DB")
      )
      _ <- logger.info(ctx.loggingCtx)(s"Deleting app $appName in workspace $workspaceId")

      app = dbApp.app

      // Delete WSM resources associated with the app
      // Ideally an app only consists of WSM resources, as more resources are moved into WSM
      // everything between deleteAppWsmResources and appQuery.markAsDeleted should be removed
      _ <- deleteAppWsmResources(app, params.workspaceId)

      namespaceName = app.appResources.namespace.name
      kubernetesNamespace = KubernetesNamespace(namespaceName)

      // Delete hybrid connection for this app
      // for backwards compatibility, name used to be just the appName
      name = app.customEnvironmentVariables.getOrElse("RELAY_HYBRID_CONNECTION_NAME", app.appName.value)

      _ <- azureRelayService
        .deleteRelayHybridConnection(
          landingZoneResources.relayNamespace,
          RelayHybridConnectionName(name),
          cloudContext
        )
        .handleErrorWith {
          case e: ManagementException if e.getResponse.getStatusCode == StatusCodes.NotFound.intValue =>
            logger.info(s"${name} does not exist to delete in ${cloudContext}")
          case e => F.raiseError[Unit](e)
        }

      client <- kubeAlg.createAzureClient(cloudContext, landingZoneResources.clusterName)

      // Delete the namespace which should delete all resources in it
      _ <- kubeAlg.deleteNamespace(client, kubernetesNamespace)

      // Poll until the namespace is actually deleted
      // Mapping to inverse because booleanDoneCheckable defines `Done` when it becomes `true`
      fa = kubeAlg.namespaceExists(client, kubernetesNamespace).map(exists => !exists)
      _ <- streamUntilDoneOrTimeout(fa,
                                    config.appMonitorConfig.deleteApp.maxAttempts,
                                    config.appMonitorConfig.deleteApp.initialDelay,
                                    "delete namespace timed out"
      )

      // Delete the Sam resource
      userEmail = app.auditInfo.creator
      tokenOpt <- samDao.getCachedArbitraryPetAccessToken(userEmail)
      _ <- tokenOpt match {
        case Some(token) =>
          samDao.deleteResourceInternal(dbApp.app.samResourceId,
                                        Authorization(Credentials.Token(AuthScheme.Bearer, token))
          )
        case None =>
          logger.warn(
            s"Could not find pet service account for user ${userEmail} in Sam. Skipping resource deletion in Sam."
          )
      }

      _ <- logger.info(
        s"Delete app operation has finished for app ${app.appName.value} in workspace ${app.workspaceId}"
      )

      _ <- appQuery.markAsDeleted(app.id, ctx.now).transaction

      _ <- logger.info(s"Done deleting app $appName in workspace $workspaceId")
    } yield ()
  }

  private[util] def pollApp(userEmail: WorkbenchEmail, relayBaseUri: Uri, appType: AppType)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] = for {
    ctx <- ev.ask
    tokenOpt <- samDao.getCachedArbitraryPetAccessToken(userEmail)
    token <- F.fromOption(tokenOpt, AppCreationException(s"Pet not found for user ${userEmail}", Some(ctx.traceId)))
    authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, token))

    op <- appType match {
      case AppType.Cromwell =>
        // Status check each configured coa service for Cromwell app type
        config.coaAppConfig.coaServices
          .collect {
            case Cbas =>
              cbasDao.getStatus(relayBaseUri, authHeader).handleError(_ => false)
            case CbasUI =>
              cbasUiDao.getStatus(relayBaseUri, authHeader).handleError(_ => false)
            case Cromwell =>
              cromwellDao.getStatus(relayBaseUri, authHeader).handleError(_ => false)
          }
          .toList
          .sequence
          .map(_.forall(identity))
      case AppType.WorkflowsApp =>
        config.workflowsAppConfig.workflowsAppServices
          .collect {
            case Cromwell =>
              cromwellDao.getStatus(relayBaseUri, authHeader).handleError(_ => false)
            case Cbas =>
              cbasDao.getStatus(relayBaseUri, authHeader).handleError(_ => false)
          }
          .toList
          .sequence
          .map(_.forall(identity))
      case AppType.CromwellRunnerApp =>
        cromwellDao.getStatus(relayBaseUri, authHeader).handleError(_ => false)
      case AppType.Wds =>
        wdsDao.getStatus(relayBaseUri, authHeader).handleError(_ => false)
      case AppType.HailBatch =>
        hailBatchDao.getStatus(relayBaseUri, authHeader).handleError(_ => false)
      case _ => F.raiseError[Boolean](AppCreationException(s"App type ${appType} not supported on Azure"))
    }
  } yield op

  private[util] def pollAppCreation(userEmail: WorkbenchEmail, relayBaseUri: Uri, appType: AppType)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] =
    for {
      _ <- ev.ask
      op = pollApp(userEmail, relayBaseUri, appType)

      appOk <- streamFUntilDone(
        op,
        maxAttempts = config.appMonitorConfig.createApp.maxAttempts,
        delay = config.appMonitorConfig.createApp.interval
      ).interruptAfter(config.appMonitorConfig.createApp.interruptAfter).compile.lastOrError
    } yield appOk.isDone

  private[util] def pollAppUpdate(userEmail: WorkbenchEmail, relayBaseUri: Uri, appType: AppType)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] =
    for {
      _ <- ev.ask
      op = pollApp(userEmail, relayBaseUri, appType)
      appOk <- streamFUntilDone(
        op,
        maxAttempts = config.appMonitorConfig.updateApp.maxAttempts,
        delay = config.appMonitorConfig.updateApp.interval
      ).interruptAfter(config.appMonitorConfig.updateApp.interruptAfter).compile.lastOrError
    } yield appOk.isDone

  private[util] def buildCromwellChartOverrideValues(release: Release,
                                                     appName: AppName,
                                                     cloudContext: AzureCloudContext,
                                                     workspaceId: WorkspaceId,
                                                     landingZoneResources: LandingZoneResources,
                                                     relayPath: Uri,
                                                     petManagedIdentity: Option[Identity],
                                                     storageContainer: StorageContainerResponse,
                                                     batchAccountKey: BatchAccountKey,
                                                     applicationInsightsConnectionString: String,
                                                     sourceWorkspaceId: Option[WorkspaceId],
                                                     userAccessToken: String,
                                                     identityType: IdentityType,
                                                     maybeDatabaseNames: Option[CromwellDatabaseNames]
  ): Values = {
    val valuesList =
      List(
        // azure resources configs
        raw"config.resourceGroup=${cloudContext.managedResourceGroupName.value}",
        raw"config.batchAccountKey=${batchAccountKey.value}",
        raw"config.batchAccountName=${landingZoneResources.batchAccountName.value}",
        raw"config.batchNodesSubnetId=${landingZoneResources.batchNodesSubnetName.value}",
        raw"config.drsUrl=${config.drsConfig.url}",
        raw"config.landingZoneId=${landingZoneResources.landingZoneId}",
        raw"config.subscriptionId=${cloudContext.subscriptionId.value}",
        raw"config.region=${landingZoneResources.region}",
        raw"config.applicationInsightsConnectionString=${applicationInsightsConnectionString}",

        // relay configs
        raw"relay.path=${relayPath.renderString}",

        // persistence configs
        raw"persistence.storageResourceGroup=${cloudContext.managedResourceGroupName.value}",
        raw"persistence.storageAccount=${landingZoneResources.storageAccountName.value}",
        raw"persistence.blobContainer=${storageContainer.name.value}",
        raw"persistence.leoAppInstanceName=${appName.value}",
        raw"persistence.workspaceManager.url=${config.wsmConfig.uri.renderString}",
        raw"persistence.workspaceManager.workspaceId=${workspaceId.value}",
        raw"persistence.workspaceManager.containerResourceId=${storageContainer.resourceId.value.toString}",

        // identity configs
        raw"identity.enabled=${identityType == PodIdentity}",
        raw"identity.name=${petManagedIdentity.map(_.name).getOrElse("none")}",
        raw"identity.resourceId=${petManagedIdentity.map(_.id).getOrElse("none")}",
        raw"identity.clientId=${petManagedIdentity.map(_.clientId).getOrElse("none")}",
        raw"workloadIdentity.enabled=${identityType == WorkloadIdentity}",
        raw"workloadIdentity.serviceAccountName=${petManagedIdentity.map(_.name).getOrElse("none")}",

        // Sam configs
        raw"sam.url=${config.samConfig.server}",

        // Leo configs
        raw"leonardo.url=${config.leoUrlBase}",

        // Enabled services configs
        raw"cbas.enabled=${config.coaAppConfig.coaServices.contains(Cbas)}",
        raw"cbasUI.enabled=${config.coaAppConfig.coaServices.contains(CbasUI)}",
        raw"cromwell.enabled=${config.coaAppConfig.coaServices.contains(Cromwell)}",
        raw"dockstore.baseUrl=${config.coaAppConfig.dockstoreBaseUrl}",

        // general configs
        raw"fullnameOverride=coa-${release.asString}",
        raw"instrumentationEnabled=${config.coaAppConfig.instrumentationEnabled}",
        // provenance (app-cloning) configs
        raw"provenance.userAccessToken=${userAccessToken}"
      )

    val postgresConfig = (maybeDatabaseNames, landingZoneResources.postgresName, petManagedIdentity) match {
      case (Some(databaseNames), Some(PostgresName(dbServer)), Some(pet)) =>
        List(
          raw"postgres.podLocalDatabaseEnabled=false",
          raw"postgres.host=$dbServer.postgres.database.azure.com",
          // convention is that the database user is the same as the service account name
          raw"postgres.user=${pet.name()}",
          raw"postgres.dbnames.cromwell=${databaseNames.cromwell}",
          raw"postgres.dbnames.cbas=${databaseNames.cbas}",
          raw"postgres.dbnames.tes=${databaseNames.tes}"
        )
      case _ => List.empty
    }

    Values((valuesList ++ postgresConfig).mkString(","))
  }

  private[util] def buildWdsChartOverrideValues(release: Release,
                                                appName: AppName,
                                                cloudContext: AzureCloudContext,
                                                workspaceId: WorkspaceId,
                                                landingZoneResources: LandingZoneResources,
                                                petManagedIdentity: Option[Identity],
                                                applicationInsightsConnectionString: String,
                                                sourceWorkspaceId: Option[WorkspaceId],
                                                userAccessToken: String,
                                                identityType: IdentityType,
                                                ksaName: Option[ServiceAccountName],
                                                wdsDbName: Option[String]
  ): Values = {
    val valuesList =
      List(
        // azure resources configs
        raw"config.resourceGroup=${cloudContext.managedResourceGroupName.value}",
        raw"config.applicationInsightsConnectionString=${applicationInsightsConnectionString}",

        // Azure subscription configs currently unused
        raw"config.subscriptionId=${cloudContext.subscriptionId.value}",
        raw"config.region=${landingZoneResources.region}",

        // persistence configs
        raw"general.leoAppInstanceName=${appName.value}",
        raw"general.workspaceManager.workspaceId=${workspaceId.value}",

        // identity configs
        raw"identity.enabled=${identityType == PodIdentity}",
        raw"identity.name=${petManagedIdentity.map(_.name).getOrElse("none")}",
        raw"identity.resourceId=${petManagedIdentity.map(_.id).getOrElse("none")}",
        raw"identity.clientId=${petManagedIdentity.map(_.clientId).getOrElse("none")}",
        raw"workloadIdentity.enabled=${identityType == WorkloadIdentity}",
        raw"workloadIdentity.serviceAccountName=${ksaName.map(_.value).getOrElse("none")}",

        // Sam configs
        raw"sam.url=${config.samConfig.server}",

        // Leo configs
        raw"leonardo.url=${config.leoUrlBase}",

        // workspace manager
        raw"workspacemanager.url=${config.wsmConfig.uri.renderString}",

        // general configs
        raw"fullnameOverride=wds-${release.asString}",
        raw"instrumentationEnabled=${config.wdsAppConfig.instrumentationEnabled}",

        // import configs
        raw"import.dataRepoUrl=${config.tdr.url}",

        // provenance (app-cloning) configs
        raw"provenance.userAccessToken=${userAccessToken}",
        raw"provenance.sourceWorkspaceId=${sourceWorkspaceId.map(_.value).getOrElse("")}"
      )

    val postgresConfig = (ksaName, wdsDbName, landingZoneResources.postgresName) match {
      case (Some(ksa), Some(db), Some(PostgresName(dbServer))) =>
        List(
          raw"postgres.podLocalDatabaseEnabled=false",
          raw"postgres.host=$dbServer.postgres.database.azure.com",
          raw"postgres.dbname=$db",
          // convention is that the database user is the same as the service account name
          raw"postgres.user=${ksa.value}"
        )
      case _ => List.empty
    }

    Values((valuesList ++ postgresConfig).mkString(","))
  }

  private[util] def buildHailBatchChartOverrideValues(appName: AppName,
                                                      workspaceId: WorkspaceId,
                                                      landingZoneResources: LandingZoneResources,
                                                      petManagedIdentity: Option[Identity],
                                                      storageContainer: StorageContainerResponse,
                                                      relayDomain: String,
                                                      hcName: RelayHybridConnectionName
  ): Values =
    Values(
      List(
        raw"persistence.storageAccount=${landingZoneResources.storageAccountName.value}",
        raw"persistence.blobContainer=${storageContainer.name.value}",
        raw"persistence.workspaceManager.url=${config.wsmConfig.uri.renderString}",
        raw"persistence.workspaceManager.workspaceId=${workspaceId.value}",
        raw"persistence.workspaceManager.containerResourceId=${storageContainer.resourceId.value.toString}",
        raw"persistence.workspaceManager.storageContainerUrl=https://${landingZoneResources.storageAccountName.value}.blob.core.windows.net/${storageContainer.name.value}",
        raw"persistence.leoAppName=${appName.value}",

        // identity configs
        raw"identity.name=${petManagedIdentity.map(_.name).getOrElse("none")}",
        raw"identity.resourceId=${petManagedIdentity.map(_.id).getOrElse("none")}",
        raw"identity.clientId=${petManagedIdentity.map(_.clientId).getOrElse("none")}",
        raw"relay.domain=${relayDomain}",
        raw"relay.subpath=/${hcName.value}"
      ).mkString(",")
    )

  private[util] def buildWorkflowsAppChartOverrideValues(release: Release,
                                                         appName: AppName,
                                                         cloudContext: AzureCloudContext,
                                                         workspaceId: WorkspaceId,
                                                         landingZoneResources: LandingZoneResources,
                                                         relayPath: Uri,
                                                         petManagedIdentity: Option[Identity],
                                                         storageContainer: StorageContainerResponse,
                                                         batchAccountKey: BatchAccountKey,
                                                         applicationInsightsConnectionString: String,
                                                         sourceWorkspaceId: Option[WorkspaceId],
                                                         userAccessToken: String,
                                                         identityType: IdentityType,
                                                         ksaName: Option[ServiceAccountName],
                                                         maybeDatabaseNames: Option[_]
  ): Values = {

    val valuesList =
      List(
        // azure resources configs
        raw"config.resourceGroup=${cloudContext.managedResourceGroupName.value}",
        raw"config.batchAccountKey=${batchAccountKey.value}",
        raw"config.batchAccountName=${landingZoneResources.batchAccountName.value}",
        raw"config.batchNodesSubnetId=${landingZoneResources.batchNodesSubnetName.value}",
        raw"config.drsUrl=${config.drsConfig.url}",
        raw"config.landingZoneId=${landingZoneResources.landingZoneId}",
        raw"config.subscriptionId=${cloudContext.subscriptionId.value}",
        raw"config.region=${landingZoneResources.region}",
        raw"config.applicationInsightsConnectionString=${applicationInsightsConnectionString}",

        // relay configs
        raw"relay.path=${relayPath.renderString}",

        // persistence configs
        raw"persistence.storageResourceGroup=${cloudContext.managedResourceGroupName.value}",
        raw"persistence.storageAccount=${landingZoneResources.storageAccountName.value}",
        raw"persistence.blobContainer=${storageContainer.name.value}",
        raw"persistence.leoAppInstanceName=${appName.value}",
        raw"persistence.workspaceManager.url=${config.wsmConfig.uri.renderString}",
        raw"persistence.workspaceManager.workspaceId=${workspaceId.value}",
        raw"persistence.workspaceManager.containerResourceId=${storageContainer.resourceId.value.toString}",

        // identity configs
        raw"identity.enabled=${identityType == PodIdentity}",
        raw"identity.name=${petManagedIdentity.map(_.name).getOrElse("none")}",
        raw"identity.resourceId=${petManagedIdentity.map(_.id).getOrElse("none")}",
        raw"identity.clientId=${petManagedIdentity.map(_.clientId).getOrElse("none")}",
        raw"workloadIdentity.enabled=${identityType == WorkloadIdentity}",
        raw"workloadIdentity.serviceAccountName=${ksaName.map(_.value).getOrElse("none")}",

        // Sam configs
        raw"sam.url=${config.samConfig.server}",

        // Leo configs
        raw"leonardo.url=${config.leoUrlBase}",

        // Enabled services configs
        // raw"cbas.enabled=${config.workflowsAppConfig.workflowsAppServices.contains(Cbas)}",
        // raw"cromwell.enabled=${config.workflowsAppConfig.workflowsAppServices.contains(Cromwell)}",
        raw"dockstore.baseUrl=${config.workflowsAppConfig.dockstoreBaseUrl}",

        // general configs
        raw"fullnameOverride=workflows-app-${release.asString}",
        raw"instrumentationEnabled=${config.workflowsAppConfig.instrumentationEnabled}",
        // provenance (app-cloning) configs
        raw"provenance.userAccessToken=${userAccessToken}"
      )

    val postgresConfig = (maybeDatabaseNames, landingZoneResources.postgresName, ksaName) match {
      case (Some(_), Some(PostgresName(dbServer)), Some(ksa)) =>
        List(
          raw"postgres.podLocalDatabaseEnabled=false",
          raw"postgres.host=$dbServer.postgres.database.azure.com",
          // convention is that the database user is the same as the service account name
          raw"postgres.user=${ksa.value}",
          raw"postgres.dbnames.cromwellMetadata=cromwellmetadata", // TODO: WM-2159
          raw"postgres.dbnames.cbas=cbas"
        )
      case _ => List.empty
    }

    Values((valuesList ++ postgresConfig).mkString(","))
  }

  private[util] def assignVmScaleSet(clusterName: AKSClusterName,
                                     cloudContext: AzureCloudContext,
                                     petManagedIdentity: Identity
  )(implicit ev: Ask[F, AppContext]): F[Unit] = for {
    // Resolve the cluster in Azure
    cluster <- azureContainerService.getCluster(clusterName, cloudContext)

    // Resolve the VM scale set backing the node pool
    // Note: we are making the assumption here that there is 1 node pool per cluster.
    compute <- buildComputeManager(cloudContext)
    getFirstVmScaleSet = for {
      vmScaleSets <- F.delay(compute.virtualMachineScaleSets().listByResourceGroup(cluster.nodeResourceGroup()))
      vmScaleSet <- F
        .fromOption(
          vmScaleSets.iterator().asScala.nextOption(),
          AppCreationException(
            s"VM scale set not found for cluster ${cloudContext.managedResourceGroupName.value}/${clusterName.value}"
          )
        )
    } yield vmScaleSet

    // Retry getting the VM scale set since Azure returns an empty list sporadically for some reason
    retryConfig = RetryPredicates.retryAllConfig
    vmScaleSet <- tracedRetryF(retryConfig)(
      getFirstVmScaleSet,
      s"Get VM scale set for cluster ${cloudContext.managedResourceGroupName.value}/${clusterName.value}"
    ).compile.lastOrError

    // Assign VM scale set to the pet UAMI (if not already assigned).
    //
    // Note: normally this is done behind the scenes by aad-pod-identity. However in our case the deny assignments
    // block it, so we need to use "Managed" mode handle the assignment ourselves. For more info see:
    // https://azure.github.io/aad-pod-identity/docs/configure/standard_to_managed_mode/
    //
    // Note also that we are using the service client instead of the fluent API to do this, because the fluent API
    // makes a POST request instead of a PATCH, leading to errors. (Possible Java SDK bug?)
    existingUamis = vmScaleSet.userAssignedManagedServiceIdentityIds().asScala
    _ <-
      if (existingUamis.contains(petManagedIdentity.id)) {
        F.unit
      } else {
        F.delay(
          compute
            .serviceClient()
            .getVirtualMachineScaleSets
            .update(
              cluster.nodeResourceGroup,
              vmScaleSet.name(),
              new VirtualMachineScaleSetUpdate()
                .withIdentity(
                  new VirtualMachineScaleSetIdentity()
                    .withType(ResourceIdentityType.USER_ASSIGNED)
                    .withUserAssignedIdentities(
                      (petManagedIdentity.id :: existingUamis.toList)
                        .map(_ -> new VirtualMachineIdentityUserAssignedIdentities())
                        .toMap
                        .asJava
                    )
                )
            )
        )
      }
  } yield ()

  private[util] def getHelmAuthContext(clusterName: AKSClusterName,
                                       cloudContext: AzureCloudContext,
                                       namespaceName: NamespaceName
  )(implicit ev: Ask[F, AppContext]): F[AuthContext] =
    for {
      ctx <- ev.ask

      credentials <- azureContainerService.getClusterCredentials(clusterName, cloudContext)

      // Don't use AppContext.now for the tmp file name because we want it to be unique
      // for each helm invocation
      now <- nowInstant

      // The helm client requires the ca cert passed as a file - hence writing a temp file before helm invocation.
      caCertFile <- writeTempFile(s"aks_ca_cert_${now.toEpochMilli}",
                                  Base64.getDecoder.decode(credentials.certificate.value)
      )

      authContext = AuthContext(
        Namespace(namespaceName.value),
        KubeToken(credentials.token.value),
        KubeApiServer(credentials.server.value),
        CaCertFile(caCertFile.toAbsolutePath)
      )

      _ <- logger.info(ctx.loggingCtx)(
        s"Helm auth context for cluster ${clusterName.value} in cloud context ${cloudContext.asString}: ${authContext
            .copy(kubeToken = org.broadinstitute.dsp.KubeToken("<redacted>"))}"
      )

    } yield authContext

  private[util] def buildMsiManager(cloudContext: AzureCloudContext): F[MsiManager] = {
    val azureProfile =
      new AzureProfile(cloudContext.tenantId.value, cloudContext.subscriptionId.value, AzureEnvironment.AZURE)
    val clientSecretCredential = new ClientSecretCredentialBuilder()
      .clientId(config.appRegistrationConfig.clientId.value)
      .clientSecret(config.appRegistrationConfig.clientSecret.value)
      .tenantId(config.appRegistrationConfig.managedAppTenantId.value)
      .build
    F.delay(MsiManager.authenticate(clientSecretCredential, azureProfile))
  }

  private[util] def buildComputeManager(cloudContext: AzureCloudContext): F[ComputeManager] = {
    val azureProfile =
      new AzureProfile(cloudContext.tenantId.value, cloudContext.subscriptionId.value, AzureEnvironment.AZURE)
    val clientSecretCredential = new ClientSecretCredentialBuilder()
      .clientId(config.appRegistrationConfig.clientId.value)
      .clientSecret(config.appRegistrationConfig.clientSecret.value)
      .tenantId(config.appRegistrationConfig.managedAppTenantId.value)
      .build
    F.delay(ComputeManager.authenticate(clientSecretCredential, azureProfile))
  }

  private def getCommonFields(name: String,
                              description: String,
                              app: App
  ): bio.terra.workspace.model.ControlledResourceCommonFields = {
    val commonFieldsBase = new bio.terra.workspace.model.ControlledResourceCommonFields()
      .resourceId(UUID.randomUUID())
      .name(name)
      .description(description)
      .managedBy(bio.terra.workspace.model.ManagedBy.APPLICATION)
      .cloningInstructions(CloningInstructionsEnum.NOTHING)
    app.samResourceId.accessScope match {
      case Some(AppAccessScope.WorkspaceShared) =>
        commonFieldsBase.accessScope(bio.terra.workspace.model.AccessScope.SHARED_ACCESS)
      case _ =>
        commonFieldsBase
          .accessScope(bio.terra.workspace.model.AccessScope.PRIVATE_ACCESS)
          .privateResourceUser(
            new bio.terra.workspace.model.PrivateResourceUser()
              .userName(app.auditInfo.creator.value)
              .privateResourceIamRole(bio.terra.workspace.model.ControlledResourceIamRole.WRITER)
          )
    }
  }

  private[util] def maybeCreateWsmIdentityAndDatabase(app: App,
                                                      workspaceId: WorkspaceId,
                                                      landingZoneResources: LandingZoneResources,
                                                      namespace: KubernetesNamespace
  )(implicit
    ev: Ask[F, AppContext]
  ): F[(Option[ServiceAccountName], Option[String])] = {
    val databaseConfigEnabled = app.appType match {
      case AppType.Wds => config.wdsAppConfig.databaseEnabled
      case _           => false
    }
    val landingZoneSupportsDatabase = landingZoneResources.postgresName.isDefined
    if (databaseConfigEnabled && landingZoneSupportsDatabase) {
      for {
        ctx <- ev.ask
        _ <- logger.info(ctx.loggingCtx)(
          s"Creating WSM identity for app ${app.appName.value} in cloud workspace ${workspaceId.value}"
        )

        // Build WSM client
        auth <- samDao.getLeoAuthToken
        token <- auth.credentials match {
          case org.http4s.Credentials.Token(_, token) => F.pure(token)
          case _ => F.raiseError(new RuntimeException("Could not obtain Leo auth token"))
        }
        wsmApi = wsmClientProvider.getControlledAzureResourceApi(token)

        // Build create managed identity request.
        // Use the k8s namespace for the name. Note dashes aren't allowed.
        identityName = s"id${namespace.name.value.split('-').head}"
        identityCommonFields = getCommonFields(identityName, s"Identity for Leo app ${app.appName.value}", app)
        createIdentityParams = new AzureManagedIdentityCreationParameters().name(
          identityName
        )
        createIdentityRequest = new CreateControlledAzureManagedIdentityRequestBody()
          .common(identityCommonFields)
          .azureManagedIdentity(createIdentityParams)

        _ <- logger.info(ctx.loggingCtx)(s"WSM create identity request: ${createIdentityRequest}")

        _ <- appControlledResourceQuery
          .insert(
            app.id.id,
            WsmControlledResourceId(createIdentityRequest.getCommon.getResourceId),
            WsmResourceType.AzureManagedIdentity,
            AppControlledResourceStatus.Created
          )
          .transaction

        // Execute WSM call
        createIdentityResponse <- F.delay(wsmApi.createAzureManagedIdentity(createIdentityRequest, workspaceId.value))

        _ <- logger.info(ctx.loggingCtx)(s"WSM create identity response: ${createIdentityResponse}")

        // Save record in APP_CONTROLLED_RESOURCE table
        _ <- appControlledResourceQuery
          .updateStatus(WsmControlledResourceId(createIdentityResponse.getResourceId),
                        AppControlledResourceStatus.Created
          )
          .transaction

        dbName <- createDatabaseInWsm(app,
                                      workspaceId,
                                      namespace,
                                      "wds",
                                      wsmApi,
                                      Option(createIdentityResponse.getResourceId)
        )
      } yield (Some(ServiceAccountName(identityName)), Some(dbName))
    } else F.pure((None, None))
  }

  private[util] def maybeCreateCromwellDatabases(app: App,
                                                 workspaceId: WorkspaceId,
                                                 landingZoneResources: LandingZoneResources,
                                                 namespace: KubernetesNamespace
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Option[CromwellDatabaseNames]] = {
    val databaseConfigEnabled = app.appType match {
      case AppType.Cromwell => config.coaAppConfig.databaseEnabled
      case _                => false
    }
    val landingZoneSupportsDatabase = landingZoneResources.postgresName.isDefined
    if (databaseConfigEnabled && landingZoneSupportsDatabase) {
      for {
        // Build WSM client
        auth <- samDao.getLeoAuthToken
        token <- auth.credentials match {
          case org.http4s.Credentials.Token(_, token) => F.pure(token)
          case _ => F.raiseError(new RuntimeException("Could not obtain Leo auth token"))
        }
        wsmApi = wsmClientProvider.getControlledAzureResourceApi(token)

        cromwellDb <- createDatabaseInWsm(app, workspaceId, namespace, "cromwell", wsmApi, None)
        cbasDb <- createDatabaseInWsm(app, workspaceId, namespace, "cbas", wsmApi, None)
        tesDb <- createDatabaseInWsm(app, workspaceId, namespace, "tes", wsmApi, None)
      } yield Some(CromwellDatabaseNames(cromwellDb, cbasDb, tesDb))
    } else F.pure(None)
  }

  private[util] def maybeCreateWorkflowsAppDatabases(app: App,
                                                     workspaceId: WorkspaceId,
                                                     landingZoneResources: LandingZoneResources,
                                                     namespace: KubernetesNamespace
  )(implicit
    ev: Ask[F, AppContext]
  ): F[(Option[ServiceAccountName], Option[_])] =
    if (app.appType == AppType.WorkflowsApp) {
      for {
        ctx <- ev.ask
        _ <- logger.info(ctx.loggingCtx)(
          s"Creating WSM identity for app ${app.appName.value} in cloud workspace ${workspaceId.value}"
        )

        // Build WSM client
        auth <- samDao.getLeoAuthToken
        token <- auth.credentials match {
          case org.http4s.Credentials.Token(_, token) => F.pure(token)
          case _ => F.raiseError(new RuntimeException("Could not obtain Leo auth token"))
        }
        wsmApi = wsmClientProvider.getControlledAzureResourceApi(token)

        // Build create managed identity request.
        // Use the k8s namespace for the name. Note dashes aren't allowed.
        identityName = s"id${namespace.name.value.split('-').head}"
        identityCommonFields = getCommonFields(identityName, s"Identity for Leo app ${app.appName.value}", app)
        createIdentityParams = new AzureManagedIdentityCreationParameters().name(
          identityName
        )
        createIdentityRequest = new CreateControlledAzureManagedIdentityRequestBody()
          .common(identityCommonFields)
          .azureManagedIdentity(createIdentityParams)

        _ <- logger.info(ctx.loggingCtx)(s"WSM create identity request: ${createIdentityRequest}")

        _ <- appControlledResourceQuery
          .insert(
            app.id.id,
            WsmControlledResourceId(createIdentityRequest.getCommon.getResourceId),
            WsmResourceType.AzureManagedIdentity,
            AppControlledResourceStatus.Created
          )
          .transaction

        // Execute WSM call
        createIdentityResponse <- F.delay(wsmApi.createAzureManagedIdentity(createIdentityRequest, workspaceId.value))

        _ <- logger.info(ctx.loggingCtx)(s"WSM create identity response: ${createIdentityResponse}")

        // Save record in APP_CONTROLLED_RESOURCE table
        _ <- appControlledResourceQuery
          .updateStatus(WsmControlledResourceId(createIdentityResponse.getResourceId),
                        AppControlledResourceStatus.Created
          )
          .transaction

        databaseNames = "foo" // TODO: WM-2159 create actual databases for workflows app
      } yield (Some(ServiceAccountName(identityName)), Some(databaseNames))
    } else F.pure((None, None))

  private[util] def createDatabaseInWsm(app: App,
                                        workspaceId: WorkspaceId,
                                        namespace: KubernetesNamespace,
                                        databaseNamePrefix: String,
                                        wsmApi: ControlledAzureResourceApi,
                                        owner: Option[UUID]
  )(implicit
    ev: Ask[F, AppContext]
  ): F[String] = {
    // Build create DB request
    // Use the k8s namespace for the name. Note dashes aren't allowed.
    val dbName = s"${databaseNamePrefix}_${namespace.name.value.split('-').head}"
    val databaseCommonFields =
      getCommonFields(dbName, s"$databaseNamePrefix database for Leo app ${app.appName.value}", app)
    val createDatabaseParams = new AzureDatabaseCreationParameters()
      .name(dbName)
      .k8sNamespace(app.appResources.namespace.name.value)
    owner.foreach(createDatabaseParams.owner)
    val createDatabaseJobControl = new JobControl().id(dbName)
    val createDatabaseRequest = new CreateControlledAzureDatabaseRequestBody()
      .common(databaseCommonFields)
      .azureDatabase(createDatabaseParams)
      .jobControl(createDatabaseJobControl)

    for {
      ctx <- ev.ask
      _ <- logger.info(ctx.loggingCtx)(
        s"Creating $databaseNamePrefix database for app ${app.appName.value} in cloud workspace ${workspaceId.value}"
      )
      _ <- logger.info(ctx.loggingCtx)(s"WSM create database request: ${createDatabaseRequest}")

      _ <- appControlledResourceQuery
        .insert(
          app.id.id,
          WsmControlledResourceId(createDatabaseRequest.getCommon.getResourceId),
          WsmResourceType.AzureDatabase,
          AppControlledResourceStatus.Creating
        )
        .transaction

      // Execute WSM call
      createDatabaseResponse <- F.delay(wsmApi.createAzureDatabase(createDatabaseRequest, workspaceId.value))

      _ <- logger.info(ctx.loggingCtx)(s"WSM create database response: ${createDatabaseResponse}")

      // Poll for DB creation
      // We don't actually care about the JobReport - just that it succeeded.
      op = F.delay(wsmApi.getCreateAzureDatabaseResult(workspaceId.value, dbName))
      result <- streamFUntilDone(
        op,
        config.appMonitorConfig.createApp.maxAttempts,
        config.appMonitorConfig.createApp.interval
      ).interruptAfter(config.appMonitorConfig.createApp.interruptAfter).compile.lastOrError

      _ <- logger.info(ctx.loggingCtx)(s"WSM create database job result: ${result}")

      _ <-
        if (result.getJobReport.getStatus != JobReport.StatusEnum.SUCCEEDED) {
          F.raiseError(
            AppCreationException(
              s"WSM database creation failed for app ${app.appName.value}. WSM response: ${result}",
              Some(ctx.traceId)
            )
          )
        } else F.unit

      // Save record in APP_CONTROLLED_RESOURCE table
      _ <- appControlledResourceQuery
        .updateStatus(
          WsmControlledResourceId(result.getAzureDatabase.getMetadata.getResourceId),
          AppControlledResourceStatus.Created
        )
        .transaction
    } yield dbName
  }

  private[util] def deleteAppWsmResources(app: App, workspaceId: WorkspaceId)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      // Build WSM client
      auth <- samDao.getLeoAuthToken
      token <- auth.credentials match {
        case org.http4s.Credentials.Token(_, token) => F.pure(token)
        case _ => F.raiseError(new RuntimeException("Could not obtain Leo auth token"))
      }
      wsmApi = wsmClientProvider.getControlledAzureResourceApi(token)

      wsmResources <- appControlledResourceQuery
        .getAllForAppByStatus(app.id.id, AppControlledResourceStatus.Created, AppControlledResourceStatus.Creating)
        .transaction

      _ <- wsmResources.traverse { wsmResource =>
        deleteWsmResource(workspaceId, app, wsmApi, wsmResource) >>
          appControlledResourceQuery.delete(wsmResource.resourceId).transaction
      }
    } yield ()

  private def deleteWsmResource(workspaceId: WorkspaceId,
                                app: App,
                                wsmApi: ControlledAzureResourceApi,
                                wsmResource: AppControlledResourceRecord
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] = {
    val delete = for {
      ctx <- ev.ask
      _ <- logger.info(ctx.loggingCtx)(
        s"Deleting WSM resource ${wsmResource.resourceId.value} for app ${app.appName.value} in workspace ${workspaceId.value}"
      )
      _ <- wsmResource.resourceType match {
        case WsmResourceType.AzureManagedIdentity =>
          F.delay(wsmApi.deleteAzureManagedIdentity(workspaceId.value, wsmResource.resourceId.value))
        case WsmResourceType.AzureDatabase =>
          F.delay(wsmApi.deleteAzureDatabase(workspaceId.value, wsmResource.resourceId.value))
        case _ =>
          // only managed identities and databases are supported for now because those are the only that exist now.
          F.raiseError(new RuntimeException(s"Unexpected WSM resource type ${wsmResource.resourceType}"))
      }
    } yield ()

    delete.handleErrorWith {
      case e: ApiException if e.getCode == StatusCodes.NotFound.intValue =>
        // If the resource doesn't exist, that's fine. We're deleting it anyway.
        F.unit
      case e => F.raiseError(e)
    }
  }

  private def updateListener(authContext: AuthContext,
                             app: App,
                             landingZoneResources: LandingZoneResources,
                             workspaceId: WorkspaceId,
                             hcName: RelayHybridConnectionName,
                             primaryKey: PrimaryKey,
                             relayDomain: String,
                             listenerChartConfig: ListenerChartConfig
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    // Update the Relay Listener if the app tracks it as a service.
    // We're not tracking the listener version in the DB so we can't really pick and choose which versions to update.
    // We started tracking it as a service when we switched the chart over to terra-helmfile.
    if (app.appResources.services.exists(s => s.config.name == listenerChartConfig.service.config.name)) {
      val values = BuildHelmChartValues.buildListenerChartOverrideValuesString(
        app.release,
        app.samResourceId,
        landingZoneResources.relayNamespace,
        hcName,
        primaryKey,
        app.appType,
        workspaceId,
        app.appName,
        refererConfig.validHosts + relayDomain,
        config.samConfig,
        config.listenerImage,
        config.leoUrlBase
      )
      for {
        ctx <- ev.ask
        _ <- logger.info(ctx.loggingCtx)(
          s"Listener values for app ${app.appName.value} are ${values.asString}"
        )
        _ <- helmClient
          .upgradeChart(
            getListenerReleaseName(app.release),
            config.listenerChartConfig.chartName,
            config.listenerChartConfig.chartVersion,
            values
          )
          .run(authContext)
      } yield ()
    } else {
      ev.ask.flatMap(ctx => logger.warn(ctx.loggingCtx)(s"Not updating relay listener for app ${app.appName.value}"))
    }
}

final case class AKSInterpreterConfig(
  coaAppConfig: CoaAppConfig,
  workflowsAppConfig: WorkflowsAppConfig,
  cromwellRunnerAppConfig: CromwellRunnerAppConfig,
  wdsAppConfig: WdsAppConfig,
  hailBatchAppConfig: HailBatchAppConfig,
  aadPodIdentityConfig: AadPodIdentityConfig,
  appRegistrationConfig: AzureAppRegistrationConfig,
  samConfig: SamConfig,
  appMonitorConfig: AppMonitorConfig,
  wsmConfig: HttpWsmDaoConfig,
  drsConfig: DrsConfig,
  leoUrlBase: URL,
  listenerImage: String,
  tdr: TdrConfig,
  listenerChartConfig: ListenerChartConfig
)

final case class CromwellDatabaseNames(cromwell: String, cbas: String, tes: String)
