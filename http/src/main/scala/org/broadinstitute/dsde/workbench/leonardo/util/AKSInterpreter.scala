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
import fs2.io.file.Files
import org.broadinstitute.dsde.workbench.DoneCheckableSyntax._
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.{KubernetesNamespace, PodStatus}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, streamUntilDoneOrTimeout, tracedRetryF}
import org.broadinstitute.dsde.workbench.leonardo.app.AppInstall
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
  appTypeToAppInstall: AppType => AppInstall[F],
  executionContext: ExecutionContext,
  logger: StructuredLogger[F],
  dbRef: DbReference[F],
  F: Async[F],
  files: Files[F]
) extends AKSAlgebra[F] {
  implicit private def booleanDoneCheckable: DoneCheckable[Boolean] = identity[Boolean]

  implicit private def listDoneCheckable[A: DoneCheckable]: DoneCheckable[List[A]] = as => as.forall(_.isDone)

  private[util] def isPodDone(podStatus: PodStatus): Boolean =
    podStatus == PodStatus.Failed || podStatus == PodStatus.Succeeded

  implicit private def podDoneCheckable: DoneCheckable[List[PodStatus]] =
    (ps: List[PodStatus]) => ps.forall(isPodDone)

  implicit private def createDatabaseDoneCheckable: DoneCheckable[CreatedControlledAzureDatabaseResult] =
    _.getJobReport.getStatus != JobReport.StatusEnum.RUNNING

  implicit private def createKubernetesNamespaceDoneCheckable
    : DoneCheckable[CreatedControlledAzureKubernetesNamespaceResult] =
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
      namespacePrefix = app.appResources.namespace.name.value

      _ <- logger.info(ctx.loggingCtx)(
        s"Begin app creation for app ${params.appName.value} in cloud context ${params.cloudContext.asString}"
      )

      // Create WSM managed identity if shared app
      wsmManagedIdentityOpt <- app.samResourceId.resourceType match {
        case SamResourceType.SharedApp =>
          createWsmIdentityResource(app, namespacePrefix, params.workspaceId).map(_.some)
        case _ => F.pure(None)
      }

      // Create WSM databases
      wsmDatabases <- createWsmDatabaseResources(app,
                                                 app.appType,
                                                 params.workspaceId,
                                                 namespacePrefix,
                                                 wsmManagedIdentityOpt.map(_.getResourceId),
                                                 params.landingZoneResources
      )

      // Create WSM kubernetes namespace
      wsmNamespace <- createWsmKubernetesNamespaceResource(app,
                                                           params.workspaceId,
                                                           namespacePrefix,
                                                           wsmDatabases.map(_.getResourceId),
                                                           wsmManagedIdentityOpt.map(_.getResourceId)
      )

      // The k8s namespace name and service account name are in the WSM response
      namespaceName = NamespaceName(wsmNamespace.getAzureKubernetesNamespace.getMetadata.getName)
      ksaName = ServiceAccountName(wsmNamespace.getAzureKubernetesNamespace.getAttributes.getKubernetesServiceAccount)

      // Create relay hybrid connection pool
      // TODO: make into a WSM resource
      hcName = RelayHybridConnectionName(s"${params.appName.value}-${params.workspaceId.value}")
      relayPrimaryKey <- azureRelayService.createRelayHybridConnection(params.landingZoneResources.relayNamespace,
                                                                       hcName,
                                                                       params.cloudContext
      )
      relayDomain = s"${params.landingZoneResources.relayNamespace.value}.servicebus.windows.net"
      relayEndpoint = s"https://${relayDomain}/"
      relayPath = Uri.unsafeFromString(relayEndpoint) / hcName.value

      // Authenticate helm client
      authContext <- getHelmAuthContext(params.landingZoneResources.clusterName, params.cloudContext, namespaceName)

      // Build listener helm values
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

      // Install listener helm chart
      _ <- helmClient
        .installChart(
          getListenerReleaseName(app.release),
          config.listenerChartConfig.chartName,
          config.listenerChartConfig.chartVersion,
          values,
          false
        )
        .run(authContext)

      // Build app helm values
      values <- app.appType.helmValues(
        params,
        config,
        app,
        relayPath,
        ksaName,
        wsmDatabases.map(_.getAzureDatabase.getMetadata.getName)
      )

      // Install app chart
      _ <- helmClient
        .installChart(
          app.release,
          app.chart.name,
          app.chart.version,
          values,
          createNamespace = false
        )
        .run(authContext)

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

  // TODO fix update!!!!
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

  private[util] def pollApp(userEmail: WorkbenchEmail, relayBaseUri: Uri, appInstall: AppInstall[F])(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] = for {
    ctx <- ev.ask
    tokenOpt <- samDao.getCachedArbitraryPetAccessToken(userEmail)
    token <- F.fromOption(tokenOpt, AppCreationException(s"Pet not found for user ${userEmail}", Some(ctx.traceId)))
    authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, token))

    res <- appInstall.checkStatus(relayBaseUri, authHeader)
  } yield res

  private[util] def pollAppCreation(userEmail: WorkbenchEmail, relayBaseUri: Uri, appInstall: AppInstall[F])(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] =
    for {
      _ <- ev.ask
      op = pollApp(userEmail, relayBaseUri, appInstall)

      appOk <- streamFUntilDone(
        op,
        maxAttempts = config.appMonitorConfig.createApp.maxAttempts,
        delay = config.appMonitorConfig.createApp.interval
      ).interruptAfter(config.appMonitorConfig.createApp.interruptAfter).compile.lastOrError
    } yield appOk.isDone

  private[util] def pollAppUpdate(userEmail: WorkbenchEmail, relayBaseUri: Uri, appInstall: AppInstall[F])(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] =
    for {
      _ <- ev.ask
      op = pollApp(userEmail, relayBaseUri, appInstall)
      appOk <- streamFUntilDone(
        op,
        maxAttempts = config.appMonitorConfig.updateApp.maxAttempts,
        delay = config.appMonitorConfig.updateApp.interval
      ).interruptAfter(config.appMonitorConfig.updateApp.interruptAfter).compile.lastOrError
    } yield appOk.isDone

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

  private def getWsmCommonFields(name: String,
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

  private[util] def createWsmIdentityResource(app: App, namespacePrefix: String, workspaceId: WorkspaceId)(implicit
    ev: Ask[F, AppContext]
  ): F[CreatedControlledAzureManagedIdentity] =
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
      identityName = s"id${namespacePrefix.split('-').headOption.getOrElse(namespacePrefix)}"
      identityCommonFields = getWsmCommonFields(identityName, s"Identity for Leo app ${app.appName.value}", app)
      createIdentityParams = new AzureManagedIdentityCreationParameters().name(
        identityName
      )
      createIdentityRequest = new CreateControlledAzureManagedIdentityRequestBody()
        .common(identityCommonFields)
        .azureManagedIdentity(createIdentityParams)

      _ <- logger.info(ctx.loggingCtx)(s"WSM create identity request: ${createIdentityRequest}")

      // Execute WSM call
      createIdentityResponse <- F.delay(wsmApi.createAzureManagedIdentity(createIdentityRequest, workspaceId.value))

      _ <- logger.info(ctx.loggingCtx)(s"WSM create identity response: ${createIdentityResponse}")

      // Save record in APP_CONTROLLED_RESOURCE table
      _ <- appControlledResourceQuery
        .insert(
          app.id.id,
          WsmControlledResourceId(createIdentityResponse.getResourceId),
          WsmResourceType.AzureManagedIdentity,
          AppControlledResourceStatus.Created
        )
        .transaction

    } yield createIdentityResponse

  private[util] def createWsmDatabaseResources(app: App,
                                               appInstall: AppInstall[F],
                                               workspaceId: WorkspaceId,
                                               namespacePrefix: String,
                                               owner: Option[UUID],
                                               landingZoneResources: LandingZoneResources
  )(implicit ev: Ask[F, AppContext]): F[List[CreatedControlledAzureDatabaseResult]] =
    if (landingZoneResources.postgresServer.isDefined) {
      for {
        // Build WSM client
        auth <- samDao.getLeoAuthToken
        token <- auth.credentials match {
          case org.http4s.Credentials.Token(_, token) => F.pure(token)
          case _ => F.raiseError(new RuntimeException("Could not obtain Leo auth token"))
        }
        wsmApi = wsmClientProvider.getControlledAzureResourceApi(token)

        res <- appInstall.databases.traverse { database =>
          createWsmDatabaseResource(app, workspaceId, database, namespacePrefix, owner, wsmApi)
        }
      } yield Some(res)
    } else {
      F.raiseError(AppCreationException("Postgres server not found in landing zone"))
    }

  private[util] def createWsmDatabaseResource(app: App,
                                              workspaceId: WorkspaceId,
                                              database: AppInstall.Database,
                                              namespacePrefix: String,
                                              owner: Option[UUID],
                                              wsmApi: ControlledAzureResourceApi
  )(implicit
    ev: Ask[F, AppContext]
  ): F[CreatedControlledAzureDatabaseResult] = {
    // Build create DB request
    // Use the k8s namespace for the name. Note dashes aren't allowed.
    val dbName = s"${database.prefix}_${namespacePrefix.split('-').headOption.getOrElse(namespacePrefix)}"
    val databaseCommonFields =
      getWsmCommonFields(dbName, s"${database.prefix} database for Leo app ${app.appName.value}", app)
    val createDatabaseParams = new AzureDatabaseCreationParameters()
      .name(dbName)
      .allowAccessForAllWorkspaceUsers(database.allowAccessForAllWorkspaceUsers)
    owner.foreach(createDatabaseParams.setOwner)
    val createDatabaseJobControl = new JobControl().id(dbName)
    val createDatabaseRequest = new CreateControlledAzureDatabaseRequestBody()
      .common(databaseCommonFields)
      .azureDatabase(createDatabaseParams)
      .jobControl(createDatabaseJobControl)

    for {
      ctx <- ev.ask
      _ <- logger.info(ctx.loggingCtx)(
        s"Creating ${database.prefix} database for app ${app.appName.value} in cloud workspace ${workspaceId.value}"
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
    } yield result
  }

  private[util] def createWsmKubernetesNamespaceResource(app: App,
                                                         workspaceId: WorkspaceId,
                                                         namespacePrefix: String,
                                                         databases: List[UUID],
                                                         identity: Option[UUID]
  )(implicit ev: Ask[F, AppContext]): F[CreatedControlledAzureKubernetesNamespaceResult] = {
    // Build create namespace request
    val namespaceCommonFields =
      getWsmCommonFields(namespacePrefix,
                         s"$namespacePrefix kubernetes namespace for Leo app ${app.appName.value}",
                         app
      )
    val createNamespaceParams = new AzureKubernetesNamespaceCreationParameters()
      .namespacePrefix(namespacePrefix)
      .databases(databases.asJava)
    identity.foreach(createNamespaceParams.setManagedIdentity)
    val createNamespaceJobControl = new JobControl().id(namespacePrefix)
    val createNamespaceRequest = new CreateControlledAzureKubernetesNamespaceRequestBody()
      .common(namespaceCommonFields)
      .azureKubernetesNamespace(createNamespaceParams)
      .jobControl(createNamespaceJobControl)

    for {
      ctx <- ev.ask
      _ <- logger.info(ctx.loggingCtx)(
        s"Creating $namespacePrefix namespace for app ${app.appName.value} in cloud workspace ${workspaceId.value}"
      )

      // Build WSM client
      auth <- samDao.getLeoAuthToken
      token <- auth.credentials match {
        case org.http4s.Credentials.Token(_, token) => F.pure(token)
        case _ => F.raiseError(new RuntimeException("Could not obtain Leo auth token"))
      }
      wsmApi = wsmClientProvider.getControlledAzureResourceApi(token)

      _ <- logger.info(ctx.loggingCtx)(s"WSM create namespace request: ${createNamespaceRequest}")

      _ <- appControlledResourceQuery
        .insert(
          app.id.id,
          WsmControlledResourceId(createNamespaceRequest.getCommon.getResourceId),
          WsmResourceType.AzureKubernetesNamespace,
          AppControlledResourceStatus.Creating
        )
        .transaction

      // Execute WSM call
      createNamespaceResponse <- F.delay(
        wsmApi.createAzureKubernetesNamespace(createNamespaceRequest, workspaceId.value)
      )

      _ <- logger.info(ctx.loggingCtx)(s"WSM create namespace response: ${createNamespaceResponse}")

      // Poll for namepsace creation
      op = F.delay(wsmApi.getCreateAzureKubernetesNamespaceResult(workspaceId.value, namespacePrefix))
      result <- streamFUntilDone(
        op,
        config.appMonitorConfig.createApp.maxAttempts,
        config.appMonitorConfig.createApp.interval
      ).interruptAfter(config.appMonitorConfig.createApp.interruptAfter).compile.lastOrError

      _ <- logger.info(ctx.loggingCtx)(s"WSM create namespace job result: ${result}")

      _ <-
        if (result.getJobReport.getStatus != JobReport.StatusEnum.SUCCEEDED) {
          F.raiseError(
            AppCreationException(
              s"WSM namespace creation failed for app ${app.appName.value}. WSM response: ${result}",
              Some(ctx.traceId)
            )
          )
        } else F.unit

      // Save record in APP_CONTROLLED_RESOURCE table
      _ <- appControlledResourceQuery
        .updateStatus(
          WsmControlledResourceId(result.getAzureKubernetesNamespace.getMetadata.getResourceId),
          AppControlledResourceStatus.Created
        )
        .transaction
    } yield result
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
        case WsmResourceType.AzureKubernetesNamespace =>
          // TODO delete namespace is async; should we poll or fire-and-forget?
          val body = new DeleteControlledAzureResourceRequest().jobControl(
            new JobControl().id(s"delete-${wsmResource.resourceId.value.toString}")
          )
          F.delay(wsmApi.deleteAzureKubernetesNamespace(body, workspaceId.value, wsmResource.resourceId.value))
        case _ =>
          // only managed identities, databases, and namespaces are supported for apps.
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

sealed trait SharedDatabaseNames
final case class WorkflowsAppDatabaseNames(cbas: String, cromwellMetadata: String) extends SharedDatabaseNames
final case class WdsDatabaseNames(wds: String) extends SharedDatabaseNames
