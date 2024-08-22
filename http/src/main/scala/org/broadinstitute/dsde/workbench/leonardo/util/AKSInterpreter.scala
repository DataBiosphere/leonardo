package org.broadinstitute.dsde.workbench
package leonardo
package util

import akka.http.scaladsl.model.StatusCodes
import bio.terra.workspace.api.{ControlledAzureResourceApi, ResourceApi, WorkspaceApi}
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model._
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import com.azure.core.management.exception.ManagementException
import fs2.io.file.Files
import org.broadinstitute.dsde.workbench.DoneCheckableSyntax._
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.{KubernetesNamespace, PodStatus}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, streamUntilDoneOrTimeout, RegionName}
import org.broadinstitute.dsde.workbench.leonardo.app.Database.{ControlledDatabase, ReferenceDatabase}
import org.broadinstitute.dsde.workbench.leonardo.app.{AppInstall, BuildHelmOverrideValuesParams}
import org.broadinstitute.dsde.workbench.leonardo.auth.SamAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.config.Config.refererConfig
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppNotFoundException
import org.broadinstitute.dsde.workbench.model.{IP, WorkbenchEmail}
import org.broadinstitute.dsp._
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials, Uri}
import org.typelevel.log4cats.StructuredLogger

import java.net.URL
import java.util.{Base64, UUID}
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._
import scala.util.Try

class AKSInterpreter[F[_]](config: AKSInterpreterConfig,
                           helmClient: HelmAlgebra[F],
                           azureContainerService: AzureContainerService[F],
                           azureRelayService: AzureRelayService[F],
                           samDao: SamDAO[F],
                           wsmDao: WsmDao[F],
                           kubeAlg: KubernetesAlgebra[F],
                           wsmClientProvider: WsmApiClientProvider[F],
                           legacyWsmDao: WsmDao[F],
                           authProvider: SamAuthProvider[F]
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

  implicit private def deleteWsmResourceDoneCheckable: DoneCheckable[DeleteControlledAzureResourceResult] =
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
      namespacePrefix = app.appResources.namespace.value

      _ <- logger.info(ctx.loggingCtx)(
        s"Begin app creation for app ${params.appName.value} in cloud context ${params.cloudContext.asString}"
      )

      // Query the Landing Zone service for the landing zone resources
      leoAuth <- samDao.getLeoAuthToken
      landingZoneResources <- childSpan("getLandingZoneResources").use { implicit ev =>
        legacyWsmDao.getLandingZoneResources(params.billingProfileId, leoAuth)
      }

      // Get the optional storage container for the workspace
      tokenOpt <- samDao.getCachedArbitraryPetAccessToken(app.auditInfo.creator)
      storageContainerOpt <- childSpan("getWorkspaceStorageContainer").use { implicit ev =>
        tokenOpt.flatTraverse { token =>
          wsmDao.getWorkspaceStorageContainer(
            params.workspaceId,
            org.http4s.headers.Authorization(org.http4s.Credentials.Token(AuthScheme.Bearer, token))
          )
        }
      }

      wsmResourceApi <- buildWsmResourceApiClient

      // Create or fetch WSM managed identity (if shared app)
      // The managed identity name is either:
      // shared apps --> the WSM identity --> shared apps
      // private apps --> pet managed identity (stored in the googleServiceAccount' column in the APP table)
      // for private apps, set the managedIdentity to None so it can be supplied below
      wsmManagedIdentityOpt <- app.samResourceId.resourceType match {
        case SamResourceType.SharedApp =>
          // if a managed identity has already been created in the workspace use that otherwise create a new managed identity
          createOrFetchWsmManagedIdentity(app, wsmResourceApi, params.workspaceId, namespacePrefix)
        case _ => F.pure(None)
      }
      managedIdentityName = ManagedIdentityName(
        wsmManagedIdentityOpt
          .map(_.managedIdentityName)
          .getOrElse(app.googleServiceAccount.value.split('/').last)
      )

      // create any missing AppControlledResources
      _ <- childSpan("createMissingAppControlledResources").use { implicit ev =>
        createMissingAppControlledResources(
          app,
          app.appType,
          params.workspaceId,
          landingZoneResources,
          wsmResourceApi
        )
      }

      // Create or fetch WSM databases
      wsmDatabases <- childSpan("createWsmDatabaseResources").use { implicit ev =>
        createOrFetchWsmDatabaseResources(
          app,
          app.appType,
          params.workspaceId,
          namespacePrefix,
          wsmManagedIdentityOpt.map(_.wsmResourceName),
          landingZoneResources,
          wsmResourceApi
        )
      }

      // get ReferenceDatabases from WSM
      referenceDatabaseNames = app.appType.databases.collect { case ReferenceDatabase(name) => name }.toSet
      referenceDatabases <-
        if (referenceDatabaseNames.nonEmpty) {
          retrieveWsmDatabases(wsmResourceApi, referenceDatabaseNames, params.workspaceId.value)
        } else F.pure(List.empty)

      // Create or fetch WSM kubernetes namespace
      namespace <- childSpan("createWsmNamespaceResource").use { implicit ev =>
        createOrFetchWsmNamespace(app,
                                  wsmDatabases,
                                  wsmResourceApi,
                                  namespacePrefix,
                                  params.workspaceId,
                                  wsmManagedIdentityOpt
        )
      }

      // Create relay hybrid connection pool
      // TODO: make into a WSM resource
      hcName = RelayHybridConnectionName(s"${params.appName.value}-${params.workspaceId.value}")
      relayPrimaryKey <- childSpan("createRelayHybridConnection").use { implicit ev =>
        azureRelayService.createRelayHybridConnection(landingZoneResources.relayNamespace, hcName, params.cloudContext)
      }
      relayDomain = s"${landingZoneResources.relayNamespace.value}.servicebus.windows.net"
      relayEndpoint = s"https://${relayDomain}/"
      relayPath = Uri.unsafeFromString(relayEndpoint) / hcName.value

      // Authenticate helm client
      authContext <- getHelmAuthContext(landingZoneResources.aksCluster.asClusterName,
                                        params.cloudContext,
                                        namespace.name
      )

      // Build listener helm values
      values = BuildHelmChartValues.buildListenerChartOverrideValuesString(
        app.release,
        app.samResourceId,
        landingZoneResources.relayNamespace,
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
      _ <- childSpan("helmInstallRelayListener").use { _ =>
        helmClient
          .installChart(
            getListenerReleaseName(app.release),
            config.listenerChartConfig.chartName,
            config.listenerChartConfig.chartVersion,
            values,
            false
          )
          .run(authContext)
      }

      // Build app helm values
      helmOverrideValueParams = BuildHelmOverrideValuesParams(
        app,
        params.workspaceId,
        params.cloudContext,
        params.billingProfileId,
        landingZoneResources,
        storageContainerOpt,
        relayPath,
        namespace.serviceAccountName,
        managedIdentityName,
        wsmDatabases ++ referenceDatabases,
        config
      )
      values <- app.appType.buildHelmOverrideValues(helmOverrideValueParams)

      _ <- logger.info(ctx.loggingCtx)(
        s"App values for app ${params.appName.value} are ${values.asString}"
      )
      _ <- logger.info(ctx.loggingCtx)(
        s"App release = ${app.release}, chart name = ${app.chart.name}, app.chart.version = ${app.chart.version}"
      )
      // Install app chart
      _ <- childSpan("helmInstallApp").use { _ =>
        helmClient
          .installChart(
            app.release,
            app.chart.name,
            app.chart.version,
            values,
            createNamespace = false
          )
          .run(authContext)
      }

      appOk <- childSpan("pollAppCreation").use { implicit ev =>
        pollAppCreation(app.auditInfo.creator, relayPath, app.appType)
      }
      _ <- F.raiseWhen(!appOk)(
        AppCreationException(
          s"App ${params.appName.value} failed to start in cluster ${landingZoneResources.aksCluster.name} in cloud context ${params.cloudContext.asString}",
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
              landingZoneResources.vnetName,
              landingZoneResources.aksSubnetName,
              IpRange("[unset]")
            )
          )
        )
        .transaction

      _ <- kubernetesClusterQuery
        .updateRegion(dbApp.cluster.id, RegionName(landingZoneResources.region.name))
        .transaction

      // If we've got here, update the App status to Running.
      _ <- appQuery.updateStatus(params.appId, AppStatus.Running).transaction

      _ <- logger.info(ctx.loggingCtx)(
        s"Finished app creation for app ${params.appName.value} in cluster ${landingZoneResources.aksCluster.name} in cloud context ${params.cloudContext.asString}"
      )
    } yield ()

  override def updateAndPollApp(params: UpdateAKSAppParams)(implicit ev: Ask[F, AppContext]): F[Unit] = {
    for {
      ctx <- ev.ask

      // Build WSM client
      wsmControlledResourceApi <- buildWsmControlledResourceApiClient
      wsmResourceApi <- buildWsmResourceApiClient

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
      leoAuth <- samDao.getLeoAuthToken
      token <- authProvider.getLeoAuthToken

      workspaceDescOpt <- childSpan("getWorkspace").use { implicit ev =>
        wsmClientProvider.getWorkspace(token, workspaceId)
      }
      workspaceDesc <- F.fromOption(
        workspaceDescOpt,
        AppUpdateException(s"Workspace ${workspaceId.value.toString} not found in WSM", Some(ctx.traceId))
      )
      // Query the Landing Zone service for the landing zone resources
      billingProfileId = BillingProfileId(workspaceDesc.spendProfile)
      landingZoneResources <- childSpan("getLandingZoneResources").use { implicit ev =>
        legacyWsmDao.getLandingZoneResources(billingProfileId, leoAuth)
      }

      // Get the optional storage container for the workspace
      storageContainerOpt <- childSpan("getWorkspaceStorageContainer").use { implicit ev =>
        wsmDao.getWorkspaceStorageContainer(
          workspaceId,
          leoAuth
        )
      }

      // Call WSM to get the managed identity for the app.
      // This is optional because a WSM identity is only created for shared apps.
      wsmIdentities <- appControlledResourceQuery
        .getAllForAppByType(app.id.id, WsmResourceType.AzureManagedIdentity)
        .transaction
      wsmIdentityOpt <- wsmIdentities.headOption.traverse { wsmIdentity =>
        F.blocking(wsmControlledResourceApi.getAzureManagedIdentity(workspaceId.value, wsmIdentity.resourceId.value))
      }

      // create any missing AppControlledResources
      _ <- createMissingAppControlledResources(
        app,
        app.appType,
        workspaceId,
        landingZoneResources,
        wsmResourceApi
      )

      // get list of APP_CONTROLLED_RESOURCES
      controlledDatabases <- appControlledResourceQuery
        .getAllForAppByType(app.id.id, WsmResourceType.AzureDatabase)
        .transaction
      // Call WSM to get more info about each database (by resourceId) that exists in APP_CONTROLLED_RESOURCE
      wsmDatabases <- controlledDatabases.traverse { controlledDatabase =>
        F.blocking(wsmControlledResourceApi.getAzureDatabase(workspaceId.value, controlledDatabase.resourceId.value))
          .map(db =>
            WsmControlledDatabaseResource(db.getMetadata.getName,
                                          db.getAttributes.getDatabaseName,
                                          db.getMetadata.getResourceId
            )
          )
      }

      // call WSM resource API to get list of ReferenceDatabases
      referenceDatabaseNames = app.appType.databases.collect { case ReferenceDatabase(name) => name }.toSet
      referenceDatabases <-
        if (referenceDatabaseNames.nonEmpty) {
          retrieveWsmDatabases(wsmResourceApi, referenceDatabaseNames, workspaceId.value)
        } else F.pure(List.empty)

      // Call WSM to get the Kubernetes namespace (required)
      wsmNamespaces <- appControlledResourceQuery
        .getAllForAppByType(app.id.id, WsmResourceType.AzureKubernetesNamespace)
        .transaction
      wsmNamespaceOpt <- wsmNamespaces.headOption.traverse { wsmNamespace =>
        F.blocking(
          wsmControlledResourceApi.getAzureKubernetesNamespace(workspaceId.value, wsmNamespace.resourceId.value)
        )
      }
      wsmNamespace <- F.fromOption(wsmNamespaceOpt,
                                   AppUpdateException("WSM namespace required for app", Some(ctx.traceId))
      )

      // The k8s namespace name and service account name are in the WSM response
      namespaceName = NamespaceName(wsmNamespace.getAttributes.getKubernetesNamespace)
      ksaName = ServiceAccountName(wsmNamespace.getAttributes.getKubernetesServiceAccount)

      // The managed identity name is either the WSM identity (for shared apps) or the
      // pet managed identity (for private apps). The latter is confusingly stored in the
      // 'googleServiceAccount' column in the APP table.
      managedIdentityName = ManagedIdentityName(
        wsmIdentityOpt
          .map(_.getAttributes.getManagedIdentityName)
          .getOrElse(app.googleServiceAccount.value.split('/').last)
      )

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
      authContext <- getHelmAuthContext(landingZoneResources.aksCluster.asClusterName,
                                        params.cloudContext,
                                        namespaceName
      )

      // Check if the app is alive before attempting to update. We transition the app to error status if this fails
      isAppAlive <- childSpan("isAppAlive").use { implicit ev =>
        isAppAlive(app.auditInfo.creator, relayPath, app.appType)
      }
      _ <-
        if (isAppAlive)
          F.unit
        else
          F.raiseError[Unit](
            AppUpdatePollingException(
              s"App ${params.appName.value} was not alive and therefore failed to update in cluster ${landingZoneResources.aksCluster.name} in cloud context ${params.cloudContext.asString}",
              Some(ctx.traceId)
            )
          )

      // The app is blocked in updating now (as in, if leo restarts between here and the app transitioning back to `Running`, it is unusable
      // See: https://broadworkbench.atlassian.net/browse/IA-4867
      _ <- appQuery.updateStatus(app.id, AppStatus.Updating).transaction

      // Update the relay listener deployment
      _ <- childSpan("helmUpdateListener").use { implicit ev =>
        updateListener(authContext,
                       app,
                       landingZoneResources,
                       workspaceId,
                       hcName,
                       relayPrimaryKey,
                       relayDomain,
                       config.listenerChartConfig
        )
      }

      // Build app helm values
      helmOverrideValueParams = BuildHelmOverrideValuesParams(
        app,
        workspaceId,
        params.cloudContext,
        billingProfileId,
        landingZoneResources,
        storageContainerOpt,
        relayPath,
        ksaName,
        managedIdentityName,
        wsmDatabases ++ referenceDatabases,
        config
      )
      values <- app.appType.buildHelmOverrideValues(helmOverrideValueParams)

      // Upgrade app chart version and explicitly pass the values
      _ <- childSpan("helmUpdateApp").use { _ =>
        helmClient
          .upgradeChart(
            app.release,
            app.chart.name,
            params.appChartVersion,
            values
          )
          .run(authContext)
      }

      // Poll until all pods in the app namespace are running
      // TODO: we should be able to test this method and the subsequent `AppUpdatePollingException` more easily when we start to implement rollbacks
      appOk <- childSpan("pollAppUpdate").use { implicit ev =>
        pollAppUpdate(app.auditInfo.creator, relayPath, app.appType)
      }
      _ <-
        if (appOk)
          F.unit
        else
          F.raiseError[Unit](
            AppUpdatePollingException(
              s"App ${params.appName.value} failed to update in cluster ${landingZoneResources.aksCluster.name} in cloud context ${params.cloudContext.asString}",
              Some(ctx.traceId)
            )
          )

      _ <- logger.info(
        s"Update app operation has finished for app ${app.appName.value} in cluster ${landingZoneResources.aksCluster.name}"
      )

      // Update app chart version in the DB
      _ <- appQuery.updateChart(app.id, Chart(app.chart.name, params.appChartVersion)).transaction
      // Put app status back to running
      _ <- appQuery.updateStatus(app.id, AppStatus.Running).transaction

      _ <- logger.info(s"Done updating app ${params.appName} in workspace ${params.workspaceId}")
    } yield ()
  }

  override def deleteApp(params: DeleteAKSAppParams)(implicit ev: Ask[F, AppContext]): F[Unit] = {
    val DeleteAKSAppParams(appName, workspaceId, cloudContext, billingProfileId) = params
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

      // Query the Landing Zone service for the landing zone resources
      leoAuth <- samDao.getLeoAuthToken
      landingZoneResources <- childSpan("getLandingZoneResources").use { implicit ev =>
        legacyWsmDao.getLandingZoneResources(billingProfileId, leoAuth)
      }

      wsmResourceApi <- buildWsmResourceApiClient

      // create any missing AppControlledResources
      _ <- createMissingAppControlledResources(
        app,
        app.appType,
        workspaceId,
        landingZoneResources,
        wsmResourceApi
      )

      // WSM deletion order matters here. Delete WSM database resources first.
      wsmDatabases <- appControlledResourceQuery
        .getAllForAppByType(app.id.id, WsmResourceType.AzureDatabase)
        .transaction
      _ <- childSpan("deleteWsmDatabases").use { implicit ev =>
        wsmDatabases.traverse { database =>
          deleteWsmResource(workspaceId, app, database)
        }
      }

      // Then delete namespace resources
      wsmNamespaces <- appControlledResourceQuery
        .getAllForAppByType(app.id.id, WsmResourceType.AzureKubernetesNamespace)
        .transaction
      deletedNamespace <- childSpan("deleteWsmNamespace").use { implicit ev =>
        wsmNamespaces
          .traverse { namespace =>
            deleteWsmResource(workspaceId, app, namespace)
          }
          .map(_.nonEmpty)
      }

      // Then delete identity resources
      wsmIdentities <- appControlledResourceQuery
        .getAllForAppByType(app.id.id, WsmResourceType.AzureManagedIdentity)
        .transaction
      _ <- childSpan("deleteWsmIdentity").use { implicit ev =>
        wsmIdentities.traverse { identity =>
          deleteWsmResource(workspaceId, app, identity)
        }
      }

      // If this app did not have a WSM-tracked kubernetes namespace, delete it explicitly
      _ <-
        if (deletedNamespace) F.unit
        else {
          for {
            client <- kubeAlg.createAzureClient(cloudContext, landingZoneResources.aksCluster.asClusterName)

            kubernetesNamespace = KubernetesNamespace(app.appResources.namespace)

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
          } yield ()
        }

      // Delete hybrid connection for this app
      // for backwards compatibility, name used to be just the appName
      // TODO: make relay hybrid connection a WSM resource
      name = app.customEnvironmentVariables.getOrElse("RELAY_HYBRID_CONNECTION_NAME", app.appName.value)

      _ <- childSpan("deleteRelayHybridConnection").use { implicit ev =>
        azureRelayService
          .deleteRelayHybridConnection(
            landingZoneResources.relayNamespace,
            RelayHybridConnectionName(name),
            cloudContext
          )
          .handleErrorWith {
            case e: ManagementException if e.getResponse.getStatusCode == StatusCodes.NotFound.intValue =>
              logger.info(ctx.loggingCtx)(s"${name} does not exist to delete in ${cloudContext}")
            case e => F.raiseError[Unit](e)
          }
      }

      // Delete the Sam resource
      userEmail = app.auditInfo.creator
      tokenOpt <- samDao.getCachedArbitraryPetAccessToken(userEmail)
      _ <- childSpan("deleteSamResource").use { implicit ev =>
        tokenOpt match {
          case Some(token) =>
            samDao.deleteResourceInternal(dbApp.app.samResourceId,
                                          Authorization(Credentials.Token(AuthScheme.Bearer, token))
            )
          case None =>
            logger.warn(
              s"Could not find pet service account for user ${userEmail} in Sam. Skipping resource deletion in Sam."
            )
        }
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

  private[util] def isAppAlive(userEmail: WorkbenchEmail, relayBaseUri: Uri, appInstall: AppInstall[F])(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] =
    for {
      _ <- ev.ask
      op = pollApp(userEmail, relayBaseUri, appInstall)
      appOk <- streamFUntilDone(
        op,
        maxAttempts = config.appMonitorConfig.appLiveness.maxAttempts,
        delay = config.appMonitorConfig.appLiveness.interval
      ).compile.lastOrError
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
                                 app: App,
                                 cloningInstructions: CloningInstructionsEnum
  ): bio.terra.workspace.model.ControlledResourceCommonFields = {
    val commonFieldsBase = new bio.terra.workspace.model.ControlledResourceCommonFields()
      .resourceId(UUID.randomUUID())
      .name(name)
      .description(description)
      .managedBy(bio.terra.workspace.model.ManagedBy.APPLICATION)
      .cloningInstructions(cloningInstructions)
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

  private def generateWsmNameForIdentity(appType: AppType): String = s"id${appType.toString.toLowerCase}"

  private[util] def createAzureManagedIdentity(app: App, namespacePrefix: String, workspaceId: WorkspaceId)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[WsmManagedAzureIdentity]] =
    childSpan("createWsmIdentityResource").use { implicit ev =>
      createWsmIdentityResource(app, namespacePrefix, workspaceId)
        .map(i =>
          WsmManagedAzureIdentity(i.getAzureManagedIdentity.getMetadata.getName,
                                  i.getAzureManagedIdentity.getAttributes.getManagedIdentityName
          )
        )
        .map(_.some)
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
      wsmApi <- buildWsmControlledResourceApiClient

      // Name of the managed identity. Must be unique per landing zone.
      identityName = s"id${namespacePrefix.split('-').headOption.getOrElse(namespacePrefix)}"

      // Name of the WSM resource. Must be unique per workspace.
      // For shared apps, name it by the appType so it's semantically meaningful.
      // There can only be at most 1 shared app type per workspace anyway.
      // For private apps, use managed identity name to ensure uniqueness.
      wsmResourceName = app.samResourceId.resourceType match {
        case SamResourceType.SharedApp => generateWsmNameForIdentity(app.appType)
        case _                         => identityName
      }

      cloningInstructions =
        if (app.appType == AppType.WorkflowsApp) CloningInstructionsEnum.RESOURCE else CloningInstructionsEnum.NOTHING

      identityCommonFields = getWsmCommonFields(wsmResourceName,
                                                s"Identity for Leo app ${app.appName.value}",
                                                app,
                                                cloningInstructions
      )
      createIdentityParams = new AzureManagedIdentityCreationParameters().name(
        identityName
      )
      createIdentityRequest = new CreateControlledAzureManagedIdentityRequestBody()
        .common(identityCommonFields)
        .azureManagedIdentity(createIdentityParams)

      _ <- logger.info(ctx.loggingCtx)(s"WSM create identity request: ${createIdentityRequest}")

      // Execute WSM call
      createIdentityResponse <- F.blocking(wsmApi.createAzureManagedIdentity(createIdentityRequest, workspaceId.value))

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

  private[util] def createOrFetchWsmDatabaseResources(app: App,
                                                      appInstall: AppInstall[F],
                                                      workspaceId: WorkspaceId,
                                                      namespacePrefix: String,
                                                      owner: Option[String],
                                                      landingZoneResources: LandingZoneResources,
                                                      wsmResourceApi: ResourceApi
  )(implicit ev: Ask[F, AppContext]): F[List[WsmControlledDatabaseResource]] =
    for {
      ctx <- ev.ask
      _ <- F.raiseWhen(landingZoneResources.postgresServer.isEmpty)(
        AppCreationException("Postgres server not found in landing zone", Some(ctx.traceId))
      )
      wsmApi <- buildWsmControlledResourceApiClient

      // get a list of database types required for this app
      controlledDbsForApp = appInstall.databases.collect { case d @ ControlledDatabase(_, _, _) => d }
      // retrieve databases that might already be created in workspace
      existingControlledDbsInWorkspace <- retrieveWsmDatabases(wsmResourceApi,
                                                               controlledDbsForApp.map(_.prefix).toSet,
                                                               workspaceId.value
      )
      wsmControlledDBResources <- controlledDbsForApp
        .traverse { controlledDbForApp =>
          // if a database already exists (because of workspace cloning or Leo restarting mid-app creation) use that otherwise create a new one
          if (
            existingControlledDbsInWorkspace
              .exists(existingDb => controlledDbForApp.prefix == existingDb.wsmDatabaseName)
          ) {
            logger.info(
              s"Database found in WSM for app ${app.appName}, using previously created database: $existingControlledDbsInWorkspace"
            ) >>
              F.pure(
                existingControlledDbsInWorkspace
                  .find(clonedDatabase => controlledDbForApp.prefix == clonedDatabase.wsmDatabaseName)
                  .get
              )
          } else {
            logger.info(s"Creating databases for app ${app.appName}") >>
              createWsmDatabaseResource(app, workspaceId, controlledDbForApp, namespacePrefix, owner, wsmApi).map {
                db =>
                  WsmControlledDatabaseResource(db.getAzureDatabase.getMetadata.getName,
                                                db.getAzureDatabase.getAttributes.getDatabaseName
                  )
              }
          }
        }
    } yield wsmControlledDBResources

  private[util] def createMissingAppControlledResources(app: App,
                                                        appInstall: AppInstall[F],
                                                        workspaceId: WorkspaceId,
                                                        landingZoneResources: LandingZoneResources,
                                                        wsmResourceApi: ResourceApi
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- F.raiseWhen(landingZoneResources.postgresServer.isEmpty)(
        AppCreationException("Postgres server not found in landing zone", Some(ctx.traceId))
      )
      wsmApi <- buildWsmControlledResourceApiClient

      // get a list of database types required for this app
      controlledDbsRequiredForApp = appInstall.databases.collect { case d @ ControlledDatabase(_, _, _) => d }

      // retrieve set of databases WSM has created for this workspace (name, wsmDatabaseName, resource_id)
      existingWsmDbsInWorkspace: List[WsmControlledDatabaseResource] <- retrieveWsmDatabases(
        wsmResourceApi,
        controlledDbsRequiredForApp.map(_.prefix).toSet,
        workspaceId.value
      )

      // Get list of APP_CONTROLLED_RESOURCE for this app (appId, resource_id, state)
      appControlledResources: List[AppControlledResourceRecord] <- appControlledResourceQuery
        .getAllForAppByType(app.id.id, WsmResourceType.AzureDatabase)
        .transaction

      // Find WSM databases in workspace that do not exist in the appControlledResources list based on resourceId
      wsmDbsNotinAppResources = existingWsmDbsInWorkspace.filterNot { wsmDb =>
        appControlledResources.exists(appRes => appRes.resourceId.value.equals(wsmDb.controlledResourceId))
      }

      // create a APP_CONTROLLED_RESOURCE for any wsm database that does not have one
      _ <- wsmDbsNotinAppResources.traverse { db =>
        for {
          res <- appControlledResourceQuery
            .insert(
              app.id.id,
              WsmControlledResourceId(db.controlledResourceId),
              WsmResourceType.AzureDatabase,
              AppControlledResourceStatus.Created
            )
            .transaction
        } yield db
      }

    } yield ()

  private[util] def createWsmDatabaseResource(app: App,
                                              workspaceId: WorkspaceId,
                                              database: ControlledDatabase,
                                              namespacePrefix: String,
                                              owner: Option[String],
                                              wsmApi: ControlledAzureResourceApi
  )(implicit
    ev: Ask[F, AppContext]
  ): F[CreatedControlledAzureDatabaseResult] = {
    // Build create DB request

    // Name of the database. Must be unique per landing zone.
    val dbName = s"${database.prefix}_${namespacePrefix.split('-').headOption.getOrElse(namespacePrefix)}"

    // Name of the WSM resource. Must be unique per workspace.
    // For shared apps, name it by the databasePrefix so it's semantically meaningful.
    // There can only be at most 1 shared app type per workspace anyway.
    // For private apps, use the database name to ensure uniqueness.
    val wsmResourceName = app.samResourceId.resourceType match {
      case SamResourceType.SharedApp => database.prefix
      case _                         => dbName
    }

    val databaseCommonFields =
      getWsmCommonFields(wsmResourceName,
                         s"${database.prefix} database for Leo app ${app.appName.value}",
                         app,
                         database.cloningInstructions
      )
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
      createDatabaseResponse <- F.blocking(wsmApi.createAzureDatabase(createDatabaseRequest, workspaceId.value))

      _ <- logger.info(ctx.loggingCtx)(s"WSM create database response: ${createDatabaseResponse}")

      // Poll for DB creation
      // We don't actually care about the JobReport - just that it succeeded.
      op = F.blocking(wsmApi.getCreateAzureDatabaseResult(workspaceId.value, dbName))
      result <- streamFUntilDone(
        op,
        config.appMonitorConfig.createApp.maxAttempts,
        config.appMonitorConfig.createApp.interval
      ).interruptAfter(config.appMonitorConfig.createApp.interruptAfter).compile.lastOrError

      _ <- logger.info(ctx.loggingCtx)(s"WSM create database job result: ${result}")

      _ <- F.raiseWhen(result.getJobReport.getStatus != JobReport.StatusEnum.SUCCEEDED)(
        AppCreationException(
          s"WSM database creation failed for app ${app.appName.value}. WSM response: ${result}",
          Some(ctx.traceId)
        )
      )

      // Save record in APP_CONTROLLED_RESOURCE table
      _ <- appControlledResourceQuery
        .updateStatus(
          WsmControlledResourceId(result.getAzureDatabase.getMetadata.getResourceId),
          AppControlledResourceStatus.Created
        )
        .transaction
    } yield result
  }

  private[util] def retrieveWsmManagedIdentity(resourceApi: ResourceApi,
                                               appType: AppType,
                                               workspaceId: UUID
  ): F[Option[WsmManagedAzureIdentity]] = {
    val wsmResourceName = generateWsmNameForIdentity(appType)
    F.blocking(
      getWorkspaceResourceByName(workspaceId, wsmResourceName, resourceApi).map { identity =>
        WsmManagedAzureIdentity(
          wsmResourceName,
          identity.getResourceAttributes.getAzureManagedIdentity.getManagedIdentityName
        )
      }
    )
  }

  private[util] def retrieveWsmDatabases(resourceApi: ResourceApi,
                                         databaseNames: Set[String],
                                         workspaceId: UUID
  ): F[List[WsmControlledDatabaseResource]] =
    // TODO: this currently matches on the 'name' (actually type) of database so for example
    // it compares for a 'cbas' or 'cromwellmetadata' database. In the future, their maybe
    // multiple of those in a workspace so this approach will have to be re-considered
    // see https://broadworkbench.atlassian.net/browse/IA-4844
    F.blocking(
      databaseNames
        .flatMap(dbName =>
          getWorkspaceResourceByName(workspaceId, dbName, resourceApi).map { r =>
            WsmControlledDatabaseResource(r.getMetadata.getName,
                                          r.getResourceAttributes.getAzureDatabase.getDatabaseName,
                                          r.getMetadata.getResourceId
            )
          }
        )
        .toList
    )

  private[util] def createOrFetchWsmManagedIdentity(app: App,
                                                    resourceApi: ResourceApi,
                                                    workspaceId: WorkspaceId,
                                                    namespacePrefix: String
  )(implicit ev: Ask[F, AppContext]): F[Option[WsmManagedAzureIdentity]] =
    for {
      wsmManagedIdentityOpt <-
        retrieveWsmManagedIdentity(resourceApi, app.appType, workspaceId.value).flatMap {
          case Some(identity) =>
            logger.info(
              s"Managed ID found in WSM app ${app.appName}, using previously created identity: ${identity.managedIdentityName}"
            ) >>
              F.pure(Option(identity))
          case None =>
            createAzureManagedIdentity(app, namespacePrefix, workspaceId)
        }
    } yield wsmManagedIdentityOpt

  private[util] def createOrFetchWsmNamespace(app: App,
                                              wsmDatabases: List[WsmControlledDatabaseResource],
                                              resourceApi: ResourceApi,
                                              namespacePrefix: String,
                                              workspaceId: WorkspaceId,
                                              wsmManagedIdentityOpt: Option[WsmManagedAzureIdentity]
  )(implicit ev: Ask[F, AppContext]): F[WsmControlledKubernetesNamespaceResource] =
    for {
      wsmNamespace <- retrieveWsmNamespace(resourceApi, namespacePrefix, workspaceId.value)
      namespace <- wsmNamespace match {
        case Some(ns) =>
          logger.info(
            s"Namespace found in WSM for app ${app.appName}, using previously created namespace: ${ns.name}"
          )
          F.pure(ns)
        case None =>
          createWsmKubernetesNamespaceResource(
            app,
            workspaceId,
            namespacePrefix,
            wsmDatabases.map(_.wsmDatabaseName),
            wsmManagedIdentityOpt.map(_.wsmResourceName)
          )
      }
    } yield namespace

  private[util] def retrieveWsmNamespace(resourceApi: ResourceApi,
                                         namespacePrefix: String,
                                         workspaceId: UUID
  ): F[Option[WsmControlledKubernetesNamespaceResource]] = {
    // The full namespace name will be {namespacePrefix}-{workspaceId},
    // and the resource name is the same as the kubernetes namespace
    // The construction of this is done in ControlledAzureResourceApiController.createAzureKubernetesNamespace
    val namespaceName = s"$namespacePrefix-$workspaceId"
    F.blocking(
      getWorkspaceResourceByName(workspaceId, namespaceName, resourceApi).map { ns =>
        WsmControlledKubernetesNamespaceResource(
          NamespaceName(ns.getResourceAttributes.getAzureKubernetesNamespace.getKubernetesNamespace),
          WsmControlledResourceId(ns.getMetadata.getResourceId),
          ServiceAccountName(
            ns.getResourceAttributes.getAzureKubernetesNamespace.getKubernetesServiceAccount
          )
        )
      }
    )
  }

  private[util] def createWsmKubernetesNamespaceResource(app: App,
                                                         workspaceId: WorkspaceId,
                                                         namespacePrefix: String,
                                                         databases: List[String],
                                                         identity: Option[String]
  )(implicit ev: Ask[F, AppContext]): F[WsmControlledKubernetesNamespaceResource] =
    for {
      ctx <- ev.ask

      _ <- logger.info(ctx.loggingCtx)(
        s"Creating $namespacePrefix namespace for app ${app.appName.value} in cloud workspace ${workspaceId.value}"
      )

      // Build WSM client
      wsmApi <- buildWsmControlledResourceApiClient

      // Name of the WSM resource. Must be unique per workspace.
      // For shared apps, name it by the appType so it's semantically meaningful.
      // There can only be at most 1 shared app type per workspace anyway.
      // For private apps, use the namespacePrefix to ensure uniqueness.
      wsmResourceName = app.samResourceId.resourceType match {
        case SamResourceType.SharedApp => s"${app.appType.toString.toLowerCase}-ns"
        case _                         => namespacePrefix
      }

      // Build common fields
      namespaceCommonFields =
        getWsmCommonFields(wsmResourceName,
                           s"$namespacePrefix kubernetes namespace for Leo app ${app.appName.value}",
                           app,
                           CloningInstructionsEnum.NOTHING
        )

      // Build createNamespace fields
      appExternalDatabaseNames = app.appType.databases.collect { case ReferenceDatabase(name) => name }.toSet
      createNamespaceParams = new AzureKubernetesNamespaceCreationParameters()
        .namespacePrefix(namespacePrefix)
        .databases((databases ++ appExternalDatabaseNames).asJava)

      _ = identity.foreach(createNamespaceParams.setManagedIdentity)

      // Build request
      createNamespaceJobControl = new JobControl().id(
        namespacePrefix
      )
      createNamespaceRequest = new CreateControlledAzureKubernetesNamespaceRequestBody()
        .common(namespaceCommonFields)
        .azureKubernetesNamespace(createNamespaceParams)
        .jobControl(createNamespaceJobControl)

      _ <- logger.info(ctx.loggingCtx)(s"WSM create namespace request: $createNamespaceRequest")

      _ <- appControlledResourceQuery
        .insert(
          app.id.id,
          WsmControlledResourceId(createNamespaceRequest.getCommon.getResourceId),
          WsmResourceType.AzureKubernetesNamespace,
          AppControlledResourceStatus.Creating
        )
        .transaction

      // Execute WSM call
      createNamespaceResponse <- F.blocking(
        wsmApi.createAzureKubernetesNamespace(createNamespaceRequest, workspaceId.value)
      )

      _ <- logger.info(ctx.loggingCtx)(s"WSM create namespace response: ${createNamespaceResponse}")

      // Poll for namespace creation
      op = F.blocking(wsmApi.getCreateAzureKubernetesNamespaceResult(workspaceId.value, namespacePrefix))
      result <- streamFUntilDone(
        op,
        config.appMonitorConfig.createApp.maxAttempts,
        config.appMonitorConfig.createApp.interval
      ).interruptAfter(config.appMonitorConfig.createApp.interruptAfter).compile.lastOrError

      _ <- logger.info(ctx.loggingCtx)(s"WSM create namespace job result: ${result}")

      _ <- F.raiseWhen(result.getJobReport.getStatus != JobReport.StatusEnum.SUCCEEDED)(
        AppCreationException(
          s"WSM namespace creation failed for app ${app.appName.value}. WSM response: ${result}",
          Some(ctx.traceId)
        )
      )

      resourceId = WsmControlledResourceId(result.getAzureKubernetesNamespace.getMetadata.getResourceId)

      // Save record in APP_CONTROLLED_RESOURCE table
      _ <- appControlledResourceQuery
        .updateStatus(
          resourceId,
          AppControlledResourceStatus.Created
        )
        .transaction
      namespaceAttributes = result.getAzureKubernetesNamespace.getAttributes
    } yield WsmControlledKubernetesNamespaceResource(
      NamespaceName(namespaceAttributes.getKubernetesNamespace),
      resourceId,
      ServiceAccountName(namespaceAttributes.getKubernetesServiceAccount)
    )

  private[util] def deleteAndPollWsmNamespaceResource(workspaceId: WorkspaceId,
                                                      app: App,
                                                      wsmResource: AppControlledResourceRecord,
                                                      jobId: UUID,
                                                      deleteResourceRequest: DeleteControlledAzureResourceRequest,
                                                      wsmApi: ControlledAzureResourceApi
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask

      _ <- logger.info(ctx.loggingCtx)(s"WSM delete namespace request: ${deleteResourceRequest}")

      // Execute WSM call
      result <- F.blocking(
        wsmApi.deleteAzureKubernetesNamespace(deleteResourceRequest, workspaceId.value, wsmResource.resourceId.value)
      )

      _ <- logger.info(ctx.loggingCtx)(s"WSM delete namespace response: ${result}")

      // Update record in APP_CONTROLLED_RESOURCE table
      _ <- appControlledResourceQuery
        .updateStatus(
          wsmResource.resourceId,
          AppControlledResourceStatus.Deleting
        )
        .transaction

      // Poll for namespace deletion
      op = F.blocking(
        wsmApi.getDeleteAzureKubernetesNamespaceResult(workspaceId.value, jobId.toString)
      )

      result <- streamFUntilDone(
        op,
        config.appMonitorConfig.deleteApp.maxAttempts,
        config.appMonitorConfig.deleteApp.interval
      ).compile.lastOrError

      _ <- logger.info(ctx.loggingCtx)(s"WSM delete namespace job result: $result")

      _ <- F.raiseWhen(result.getJobReport.getStatus != JobReport.StatusEnum.SUCCEEDED)(
        AppDeletionException(
          s"WSM namespace deletion failed for app ${app.appName.value}. WSM response: $result"
        )
      )

      // record in APP_CONTROLLED_RESOURCE table is set to deleted by caller
    } yield ()

  private[util] def deleteAndPollWsmDatabaseResource(workspaceId: WorkspaceId,
                                                     app: App,
                                                     wsmResource: AppControlledResourceRecord,
                                                     jobId: UUID,
                                                     deleteResourceRequest: DeleteControlledAzureResourceRequest,
                                                     wsmApi: ControlledAzureResourceApi
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask

      _ <- logger.info(ctx.loggingCtx)(s"WSM delete database request: ${deleteResourceRequest}")

      // Execute WSM call
      result <- F.blocking(
        wsmApi.deleteAzureDatabaseAsync(deleteResourceRequest, workspaceId.value, wsmResource.resourceId.value)
      )

      _ <- logger.info(ctx.loggingCtx)(s"WSM delete database response: ${result}")

      // Update record in APP_CONTROLLED_RESOURCE table
      _ <- appControlledResourceQuery
        .updateStatus(
          wsmResource.resourceId,
          AppControlledResourceStatus.Deleting
        )
        .transaction

      // Poll for database deletion
      op = F.blocking(
        wsmApi.getDeleteAzureDatabaseResult(workspaceId.value, jobId.toString)
      )

      result <- streamFUntilDone(
        op,
        config.appMonitorConfig.deleteApp.maxAttempts,
        config.appMonitorConfig.deleteApp.interval
      ).compile.lastOrError

      _ <- logger.info(ctx.loggingCtx)(s"WSM delete database job result: $result")

      _ <- F.raiseWhen(result.getJobReport.getStatus != JobReport.StatusEnum.SUCCEEDED)(
        AppDeletionException(
          s"WSM database deletion failed for app ${app.appName.value}. WSM response: $result"
        )
      )

      // record in APP_CONTROLLED_RESOURCE table is set to deleted by caller
    } yield ()

  private[util] def deleteWsmResource(workspaceId: WorkspaceId, app: App, wsmResource: AppControlledResourceRecord)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit] = {
    val delete = for {
      ctx <- ev.ask
      wsmApi <- buildWsmControlledResourceApiClient
      _ <- logger.info(ctx.loggingCtx)(
        s"Deleting WSM resource ${wsmResource.resourceId.value} for app ${app.appName.value} in workspace ${workspaceId.value}"
      )

      jobId = UUID.randomUUID()
      deleteResourceRequest = new DeleteControlledAzureResourceRequest().jobControl(
        new JobControl().id(jobId.toString)
      )
      _ <- wsmResource.resourceType match {
        case WsmResourceType.AzureManagedIdentity =>
          F.blocking(wsmApi.deleteAzureManagedIdentity(workspaceId.value, wsmResource.resourceId.value))
        case WsmResourceType.AzureDatabase =>
          deleteAndPollWsmDatabaseResource(workspaceId, app, wsmResource, jobId, deleteResourceRequest, wsmApi)
        case WsmResourceType.AzureKubernetesNamespace =>
          deleteAndPollWsmNamespaceResource(workspaceId, app, wsmResource, jobId, deleteResourceRequest, wsmApi)
        case _ =>
          F.raiseError(AppDeletionException(s"Unexpected WSM resource type ${wsmResource.resourceType}"))
      }
      // Update record in APP_CONTROLLED_RESOURCE table
      _ <- appControlledResourceQuery.delete(wsmResource.resourceId).transaction
    } yield ()

    delete.handleErrorWith {
      case e: ApiException if e.getCode == StatusCodes.NotFound.intValue =>
        // If the resource doesn't exist, that's fine. We're deleting it anyway.
        for {
          _ <- logger.info(s"No-op for delete WSM app resource ${wsmResource.resourceId.value}")
          _ <- appControlledResourceQuery.delete(wsmResource.resourceId).transaction
        } yield ()
      case e => F.raiseError(e)
    }
  }

  // This should probably be moved to WsmDao, if HttpWsmDao switches to using the WSM api clients
  private def getWorkspaceResourceByName(
    workspaceId: UUID,
    resourceName: String,
    resourceApi: ResourceApi
  ): Option[ResourceDescription] =
    Try(resourceApi.getResourceByName(workspaceId, resourceName))
      .map(Option.apply)
      .handleError {
        case e: ApiException if e.getCode == StatusCodes.NotFound.intValue => None
      }
      .get

  private[util] def updateListener(authContext: AuthContext,
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

  private def buildWsmControlledResourceApiClient(implicit ev: Ask[F, AppContext]): F[ControlledAzureResourceApi] =
    for {
      token <- authProvider.getLeoAuthToken
      wsmApi <- wsmClientProvider.getControlledAzureResourceApi(token)
    } yield wsmApi

  private def buildWsmResourceApiClient(implicit ev: Ask[F, AppContext]): F[ResourceApi] =
    for {
      token <- authProvider.getLeoAuthToken
      wsmApi <- wsmClientProvider.getResourceApi(token)
    } yield wsmApi

  private def buildWsmWorkspaceApiClient(implicit ev: Ask[F, AppContext]): F[WorkspaceApi] =
    for {
      token <- authProvider.getLeoAuthToken
      wsmApi <- wsmClientProvider.getWorkspaceApi(token)
    } yield wsmApi
}

final case class AKSInterpreterConfig(
  samConfig: SamConfig,
  appMonitorConfig: AppMonitorConfig,
  wsmConfig: HttpWsmDaoConfig,
  leoUrlBase: URL,
  listenerImage: String,
  listenerChartConfig: ListenerChartConfig
)
