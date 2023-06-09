package org.broadinstitute.dsde.workbench
package leonardo
package util

import cats.Show
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import com.azure.core.management.AzureEnvironment
import com.azure.core.management.profile.AzureProfile
import com.azure.identity.ClientSecretCredentialBuilder
import com.azure.resourcemanager.compute.ComputeManager
import com.azure.resourcemanager.compute.models.{ResourceIdentityType, VirtualMachineIdentityUserAssignedIdentities, VirtualMachineScaleSetIdentity, VirtualMachineScaleSetUpdate}
import com.azure.resourcemanager.msi.MsiManager
import com.azure.resourcemanager.msi.models.Identity
import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.V1NamespaceList
import io.kubernetes.client.util.Config
import org.broadinstitute.dsde.workbench.DoneCheckableSyntax._
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.{KubernetesNamespace, PodStatus}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates.whenStatusCode
import org.broadinstitute.dsde.workbench.google2.{autoClosableResourceF, recoverF, streamFUntilDone, streamUntilDoneOrTimeout, tracedRetryF}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.AppSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.config.CoaService.{Cbas, CbasUI, Cromwell}
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppNotFoundException
import org.broadinstitute.dsde.workbench.model.{IP, TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util2.withLogging
import org.broadinstitute.dsp.{Release, _}
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials, Uri}
import org.typelevel.log4cats.StructuredLogger

import java.io.ByteArrayInputStream
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
                           hailBatchDao: HailBatchDAO[F]
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

  private def getTerraAppSetupChartReleaseName(appReleaseName: Release): Release =
    Release(s"${appReleaseName.asString}-setup-rls")

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

      ksaName <- F.fromOption(
        app.appResources.kubernetesServiceAccountName,
        AppCreationException(
          s"Kubernetes Service Account not found in DB for app ${app.appName.value}",
          Some(ctx.traceId)
        )
      )

      _ <- logger.info(ctx.loggingCtx)(
        s"Begin app creation for app ${params.appName.value} in cloud context ${params.cloudContext.asString}"
      )

      // Authenticate helm client
      authContext <- getHelmAuthContext(params.landingZoneResources.clusterName, params.cloudContext, namespaceName)

      // Deploy aad-pod-identity chart
      // This only needs to be done once per cluster, but multiple helm installs have no effect.
      // See https://broadworkbench.atlassian.net/browse/IA-3804 for tracking migration to AKS Workload Identity.
      _ <- helmClient
        .installChart(
          config.aadPodIdentityConfig.release,
          config.aadPodIdentityConfig.chartName,
          config.aadPodIdentityConfig.chartVersion,
          config.aadPodIdentityConfig.values,
          true
        )
        .run(authContext.copy(namespace = config.aadPodIdentityConfig.namespace))

      // Create relay hybrid connection pool
      hcName = RelayHybridConnectionName(s"${params.appName.value}-${params.workspaceId.value}")
      relayPrimaryKey <- azureRelayService.createRelayHybridConnection(params.landingZoneResources.relayNamespace,
        hcName,
        params.cloudContext
      )
      relayDomain = s"${params.landingZoneResources.relayNamespace.value}.servicebus.windows.net"
      relayEndpoint = s"https://${relayDomain}/"
      relayPath = Uri.unsafeFromString(relayEndpoint) / hcName.value

      _ <- helmClient
        .installChart(
          getTerraAppSetupChartReleaseName(app.release),
          config.terraAppSetupChartConfig.chartName,
          config.terraAppSetupChartConfig.chartVersion,
          buildSetupChartOverrideValues(
            app.release,
            app.samResourceId,
            ksaName,
            params.landingZoneResources.relayNamespace,
            hcName,
            relayPrimaryKey,
            app.appType,
            params.workspaceId,
            app.appName
          ),
          true
        )
        .run(authContext)

      // get the pet userToken
      tokenOpt <- samDao.getCachedArbitraryPetAccessToken(app.auditInfo.creator)
      userToken <- F.fromOption(
        tokenOpt,
        AppCreationException(s"Pet not found for user ${app.auditInfo.creator}", Some(ctx.traceId))
      )

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

            // Assign the pet managed identity to the VM scale set backing the cluster node pool
            _ <- assignVmScaleSet(params.landingZoneResources.clusterName, params.cloudContext, petMi)
          } yield Some(petMi)
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
                  userToken // TODO: Remove once permanent solution utilizing the multi-user sam app identity has been implemented
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
                  userToken // TODO: Remove once permanent solution utilizing the multi-user sam app identity has been implemented
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

  override def deleteApp(params: DeleteAKSAppParams)(implicit ev: Ask[F, AppContext]): F[Unit] = {
    val DeleteAKSAppParams(appName, workspaceId, landingZoneResources, cloudContext, keepHistory) = params
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
      namespaceName = app.appResources.namespace.name
      kubernetesNamespace = KubernetesNamespace(namespaceName)

      clusterName = landingZoneResources.clusterName // NOT the same as dbCluster.clusterName

      // Delete hybrid connection for this app
      // for backwards compatibility, name used to be just the appName
      name = app.customEnvironmentVariables.getOrElse("RELAY_HYBRID_CONNECTION_NAME", app.appName.value)

      _ <- azureRelayService
        .deleteRelayHybridConnection(
          landingZoneResources.relayNamespace,
          RelayHybridConnectionName(name),
          cloudContext
        )

      // Authenticate helm client
      authContext <- getHelmAuthContext(landingZoneResources.clusterName, cloudContext, namespaceName)

      // Uninstall the app chart and setup chart
      _ <- helmClient.uninstall(app.release, keepHistory).run(authContext)
      _ <- helmClient
        .uninstall(
          getTerraAppSetupChartReleaseName(app.release),
          keepHistory
        )
        .run(authContext)

      client <- buildCoreV1Client(cloudContext, landingZoneResources.clusterName)

      // Poll until all pods in the app namespace are deleted
      _ <- streamUntilDoneOrTimeout(
        listPodStatus(client, KubernetesNamespace(app.appResources.namespace.name)),
        config.appMonitorConfig.deleteApp.maxAttempts,
        config.appMonitorConfig.deleteApp.interval,
        "helm deletion timed out"
      )

      // Delete the namespace only after the helm uninstall completes.
      _ <- deleteNamespace(client, kubernetesNamespace)

      // Poll until the namespace is actually deleted
      // Mapping to inverse because booleanDoneCheckable defines `Done` when it becomes `true`
      fa = namespaceExists(client, kubernetesNamespace).map(exists => !exists)
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
        s"Delete app operation has finished for app ${app.appName.value} in cluster ${clusterName}"
      )

      _ <- appQuery.updateStatus(app.id, AppStatus.Deleted).transaction

      _ <- logger.info(s"Done deleting app $appName in workspace $workspaceId")
    } yield ()
  }

  private[util] def pollAppCreation(userEmail: WorkbenchEmail, relayBaseUri: Uri, appType: AppType)(implicit
                                                                                                    ev: Ask[F, AppContext]
  ): F[Boolean] =
    for {
      ctx <- ev.ask
      tokenOpt <- samDao.getCachedArbitraryPetAccessToken(userEmail)
      token <- F.fromOption(tokenOpt, AppCreationException(s"Pet not found for user ${userEmail}", Some(ctx.traceId)))
      authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, token))

      op = appType match {
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
        case AppType.Wds =>
          wdsDao.getStatus(relayBaseUri, authHeader, appType).handleError(_ => false)
        case AppType.HailBatch =>
          hailBatchDao.getStatus(relayBaseUri, authHeader).handleError(_ => false)
        case _ => F.raiseError[Boolean](AppCreationException(s"App type ${appType} not supported on Azure"))
      }

      appOk <- streamFUntilDone(
        op,
        maxAttempts = config.appMonitorConfig.createApp.maxAttempts,
        delay = config.appMonitorConfig.createApp.interval
      ).interruptAfter(config.appMonitorConfig.createApp.interruptAfter).compile.lastOrError
    } yield appOk.isDone

  private[util] def buildSetupChartOverrideValues(release: Release,
                                                  samResourceId: AppSamResourceId,
                                                  ksaName: ServiceAccountName,
                                                  relayNamespace: RelayNamespace,
                                                  relayHcName: RelayHybridConnectionName,
                                                  relayPrimaryKey: PrimaryKey,
                                                  appType: AppType,
                                                  workspaceId: WorkspaceId,
                                                  appName: AppName
                                                 ): Values = {
    val relayTargetHost = appType match {
      case AppType.Cromwell => s"http://coa-${release.asString}-reverse-proxy-service:8000/"
      case AppType.Wds => s"http://wds-${release.asString}-wds-svc:8080"
      case AppType.HailBatch => "http://batch:8080"
      case AppType.Galaxy | AppType.Custom | AppType.RStudio =>
        F.raiseError(AppCreationException(s"App type $appType not supported on Azure"))
    }

    // Hail batch serves requests on /{appName}/batch and uses relative redirects,
    // so requires that we don't strip the entity path. For other app types we do
    // strip the entity path.
    val removeEntityPathFromHttpUrl = appType != AppType.HailBatch

    Values(
      List(
        raw"cloud=azure",
        // KSA configs
        raw"serviceAccount.name=${ksaName.value}",

        // relay configs
        raw"relaylistener.connectionString=Endpoint=sb://${relayNamespace.value}.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=${relayPrimaryKey.value};EntityPath=${relayHcName.value}",
        raw"relaylistener.connectionName=${relayHcName.value}",
        raw"relaylistener.endpoint=https://${relayNamespace.value}.servicebus.windows.net",
        raw"relaylistener.targetHost=$relayTargetHost",
        raw"relaylistener.samUrl=${config.samConfig.server}",
        raw"relaylistener.samResourceId=${samResourceId.resourceId}",
        raw"relaylistener.samResourceType=${samResourceId.resourceType.asString}",
        raw"relaylistener.samAction=connect",
        raw"relaylistener.workspaceId=${workspaceId.value.toString}",
        raw"relaylistener.runtimeName=${appName.value}",
        raw"relaylistener.image=${config.listenerImage}",
        raw"""relaylistener.removeEntityPathFromHttpUrl="${removeEntityPathFromHttpUrl.toString}"""",

        // general configs
        raw"fullnameOverride=setup-${release.asString}"
      ).mkString(",")
    )
  }

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
                                                     userAccessToken: String
                                                    ): Values =
    Values(
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
        raw"identity.name=${petManagedIdentity.map(_.name).getOrElse("none")}",
        raw"identity.resourceId=${petManagedIdentity.map(_.id).getOrElse("none")}",
        raw"identity.clientId=${petManagedIdentity.map(_.clientId).getOrElse("none")}",

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
      ).mkString(",")
    )

  private[util] def buildWdsChartOverrideValues(release: Release,
                                                appName: AppName,
                                                cloudContext: AzureCloudContext,
                                                workspaceId: WorkspaceId,
                                                landingZoneResources: LandingZoneResources,
                                                petManagedIdentity: Option[Identity],
                                                applicationInsightsConnectionString: String,
                                                sourceWorkspaceId: Option[WorkspaceId],
                                                userAccessToken: String
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
        raw"identity.name=${petManagedIdentity.map(_.name).getOrElse("none")}",
        raw"identity.resourceId=${petManagedIdentity.map(_.id).getOrElse("none")}",
        raw"identity.clientId=${petManagedIdentity.map(_.clientId).getOrElse("none")}",

        // Sam configs
        raw"sam.url=${config.samConfig.server}",

        // workspace manager
        raw"workspacemanager.url=${config.wsmConfig.uri.renderString}",

        // general configs
        raw"fullnameOverride=wds-${release.asString}",
        raw"instrumentationEnabled=${config.wdsAppConfig.instrumentationEnabled}",

        // import configs
        raw"import.dataRepoUrl=${config.tdr.url}",

        // provenance (app-cloning) configs
        raw"provenance.userAccessToken=${userAccessToken}"
      )
    val updatedLs = sourceWorkspaceId match {
      case Some(value) => valuesList ::: List(raw"provenance.sourceWorkspaceId=${value.value}")
      case None => valuesList
    }
    Values(updatedLs.mkString(","))
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
        s"Helm auth context for cluster ${clusterName.value} in cloud context ${cloudContext.asString}: ${
          authContext
            .copy(kubeToken = org.broadinstitute.dsp.KubeToken("<redacted>"))
        }"
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

  private[util] def buildCoreV1Client(cloudContext: AzureCloudContext, clusterName: AKSClusterName): F[CoreV1Api] = {
    // we do not want to have to specify this at resource (class) creation time, so we create one on each load here
    implicit val traceId = Ask.const[F, TraceId](TraceId(UUID.randomUUID()))
    for {
      credentials <- azureContainerService.getClusterCredentials(clusterName, cloudContext)
      client <- createClient(
        credentials
      )
      _ <- F.blocking(client.setApiKey(credentials.token.value))
    } yield new CoreV1Api(client)
  }

  private def deleteNamespace(client: CoreV1Api, namespace: KubernetesNamespace)(implicit
                                                                                 ev: Ask[F, TraceId]
  ): F[Unit] = {
    val delete = for {
      traceId <- ev.ask
      call =
        recoverF(
          F.blocking(
            client.deleteNamespace(
              namespace.name.value,
              "true",
              null,
              null,
              null,
              null,
              null
            )
          ).void
            .recoverWith {
              case e: com.google.gson.JsonSyntaxException
                if e.getMessage.contains("Expected a string but was BEGIN_OBJECT") =>
                logger.error(e)("Ignore response parsing error")
            } // see https://github.com/kubernetes-client/java/wiki/6.-Known-Issues#1-exception-on-deleting-resources-javalangillegalstateexception-expected-a-string-but-was-begin_object
          ,
          whenStatusCode(404)
        )
      _ <- withLogging(
        call,
        Some(traceId),
        s"io.kubernetes.client.openapi.apis.CoreV1Api.deleteNamespace(${namespace.name.value}, true, null, null, null, null, null)"
      )
    } yield ()

    // There is a known bug with the client lib json decoding.  `com.google.gson.JsonSyntaxException` occurs every time.
    // See https://github.com/kubernetes-client/java/issues/86
    delete.handleErrorWith {
      case _: com.google.gson.JsonSyntaxException =>
        F.unit
      case e: Throwable => F.raiseError(e)
    }
  }

  private def listPodStatus(client: CoreV1Api, namespace: KubernetesNamespace)(implicit
                                                                               ev: Ask[F, TraceId]
  ): F[List[PodStatus]] =
    for {
      traceId <- ev.ask
      call =
        F.blocking(
          client.listNamespacedPod(namespace.name.value, "true", null, null, null, null, null, null, null, null, null)
        )

      response <- withLogging(
        call,
        Some(traceId),
        s"io.kubernetes.client.apis.CoreV1Api.listNamespacedPod(${namespace.name.value}, true, null, null, null, null, null, null, null, null)"
      )

      listPodStatus: List[PodStatus] = response.getItems.asScala.toList.flatMap(v1Pod =>
        PodStatus.stringToPodStatus
          .get(v1Pod.getStatus.getPhase)
      )

    } yield listPodStatus

  // The underlying http client for ApiClient claims that it releases idle threads and that shutdown is not necessary
  // Here is a guide on how to proactively release resource if this proves to be problematic https://square.github.io/okhttp/4.x/okhttp/okhttp3/-ok-http-client/#shutdown-isnt-necessary
  private def createClient(credentials: AKSCredentials): F[ApiClient] = {
    val certResource = autoClosableResourceF(
      new ByteArrayInputStream(Base64.getDecoder.decode(credentials.certificate.value))
    )

    for {
      apiClient <- certResource.use { certStream =>
        F.delay(
          Config
            .fromToken(
              credentials.server.value,
              credentials.token.value
            )
            .setSslCaCert(certStream)
        )
      }
    } yield apiClient // appending here a .setDebugging(true) prints out useful API request/response info for development
  }

  private def namespaceExists(client: CoreV1Api, namespace: KubernetesNamespace)(implicit
                                                                                 ev: Ask[F, TraceId]
  ): F[Boolean] =
    for {
      traceId <- ev.ask
      call =
        recoverF(
          F.blocking(
            client.listNamespace("true", false, null, null, null, null, null, null, null, false)
          ),
          whenStatusCode(409)
        )
      v1NamespaceList <- withLogging(
        call,
        Some(traceId),
        s"io.kubernetes.client.apis.CoreV1Api.listNamespace()",
        Show.show[Option[V1NamespaceList]](
          _.fold("No namespace found")(x => x.getItems.asScala.toList.map(_.getMetadata.getName).mkString(","))
        )
      )
    } yield v1NamespaceList
      .map(ls => ls.getItems.asScala.toList)
      .getOrElse(List.empty)
      .exists(x => x.getMetadata.getName == namespace.name.value)

}

final case class AKSInterpreterConfig(
                                       terraAppSetupChartConfig: TerraAppSetupChartConfig,
                                       coaAppConfig: CoaAppConfig,
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
                                       tdr: TdrConfig
                                     )
