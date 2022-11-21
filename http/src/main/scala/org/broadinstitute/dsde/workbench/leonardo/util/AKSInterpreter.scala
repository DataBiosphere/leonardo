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
import com.azure.resourcemanager.compute.models.{
  ResourceIdentityType,
  VirtualMachineIdentityUserAssignedIdentities,
  VirtualMachineScaleSetIdentity,
  VirtualMachineScaleSetUpdate
}
import com.azure.resourcemanager.msi.MsiManager
import com.azure.resourcemanager.msi.models.Identity
import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.V1NamespaceList
import io.kubernetes.client.util.Config
import org.broadinstitute.dsde.workbench.DoneCheckableSyntax._
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.{
  KubernetesApiServerIp,
  KubernetesNamespace,
  PodStatus
}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.NamespaceName
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates.whenStatusCode
import org.broadinstitute.dsde.workbench.google2.{
  autoClosableResourceF,
  recoverF,
  streamFUntilDone,
  streamUntilDoneOrTimeout,
  tracedRetryF
}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.AppSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.config.{AppMonitorConfig, CoaAppConfig, SamConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.{CbasDAO, CromwellDAO, SamDAO, WdsDAO}
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
import java.util.{Base64, UUID}
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

class AKSInterpreter[F[_]](config: AKSInterpreterConfig,
                           helmClient: HelmAlgebra[F],
                           azureContainerService: AzureContainerService[F],
                           azureRelayService: AzureRelayService[F],
                           samDao: SamDAO[F],
                           cromwellDao: CromwellDAO[F],
                           cbasDao: CbasDAO[F],
                           wdsDao: WdsDAO[F]
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
        .getActiveFullAppByName(CloudContext.Azure(params.cloudContext), params.appName)
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
      petEmail = app.googleServiceAccount

      _ <- logger.info(ctx.loggingCtx)(
        s"Begin app creation for app ${params.appName.value} in cloud context ${params.cloudContext.asString}"
      )

      landingZoneResources <- F.fromOption(
        params.landingZoneResourcesOpt,
        AppCreationException(
          s"Landing Zone Resources not found in app creation params for app ${app.appName.value}",
          Some(ctx.traceId)
        )
      )

      // Authenticate helm client
      authContext <- getHelmAuthContext(landingZoneResources.clusterName, params.cloudContext, namespaceName)

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

      // Deploy setup chart
      _ <- helmClient
        .installChart(
          getTerraAppSetupChartReleaseName(app.release),
          config.terraAppSetupChartConfig.chartName,
          config.terraAppSetupChartConfig.chartVersion,
          org.broadinstitute.dsp.Values(
            s"cloud=azure,serviceAccount.name=${ksaName.value}"
          ),
          true
        )
        .run(authContext)

      // Create relay hybrid connection pool
      hcName = RelayHybridConnectionName(params.appName.value)
      primaryKey <- azureRelayService.createRelayHybridConnection(landingZoneResources.relayNamespace,
                                                                  hcName,
                                                                  params.cloudContext
      )

      // Resolve pet managed identity in Azure
      msi <- buildMsiManager(params.cloudContext)
      petMi <- F.delay(
        msi.identities().getById(petEmail.value)
      )

      // Assign the pet managed identity to the VM scale set backing the cluster node pool
      _ <- assignVmScaleSet(landingZoneResources.clusterName, params.cloudContext, petMi)

      // Build values and install chart
      values = buildCromwellChartOverrideValues(app.release,
                                                params.cloudContext,
                                                app.samResourceId,
                                                landingZoneResources,
                                                hcName,
                                                primaryKey,
                                                petMi
      )
      _ <- helmClient
        .installChart(
          app.release,
          app.chart.name,
          app.chart.version,
          values,
          createNamespace = true
        )
        .run(authContext)

      // Poll app status
      relayEndpoint = s"https://${landingZoneResources.relayNamespace.value}.servicebus.windows.net/"
      appOk <- pollCromwellAppCreation(app.auditInfo.creator, Uri.unsafeFromString(relayEndpoint) / app.appName.value)
      _ <-
        if (appOk)
          F.unit
        else
          F.raiseError[Unit](
            AppCreationException(
              s"App ${params.appName.value} failed to start in cluster ${landingZoneResources.clusterName.value} in cloud context ${params.cloudContext.asString}",
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

      // If we've got here, update the App status to Running.
      _ <- appQuery.updateStatus(params.appId, AppStatus.Running).transaction

      _ <- logger.info(ctx.loggingCtx)(
        s"Finished app creation for app ${params.appName.value} in cluster ${landingZoneResources.clusterName.value} in cloud context ${params.cloudContext.asString}"
      )

    } yield ()

  private[util] def pollCromwellAppCreation(userEmail: WorkbenchEmail, relayBaseUri: Uri)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] =
    for {
      ctx <- ev.ask
      tokenOpt <- samDao.getCachedArbitraryPetAccessToken(userEmail)
      token <- F.fromOption(tokenOpt, AppCreationException(s"Pet not found for user ${userEmail}", Some(ctx.traceId)))
      authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, token))

      op = List(
        cbasDao
          .getStatus(relayBaseUri, authHeader)
          .handleError(_ => false)
          // TODO: add WDS to status checks once https://github.com/DataBiosphere/terra-workspace-data-service/pull/135 is in a release
          // wdsDao.getStatus(relayBaseUri, authHeader).handleError(_ => false)
          // TODO (TOAZ-241): add cromwell to the status checks once it starts up
          // cromwellDao.getStatus(relayBaseUri, authHeader).handleError(_ => false)
      ).sequence
      cromwellOk <- streamFUntilDone(
        op,
        maxAttempts = config.appMonitorConfig.createApp.maxAttempts,
        delay = config.appMonitorConfig.createApp.interval
      ).interruptAfter(config.appMonitorConfig.createApp.interruptAfter).compile.lastOrError
    } yield cromwellOk.isDone

  private[util] def buildCromwellChartOverrideValues(release: Release,
                                                     cloudContext: AzureCloudContext,
                                                     samResourceId: AppSamResourceId,
                                                     landingZoneResources: LandingZoneResources,
                                                     relayHcName: RelayHybridConnectionName,
                                                     relayPrimaryKey: PrimaryKey,
                                                     petManagedIdentity: Identity
  ): Values =
    Values(
      List(
        // azure resources configs
        raw"config.resourceGroup=${cloudContext.managedResourceGroupName.value}",
        // TODO (TOAZ-227): set up Application Insights
//      raw"config.azureServicesAuthConnectionString=???",
//      raw"config.applicationInsightsAccountName=???",
//      raw"config.cosmosDbAccountName=???",
        raw"config.batchAccountName=${landingZoneResources.batchAccountName.value}",
        raw"config.batchNodesSubnetId=${landingZoneResources.batchNodesSubnetName.value}",

        // relay configs
        raw"relaylistener.connectionString=Endpoint=sb://${landingZoneResources.relayNamespace.value}.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=${relayPrimaryKey.value};EntityPath=${relayHcName.value}",
        raw"relaylistener.connectionName=${relayHcName.value}",
        raw"relaylistener.endpoint=https://${landingZoneResources.relayNamespace.value}.servicebus.windows.net",
        raw"relaylistener.targetHost=http://coa-${release.asString}-reverse-proxy-service:8000/",
        raw"relaylistener.samUrl=${config.samConfig.server}",
        raw"relaylistener.samResourceId=${samResourceId.resourceId}",
        raw"relaylistener.samResourceType=kubernetes-app",
        raw"relaylistener.samAction=connect",

        // persistence configs
        raw"persistence.storageResourceGroup=${cloudContext.managedResourceGroupName.value}",
        raw"persistence.storageAccount=${landingZoneResources.storageAccountName.value}",

        // identity configs
        raw"identity.name=${petManagedIdentity.name()}",
        raw"identity.resourceId=${petManagedIdentity.id()}",
        raw"identity.clientId=${petManagedIdentity.clientId()}",

        // general configs
        raw"fullnameOverride=coa-${release.asString}"
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

  override def deleteApp(params: DeleteAKSAppParams)(implicit ev: Ask[F, AppContext]): F[Unit] = {
    val DeleteAKSAppParams(appName, workspaceId, landingZoneResourcesOpt, cloudContext, keepHistory) = params
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
      dbCluster = dbApp.cluster

      // Get resources from landing zone
      landingZoneResources <- F.fromOption(
        landingZoneResourcesOpt,
        AppCreationException(
          s"Landing Zone Resources not found in app creation params for app ${app.appName.value}",
          Some(ctx.traceId)
        )
      )

      clusterName = landingZoneResources.clusterName // NOT the same as dbCluster.clusterName
      client <- buildCoreV1Client(cloudContext, landingZoneResources.clusterName)

      // Authenticate helm client
      authContext <- getHelmAuthContext(landingZoneResources.clusterName, cloudContext, namespaceName)

      _ <- helmClient.uninstall(app.release, keepHistory).run(authContext)

      // poll until the app pods are deleted
      last <- streamFUntilDone(
        listPodStatus(client, cloudContext, clusterName, KubernetesNamespace(app.appResources.namespace.name)),
        config.appMonitorConfig.deleteApp.maxAttempts,
        config.appMonitorConfig.deleteApp.interval
      ).compile.lastOrError

      _ <-
        if (!podDoneCheckable.isDone(last)) {
          val msg =
            s"Helm deletion has failed or timed out for app ${app.appName.value} in cluster ${dbCluster.getClusterId.toString}."
          logger.error(ctx.loggingCtx)(msg) >>
            F.raiseError[Unit](AppDeletionException(msg))
        } else F.unit

      // helm uninstall the setup chart
      _ <- helmClient
        .uninstall(
          getTerraAppSetupChartReleaseName(app.release),
          keepHistory
        )
        .run(authContext)

      // delete the namespace only after the helm uninstall completes.
      _ <- deleteNamespace(client, cloudContext, clusterName, kubernetesNamespace)

      fa = namespaceExists(client, cloudContext, clusterName, kubernetesNamespace)
        .map(
          !_
        ) // mapping to inverse because booleanDoneCheckable defines `Done` when it becomes `true`...In this case, the namespace will exists for a while, and eventually becomes non-existent

      _ <- streamUntilDoneOrTimeout(fa,
                                    config.appMonitorConfig.deleteApp.maxAttempts,
                                    config.appMonitorConfig.deleteApp.initialDelay,
                                    "delete namespace timed out"
      )

      userEmail = dbApp.app.googleServiceAccount
      tokenOpt <- samDao.getCachedArbitraryPetAccessToken(userEmail)

      _ <- tokenOpt match {
        case Some(token) =>
          for {
            _ <- samDao.deleteResourceInternal(dbApp.app.samResourceId,
                                               Authorization(Credentials.Token(AuthScheme.Bearer, token))
            )

          } yield ()
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

  private def deleteNamespace(client: CoreV1Api,
                              cloudContext: AzureCloudContext,
                              clusterName: AKSClusterName,
                              namespace: KubernetesNamespace
  )(implicit
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

  private def listPodStatus(client: CoreV1Api,
                            cloudContext: AzureCloudContext,
                            clusterName: AKSClusterName,
                            namespace: KubernetesNamespace
  )(implicit
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
    val certResource = autoClosableResourceF(new ByteArrayInputStream(credentials.certificate.value.getBytes))
    val endpoint = KubernetesApiServerIp(credentials.server.value)

    for {
      apiClient <- certResource.use { certStream =>
        F.delay(
          Config
            .fromToken(
              endpoint.url,
              credentials.token.value
            )
            .setSslCaCert(certStream)
        )
      }
    } yield apiClient // appending here a .setDebugging(true) prints out useful API request/response info for development
  }

  private def namespaceExists(client: CoreV1Api,
                              cloudContext: AzureCloudContext,
                              clusterName: AKSClusterName,
                              namespace: KubernetesNamespace
  )(implicit
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
  aadPodIdentityConfig: AadPodIdentityConfig,
  appRegistrationConfig: AzureAppRegistrationConfig,
  samConfig: SamConfig,
  appMonitorConfig: AppMonitorConfig
)
