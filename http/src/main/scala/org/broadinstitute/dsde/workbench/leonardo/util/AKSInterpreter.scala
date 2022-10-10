package org.broadinstitute.dsde.workbench
package leonardo
package util

import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import com.azure.core.management.AzureEnvironment
import com.azure.core.management.profile.AzureProfile
import com.azure.identity.ClientSecretCredential
import com.azure.resourcemanager.msi.MsiManager
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.leonardo.db.{KubernetesServiceDbQueries, _}
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppNotFoundException
import org.broadinstitute.dsp.{Release, _}
import org.http4s.Uri
import org.typelevel.log4cats.StructuredLogger

import java.util.Base64
import scala.concurrent.ExecutionContext

class AKSInterpreter[F[_]](config: AKSInterpreterConfig,
                           clientSecretCredential: ClientSecretCredential,
                           helmClient: HelmAlgebra[F],
                           azureContainerService: AzureContainerService[F],
                           azureRelayService: AzureRelayService[F]
)(implicit
  executionContext: ExecutionContext,
  logger: StructuredLogger[F],
  dbRef: DbReference[F],
  F: Async[F]
) extends AKSAlgebra[F] {

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

      _ <- logger.info(ctx.loggingCtx)(
        s"Begin app creation for app ${params.appName.value} in cloud context ${params.cloudContext.asString}"
      )

      // Get resources from landing zone
      // TODO hardcoded!!!
      landingZoneResources = LandingZoneResources(
        AKSClusterName("cluster"),
        BatchAccountName("batch-account"),
        RelayNamespace("relay-ns"),
        StorageAccountName("storage-account"),
        SubnetName("batch-subnet")
      )

      // Resolve user-assigned managed identity in Azure
      msiManager <- buildMsiManager(params.cloudContext)
      uami <- F.delay(
        msiManager
          .identities()
          .getByResourceGroup(params.cloudContext.managedResourceGroupName.value, params.managedIdentityName.value)
      )

      // Deploy setup chart
      authContext <- getHelmAuthContext(landingZoneResources.clusterName, params.cloudContext, namespaceName)
      _ <- helmClient
        .installChart(
          Release(s"${app.release.asString}-setup-rls"),
          config.terraAppSetupChartConfig.chartName,
          config.terraAppSetupChartConfig.chartVersion,
          org.broadinstitute.dsp.Values(
            s"cloud=azure,serviceAccount.annotations.azureManagedIdentityClientId=${uami.clientId},serviceAccount.name=${ksaName.value}"
          ),
          true
        )
        .run(authContext)

      // Create federated identity
      _ <- setUpFederatedIdentity(params.cloudContext,
                                  landingZoneResources.clusterName,
                                  params.managedIdentityName,
                                  namespaceName,
                                  ksaName
      )

      // Create relay hybrid connection pool
      hcName = RelayHybridConnectionName(params.appName.value)
      primaryKey <- azureRelayService.createRelayHybridConnection(landingZoneResources.relayNamespace,
                                                                  hcName,
                                                                  params.cloudContext
      )

      // Build values and install chart
      values = buildCromwellChartOverrideValues(app.release,
                                                params.cloudContext,
                                                ksaName,
                                                app.samResourceId,
                                                landingZoneResources,
                                                hcName,
                                                primaryKey
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

      _ <- logger.info(ctx.loggingCtx)(
        s"Finished app creation for app ${params.appName.value} in cluster ${landingZoneResources.clusterName.value} in cloud context ${params.cloudContext.asString}"
      )

      // TODO: poll app!!

    } yield ()

  private[util] def buildCromwellChartOverrideValues(release: Release,
                                                     cloudContext: AzureCloudContext,
                                                     ksaName: ServiceAccountName,
                                                     samResourceId: SamResourceId,
                                                     landingZoneResources: LandingZoneResources,
                                                     relayHcName: RelayHybridConnectionName,
                                                     relayPrimaryKey: PrimaryKey
  ): Values =
    Values(
      List(
        // azure resources configs
        raw"config.resourceGroup=${cloudContext.managedResourceGroupName.value}",
        // TODO: missing, what are these?
//      raw"config.azureServicesAuthConnectionString=???",
//      raw"config.applicationInsightsAccountName=???",
//      raw"config.cosmosDbAccountName=???",
        raw"config.batchAccountName=${landingZoneResources.batchAccountName.value}",
        raw"config.batchNodesSubnetId=${landingZoneResources.batchNodesSubnetName.value}",

        // relay configs
        raw"relaylistener.samResourceId=${samResourceId.resourceId}",
        raw"relaylistener.connectionString=Endpoint=sb://${landingZoneResources.relayNamespace.value}.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=${relayPrimaryKey.value};EntityPath=${relayHcName.value}",
        raw"relaylistener.hostIp=${landingZoneResources.relayNamespace.value}.servicebus.windows.net",
        raw"relaylistener.connectionName=${relayHcName.value}",
        raw"relaylistener.samUrl=${config.samUrl.renderString}",

        // persistence configs
        raw"persistence.storageResourceGroup=${cloudContext.managedResourceGroupName.value}",
        raw"persistence.storageAccount=${landingZoneResources.storageAccountName.value}",

        // general configs
        raw"fullnameOverride=coa-${release.asString}"
        // TODO ksaNama
      ).mkString(",")
    )

  private def getHelmAuthContext(clusterName: AKSClusterName,
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

  // TODO: the Java APIs don't exist so this is a no-op
  // Investigate implementing with REST API calls or ARM templates
  private def setUpFederatedIdentity(cloudContext: AzureCloudContext,
                                     clusterName: AKSClusterName,
                                     managedIdentityName: ManagedIdentityName,
                                     namespace: NamespaceName,
                                     ksaName: ServiceAccountName
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask

      // Resolve cluster in Azure
      // TODO ideally there'd be a way to get the oidc issuer
      cluster <- azureContainerService.getCluster(clusterName, cloudContext)
      oidcIssuer = "???"

      // Create federated credential
      // TODO this API doesn't exist, showing how it should look
      msiManager <- buildMsiManager(cloudContext)
//      _ <- F.delay(
//        msiManager
//          .federatedIdentities()
//          .create(
//            name = s"federated-${managedIdentityName.value}",
//            uami = managedIdentityName.value,
//            issuer = oidcIssuer,
//            subject = s"system:serviceaccount:${namespace.asString}:${ksaName.value}"
//          )
//      )
    } yield ()

  private def buildMsiManager(azureCloudContext: AzureCloudContext): F[MsiManager] = {
    val azureProfile =
      new AzureProfile(azureCloudContext.tenantId.value, azureCloudContext.subscriptionId.value, AzureEnvironment.AZURE)
    F.delay(MsiManager.authenticate(clientSecretCredential, azureProfile))
  }

}

final case class AKSInterpreterConfig(terraAppSetupChartConfig: TerraAppSetupChartConfig, samUrl: Uri)
