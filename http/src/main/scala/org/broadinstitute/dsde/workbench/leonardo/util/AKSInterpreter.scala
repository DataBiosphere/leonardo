package org.broadinstitute.dsde.workbench
package leonardo
package util

import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.NamespaceName
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.AppSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.config.{CoaAppConfig, SamConfig}
import org.broadinstitute.dsde.workbench.leonardo.db.{KubernetesServiceDbQueries, _}
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppNotFoundException
import org.broadinstitute.dsp.{Release, _}
import org.typelevel.log4cats.StructuredLogger

import java.util.Base64
import scala.concurrent.ExecutionContext

class AKSInterpreter[F[_]](config: AKSInterpreterConfig,
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
      landingZoneResources = getLandingZoneResources

      // TODO (TOAZ-228): Annotate managed identity and set up Workload Identity

      // Deploy setup chart
      authContext <- getHelmAuthContext(landingZoneResources.clusterName, params.cloudContext, namespaceName)
      _ <- helmClient
        .installChart(
          Release(s"${app.release.asString}-setup-rls"),
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

      // Build values and install chart
      values = buildCromwellChartOverrideValues(app.release,
                                                params.cloudContext,
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

      // TODO (TOAZ-229): poll app for completion

    } yield ()

  private[util] def buildCromwellChartOverrideValues(release: Release,
                                                     cloudContext: AzureCloudContext,
                                                     samResourceId: AppSamResourceId,
                                                     landingZoneResources: LandingZoneResources,
                                                     relayHcName: RelayHybridConnectionName,
                                                     relayPrimaryKey: PrimaryKey
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
        // TODO (TOAZ-242): change to kubernetes-app resource type once listener supports it
        raw"relaylistener.samResourceType=controlled-application-private-workspace-resource",

        // persistence configs
        raw"persistence.storageResourceGroup=${cloudContext.managedResourceGroupName.value}",
        raw"persistence.storageAccount=${landingZoneResources.storageAccountName.value}",

        // general configs
        raw"fullnameOverride=coa-${release.asString}"
      ).mkString(",")
    )

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

  // TODO (TOAZ-232): replace hard-coded values with LZ API calls
  private def getLandingZoneResources: LandingZoneResources =
    LandingZoneResources(
      AKSClusterName("lz2e6dcd2d552ad623cd338a1"),
      BatchAccountName("lzcfda4a58c8aee1f20396d8"),
      RelayNamespace("lz90d831b04e4bc77a174535ec31929eb838b9e306404081a1"),
      StorageAccountName("lzd8f8824b75a8148fb67dff"),
      SubnetName("BATCH_SUBNET")
    )

}

final case class AKSInterpreterConfig(terraAppSetupChartConfig: TerraAppSetupChartConfig,
                                      coaAppConfig: CoaAppConfig,
                                      samConfig: SamConfig
)
