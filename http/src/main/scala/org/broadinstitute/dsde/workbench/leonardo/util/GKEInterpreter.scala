package org.broadinstitute.dsde.workbench.leonardo
package util

import _root_.org.typelevel.log4cats.StructuredLogger
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.compute.v1.Disk
import com.google.container.v1._
import fs2.io.file.Files
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.DoneCheckableInstances._
import org.broadinstitute.dsde.workbench.DoneCheckableSyntax._
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.google.GoogleUtilities.RetryPredicates._
import org.broadinstitute.dsde.workbench.google2.GKEModels._
import org.broadinstitute.dsde.workbench.google2.KubernetesModels._
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{
  NamespaceName,
  ServiceAccountName,
  ServiceName
}
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.{
  streamFUntilDone,
  streamUntilDoneOrTimeout,
  tracedRetryF,
  DiskName,
  GoogleComputeService,
  GoogleDiskService,
  GoogleResourceService,
  KubernetesClusterNotFoundException,
  PvName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.dao.{AppDAO, AppDescriptorDAO}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppNotFoundException
import org.broadinstitute.dsde.workbench.leonardo.util.BuildHelmChartValues.{
  buildAllowedAppChartOverrideValuesString,
  buildCromwellAppChartOverrideValuesString,
  buildCustomChartOverrideValuesString,
  buildGalaxyChartOverrideValuesString
}
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError.PubsubKubernetesError
import org.broadinstitute.dsde.workbench.leonardo.util.GKEAlgebra._
import org.broadinstitute.dsde.workbench.model.google.{generateUniqueBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{IP, TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsp._
import org.http4s.Uri
import org.broadinstitute.dsde.workbench.leonardo.Autopilot
import com.google.api.services.container.model.WorkloadPolicyConfig
import java.net.URL
import java.util.Base64
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class GKEInterpreter[F[_]](
  config: GKEInterpreterConfig,
  bucketHelper: BucketHelper[F],
  vpcAlg: VPCAlgebra[F],
  gkeService: org.broadinstitute.dsde.workbench.google2.GKEService[F],
  kubeService: org.broadinstitute.dsde.workbench.google2.KubernetesService[F],
  helmClient: HelmAlgebra[F],
  appDao: AppDAO[F],
  credentials: GoogleCredentials,
  googleIamDAO: GoogleIamDAO,
  googleDiskService: GoogleDiskService[F],
  appDescriptorDAO: AppDescriptorDAO[F],
  nodepoolLock: KeyLock[F, KubernetesClusterId],
  googleResourceService: GoogleResourceService[F],
  computeService: GoogleComputeService[F]
)(implicit
  val executionContext: ExecutionContext,
  logger: StructuredLogger[F],
  dbRef: DbReference[F],
  metrics: OpenTelemetryMetrics[F],
  F: Async[F],
  files: Files[F]
) extends GKEAlgebra[F] {
  // DoneCheckable instances
  implicit private def optionDoneCheckable[A]: DoneCheckable[Option[A]] = (a: Option[A]) => a.isDefined
  implicit private def booleanDoneCheckable: DoneCheckable[Boolean] = identity[Boolean]
  implicit private def podDoneCheckable: DoneCheckable[List[KubernetesPodStatus]] =
    (ps: List[KubernetesPodStatus]) => ps.forall(isPodDone)
  implicit private def listDoneCheckable[A: DoneCheckable]: DoneCheckable[List[A]] = as => as.forall(_.isDone)

  override def createCluster(params: CreateClusterParams)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[CreateClusterResult]] = {
    val autopilot = new com.google.api.services.container.model.Autopilot().setEnabled(params.autopilot)
    if (params.autopilot)
      autopilot.setWorkloadPolicyConfig(new WorkloadPolicyConfig().setAllowNetAdmin(true))

    for {
      ctx <- ev.ask

      // Grab records from the database
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(params.clusterId).transaction
      dbCluster <- F.fromOption(
        clusterOpt,
        KubernetesClusterNotFoundException(
          s"Failed kubernetes cluster creation. Cluster with id ${params.clusterId.id} not found in database | trace id: ${ctx.traceId}"
        )
      )

      _ <- logger.info(ctx.loggingCtx)(
        s"Beginning cluster creation for cluster ${dbCluster.getClusterId.toString}"
      )

      // Get nodepools to pass in the create cluster request
      projectLabels <- googleResourceService.getLabels(params.googleProject)
      nodepools =
        if (params.autopilot) List.empty
        else
          dbCluster.nodepools
            .filter(n => params.nodepoolsToCreate.contains(n.id))
            .map(np => buildLegacyGoogleNodepool(np, params.googleProject, projectLabels))

      _ <-
        if (nodepools.size != params.nodepoolsToCreate.size)
          F.raiseError[Unit](
            ClusterCreationException(
              ctx.traceId,
              s"CreateCluster was called with nodepools that are not present in the database for cluster ${dbCluster.getClusterId.toString}"
            )
          )
        else F.unit

      // Set up VPC and firewall
      (network, subnetwork) <- vpcAlg.setUpProjectNetworkAndFirewalls(
        SetUpProjectNetworkParams(params.googleProject, dbCluster.region)
      )

      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(dbCluster.cloudContext),
        new RuntimeException("trying to create an azure runtime in GKEInterpreter. This should never happen")
      )
      kubeNetwork = KubernetesNetwork(googleProject, network)
      kubeSubNetwork = KubernetesSubNetwork(googleProject, dbCluster.region, subnetwork)

      networkConfig = new com.google.api.services.container.model.NetworkConfig()
        .setEnableIntraNodeVisibility(params.enableIntraNodeVisibility)

      networkPolicy =
        if (params.autopilot) null else new com.google.api.services.container.model.NetworkPolicy().setEnabled(true)

      legacyCreateClusterRec = new com.google.api.services.container.model.Cluster()
        .setName(dbCluster.clusterName.value)
        .setInitialClusterVersion(config.clusterConfig.version.value)
        .setNodePools(nodepools.asJava)
        .setAutopilot(
          autopilot
        )
        .setLegacyAbac(new com.google.api.services.container.model.LegacyAbac().setEnabled(false))
        .setNetwork(kubeNetwork.idString)
        .setSubnetwork(kubeSubNetwork.idString)
        .setResourceLabels(Map("leonardo" -> "true").asJava)
        .setNetworkConfig(networkConfig)
        .setNetworkPolicy(
          networkPolicy
        )
        .setMasterAuthorizedNetworksConfig(
          new com.google.api.services.container.model.MasterAuthorizedNetworksConfig()
            .setEnabled(true)
            .setCidrBlocks(
              config.clusterConfig.authorizedNetworks
                .map(ip => new com.google.api.services.container.model.CidrBlock().setCidrBlock(ip.value))
                .asJava
            )
        )
        .setIpAllocationPolicy(
          new com.google.api.services.container.model.IPAllocationPolicy()
            .setUseIpAliases(true)
        )
        .setWorkloadIdentityConfig(
          new com.google.api.services.container.model.WorkloadIdentityConfig()
            .setWorkloadPool(s"${params.googleProject.value}.svc.id.goog")
        )
      location =
        if (params.autopilot) org.broadinstitute.dsde.workbench.google2.Location(dbCluster.region.value)
        else dbCluster.location
      // Submit request to GKE
      req = KubernetesCreateClusterRequest(googleProject, location, legacyCreateClusterRec)
      // the Operation will be none if we get a 409, indicating we have already created this cluster
      operationOpt <- gkeService.createCluster(req)

    } yield operationOpt.map(op =>
      CreateClusterResult(KubernetesOperationId(googleProject, location, op.getName), kubeNetwork, kubeSubNetwork)
    )
  }

  override def pollCluster(params: PollClusterParams)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask

      // Grab records from the database
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(params.clusterId).transaction
      dbCluster <- F.fromOption(
        clusterOpt,
        KubernetesClusterNotFoundException(
          s"Failed kubernetes cluster creation. Cluster with id ${params.clusterId} not found in database"
        )
      )

      _ <- logger.info(ctx.loggingCtx)(
        s"Polling cluster creation for cluster ${dbCluster.getClusterId.toString}"
      )

      _ <- F.fromOption(dbCluster.nodepools.find(_.isDefault), DefaultNodepoolNotFoundException(dbCluster.id))
      // Poll GKE until completion
      lastOp <- gkeService
        .pollOperation(
          params.createResult.op,
          config.monitorConfig.clusterCreate.interval,
          config.monitorConfig.clusterCreate.maxAttempts
        )
        .compile
        .lastOrError

      _ <-
        if (lastOp.isDone)
          logger.info(ctx.loggingCtx)(
            s"Create cluster operation has finished for cluster ${dbCluster.getClusterId.toString}"
          )
        else
          logger.error(ctx.loggingCtx)(
            s"Create cluster operation timed out or failed for cluster ${dbCluster.getClusterId.toString}"
          ) >>
            // Note LeoPubsubMessageSubscriber will transition things to Error status if an exception is thrown
            F.raiseError[Unit](
              ClusterCreationException(
                ctx.traceId,
                s"Cluster creation timed out or failed for ${dbCluster.getClusterId.toString} | trace id: ${ctx.traceId}"
              )
            )

      // Resolve the cluster in Google
      googleClusterOpt <- gkeService.getCluster(dbCluster.getClusterId)
      googleCluster <- F.fromOption(
        googleClusterOpt,
        ClusterCreationException(
          ctx.traceId,
          s"Cluster not found in Google: ${dbCluster.getClusterId.toString} | trace id: ${ctx.traceId}"
        )
      )

      _ <- logger.info(ctx.loggingCtx)(
        s"Successfully created cluster ${dbCluster.getClusterId.toString}!"
      )

      // TODO: Handle the case where currently, if ingress installation fails, the cluster is marked as `Error`ed
      // and users can no longer create apps in the cluster's project
      // helm install nginx
      loadBalancerIp <- installNginx(dbCluster, googleCluster)
      ipRange <- F.fromOption(Config.vpcConfig.subnetworkRegionIpRangeMap.get(dbCluster.region),
                              RegionNotSupportedException(dbCluster.region, ctx.traceId)
      )

      _ <- kubernetesClusterQuery
        .updateAsyncFields(
          dbCluster.id,
          KubernetesClusterAsyncFields(
            IP(loadBalancerIp.asString),
            IP(googleCluster.getEndpoint),
            NetworkFields(
              params.createResult.network.name,
              params.createResult.subnetwork.name,
              ipRange
            )
          )
        )
        .transaction
      _ <- kubernetesClusterQuery.updateStatus(dbCluster.id, KubernetesClusterStatus.Running).transaction
      _ <- nodepoolQuery.updateStatuses(dbCluster.nodepools.map(_.id), NodepoolStatus.Running).transaction
    } yield ()

  override def createAndPollNodepool(params: CreateNodepoolParams)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      dbNodepoolOpt <- nodepoolQuery.getMinimalById(params.nodepoolId).transaction
      dbNodepool <- F.fromOption(dbNodepoolOpt, NodepoolNotFoundException(params.nodepoolId))
      dbClusterOpt <- kubernetesClusterQuery.getMinimalClusterById(dbNodepool.clusterId).transaction
      dbCluster <- F.fromOption(
        dbClusterOpt,
        KubernetesClusterNotFoundException(
          s"Cluster with id ${dbNodepool.clusterId} not found in database | trace id: ${ctx.traceId}"
        )
      )

      _ <- logger.info(ctx.loggingCtx)(
        s"Beginning nodepool creation for nodepool ${dbNodepool.nodepoolName.value} in cluster ${dbCluster.getClusterId.toString}"
      )

      projectLabels <- googleResourceService.getLabels(params.googleProject)
      req = KubernetesCreateNodepoolRequest(
        dbCluster.getClusterId,
        buildGoogleNodepool(dbNodepool, params.googleProject, projectLabels)
      )

      operationOpt <- nodepoolLock.withKeyLock(dbCluster.getClusterId) {
        for {
          opOpt <- gkeService.createNodepool(req)
          lastOpOpt <- opOpt.traverse { op =>
            F.sleep(10 seconds) >> gkeService
              .pollOperation(
                KubernetesOperationId(params.googleProject, dbCluster.location, op.getName),
                config.monitorConfig.nodepoolCreate.interval,
                config.monitorConfig.nodepoolCreate.maxAttempts
              )
              .compile
              .lastOrError
          }
          _ <- lastOpOpt.traverse_ { op =>
            if (op.isDone)
              logger.info(ctx.loggingCtx)(
                s"Nodepool creation operation has finished for nodepool with id ${params.nodepoolId.id}"
              )
            else
              logger.error(ctx.loggingCtx)(
                s"Create nodepool operation has failed or timed out for nodepool with id ${params.nodepoolId.id}"
              ) >>
                // Note LeoPubsubMessageSubscriber will transition things to Error status if an exception is thrown
                F.raiseError[Unit](NodepoolCreationException(params.nodepoolId))
          }
        } yield opOpt
      }

      _ <- operationOpt.traverse(_ => nodepoolQuery.updateStatus(params.nodepoolId, NodepoolStatus.Running).transaction)
    } yield ()

  override def createAndPollApp(params: CreateAppParams)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      // Grab records from the database
      dbAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(CloudContext.Gcp(params.googleProject), params.appName)
        .transaction
      dbApp <- F.fromOption(dbAppOpt,
                            AppNotFoundException(CloudContext.Gcp(params.googleProject),
                                                 params.appName,
                                                 ctx.traceId,
                                                 "No active app found in DB"
                            )
      )
      app = dbApp.app
      namespaceName = app.appResources.namespace
      dbCluster = dbApp.cluster
      gkeClusterId = dbCluster.getClusterId
      googleProject = params.googleProject

      // TODO: This DB query might not be needed if it makes sense to add diskId in App model (will revisit in next PR)
      diskOpt <- appQuery.getDiskId(app.id).transaction
      diskId <- F.fromOption(diskOpt, DiskNotFoundForAppException(app.id, ctx.traceId))

      // Create namespace and secrets
      _ <- logger.info(ctx.loggingCtx)(
        s"Begin App(${app.appName.value}) Creation."
      )

      // Create KSA
      ksaName <- F.fromOption(
        app.appResources.kubernetesServiceAccountName,
        AppCreationException(
          s"Kubernetes Service Account not found in DB for app ${app.appName.value} | trace id: ${ctx.traceId}"
        )
      )
      gsa = dbApp.app.googleServiceAccount

      // Resolve the cluster in Google
      googleClusterOpt <- gkeService.getCluster(gkeClusterId)
      googleCluster <- googleClusterOpt match {
        case Some(value) => F.pure(value)
        case None =>
          kubernetesClusterQuery.markAsDeleted(dbCluster.id, ctx.now).transaction >>
            F.raiseError[Cluster](
              ClusterCreationException(
                ctx.traceId,
                s"Cluster not found in Google: ${gkeClusterId.toString} | trace id: ${ctx.traceId}"
              )
            )
      }

      nfsDisk <- F.fromOption(
        dbApp.app.appResources.disk,
        AppCreationException(s"NFS disk not found in DB for app ${app.appName.value} | trace id: ${ctx.traceId}")
      )

      helmAuthContext <- getHelmAuthContext(googleCluster, dbCluster, namespaceName)

      _ <- helmClient
        .installChart(
          getTerraAppSetupChartReleaseName(app.release),
          config.terraAppSetupChartConfig.chartName,
          config.terraAppSetupChartConfig.chartVersion,
          org.broadinstitute.dsp.Values(
            s"serviceAccount.annotations.gcpServiceAccount=${gsa.value},serviceAccount.name=${ksaName.value}"
          ),
          true
        )
        .run(helmAuthContext)
      // update KSA in DB
      _ <- appQuery.updateKubernetesServiceAccount(app.id, ksaName).transaction

      // Associate GSA to newly created KSA
      // This string is constructed based on Google requirements to associate a GSA to a KSA
      // (https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity#creating_a_relationship_between_ksas_and_gsas)
      ksaToGsa = s"${googleProject.value}.svc.id.goog[${namespaceName.value}/${ksaName.value}]"
      call = F.fromFuture(
        F.delay(
          googleIamDAO.addIamPolicyBindingOnServiceAccount(googleProject,
                                                           gsa,
                                                           WorkbenchEmail(ksaToGsa),
                                                           Set("roles/iam.workloadIdentityUser")
          )
        )
      )
      retryConfig = RetryPredicates.retryConfigWithPredicates(
        when409
      )
      _ <- tracedRetryF(retryConfig)(
        call,
        s"googleIamDAO.addIamPolicyBindingOnServiceAccount for GSA ${gsa.value} & KSA ${ksaName.value}"
      ).compile.lastOrError

      // TODO: validate app release is the same as restore release
      appRestore: Option[AppRestore] <- persistentDiskQuery.getAppDiskRestore(diskId).transaction
      galaxyRestore: Option[AppRestore.GalaxyRestore] = appRestore.flatMap {
        case a: AppRestore.GalaxyRestore => Some(a)
        case _: AppRestore.Other         => None
      }

      nodepool = if (app.autopilot.isDefined) None else Some(dbApp.nodepool.nodepoolName)
      // helm install and wait
      _ <- app.appType match {
        case AppType.Galaxy =>
          for {
            machineType <- F.fromOption(
              params.appMachineType,
              new LeoException(
                s"can't find machine config for ${googleProject.value}/${app.appName.value}. This should never happen",
                traceId = Some(ctx.traceId)
              )
            )
            _ <- installGalaxy(
              helmAuthContext,
              app.appName,
              app.release,
              app.chart,
              dbCluster,
              dbApp.nodepool.nodepoolName, // TODO: support autopilot mode
              namespaceName,
              app.auditInfo.creator,
              app.customEnvironmentVariables,
              ksaName,
              nfsDisk,
              machineType,
              galaxyRestore
            )
          } yield ()
        case AppType.Cromwell =>
          installCromwellApp(
            helmAuthContext,
            app.appName,
            app.release,
            dbCluster,
            nodepool,
            namespaceName,
            nfsDisk,
            ksaName,
            gsa,
            app.customEnvironmentVariables
          )
        case AppType.Allowed =>
          installAllowedApp(
            helmAuthContext,
            app.id,
            app.appName,
            app.release,
            app.chart,
            dbCluster,
            nodepool,
            namespaceName,
            nfsDisk,
            ksaName,
            gsa,
            app.auditInfo.creator,
            app.customEnvironmentVariables,
            app.autopilot
          )
        case AppType.Custom =>
          installCustomApp(
            app.id,
            app.appName,
            app.release,
            dbCluster,
            googleCluster,
            nodepool,
            namespaceName,
            nfsDisk,
            app.descriptorPath,
            app.extraArgs,
            ksaName,
            app.customEnvironmentVariables
          )
        case _ =>
          F.raiseError(AppCreationException(s"App type ${app.appType} not supported on GCP"))
      }

      _ <- logger.info(ctx.loggingCtx)(
        s"Finished app creation for app ${app.appName.value} in cluster ${gkeClusterId.toString}"
      )

      _ <- app.appType match {
        case AppType.Galaxy =>
          if (galaxyRestore.isDefined) persistentDiskQuery.updateLastUsedBy(diskId, app.id).transaction.void
          else
            for {
              pvcs <- kubeService.listPersistentVolumeClaims(gkeClusterId,
                                                             KubernetesNamespace(app.appResources.namespace)
              )

              _ <- pvcs
                // We added an extra -galaxy here: https://github.com/galaxyproject/galaxykubeman-helm/blob/f7f27be74c213deda3ae53122[…]959c96480bb21f/galaxykubeman/templates/config-setup-galaxy.yaml
                .find(pvc => pvc.getMetadata.getName == s"${app.release.asString}-galaxy-galaxy-pvc")
                .fold(
                  F.raiseError[Unit](
                    PubsubKubernetesError(AppError("Fail to retrieve pvc ids",
                                                   ctx.now,
                                                   ErrorAction.CreateApp,
                                                   ErrorSource.App,
                                                   None,
                                                   Some(ctx.traceId)
                                          ),
                                          Some(app.id),
                                          false,
                                          None,
                                          None,
                                          None
                    )
                  )
                ) { galaxyPvc =>
                  val galaxyDiskRestore = AppRestore.GalaxyRestore(
                    PvcId(galaxyPvc.getMetadata.getUid),
                    app.id
                  )
                  persistentDiskQuery
                    .updateGalaxyDiskRestore(diskId, galaxyDiskRestore)
                    .transaction
                    .void
                }
            } yield ()
        case AppType.Cromwell => persistentDiskQuery.updateLastUsedBy(diskId, app.id).transaction
        case AppType.Allowed  => persistentDiskQuery.updateLastUsedBy(diskId, app.id).transaction
        case AppType.Custom   => F.unit
        case _ =>
          F.raiseError(AppCreationException(s"App type ${app.appType} not supported on GCP"))
      }

      readyTime <- F.realTimeInstant
      _ <- appUsageQuery.recordStart(params.appId, readyTime)
      _ <- appQuery.updateStatus(params.appId, AppStatus.Running).transaction
    } yield ()

  override def updateAndPollApp(params: UpdateAppParams)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask

      googleProject <- F.fromOption(
        params.googleProject,
        AppUpdateException(
          s"${params.appName} must have a google project in the GCP cloud context",
          Some(ctx.traceId)
        )
      )

      // Grab records from the database
      dbAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(CloudContext.Gcp(googleProject), params.appName)
        .transaction
      dbApp <- F.fromOption(
        dbAppOpt,
        AppNotFoundException(CloudContext.Gcp(googleProject), params.appName, ctx.traceId, "No active app found in DB")
      )
      _ <- logger.info(ctx.loggingCtx)(s"Updating app ${params.appName} in project ${googleProject}")

      // Grab all of the values that we need to resend to the helm update command for each app
      dbCluster = dbApp.cluster
      nodepoolName = dbApp.nodepool.nodepoolName
      machineTypeName = dbApp.nodepool.machineType
      gkeClusterId = dbCluster.getClusterId

      app = dbApp.app
      namespaceName = app.appResources.namespace
      gsa = app.googleServiceAccount
      nfsDisk <- F.fromOption(
        dbApp.app.appResources.disk,
        AppUpdateException(s"NFS disk not found in DB for app ${app.appName.value}", Some(ctx.traceId))
      )
      ksaName <- F.fromOption(
        app.appResources.kubernetesServiceAccountName,
        AppUpdateException(
          s"Kubernetes Service Account not found in DB for app ${app.appName.value}",
          Some(ctx.traceId)
        )
      )
      userEmail = app.auditInfo.creator
      stagingBucketName = generateUniqueBucketName("leostaging-" + params.appName.value)

      // Resolve the cluster in Google
      googleClusterOpt <- gkeService.getCluster(gkeClusterId)
      googleCluster <- F.fromOption(
        googleClusterOpt,
        AppUpdateException(s"Cluster not found in Google: ${gkeClusterId}", Some(ctx.traceId))
      )
      nodepool = if (app.autopilot.isDefined) None else Some(dbApp.nodepool.nodepoolName)
      chartOverridesAndAppOkF = app.appType match {
        case AppType.Galaxy =>
          for {

            postgresDiskNameOpt <- for {
              disk <- getGalaxyPostgresDisk(nfsDisk.name, namespaceName, googleProject, nfsDisk.zone)
            } yield disk.map(x => DiskName(x.getName))

            postgresDiskName <- F.fromOption(
              postgresDiskNameOpt,
              AppUpdateException(s"No postgres disk found in google for app ${app.appName.value} ",
                                 traceId = Some(ctx.traceId)
              )
            )

            appRestore: Option[AppRestore] <- persistentDiskQuery.getAppDiskRestore(nfsDisk.id).transaction
            galaxyRestore: Option[AppRestore.GalaxyRestore] = appRestore.flatMap {
              case a: AppRestore.GalaxyRestore => Some(a)
              case _: AppRestore.Other         => None
            }

            machineType <- computeService
              .getMachineType(googleProject,
                              ZoneName("us-central1-a"),
                              machineTypeName
              ) // TODO: if use non `us-central1-a` zone for galaxy, this needs to be udpated
              .flatMap(opt =>
                F.fromOption(
                  opt,
                  AppUpdateException(s"Unknown machine type for ${machineTypeName.value}", traceId = Some(ctx.traceId))
                )
              )

            appMachineType = AppMachineType(machineType.getMemoryMb / 1024, machineType.getGuestCpus)

            chartValues = buildGalaxyChartOverrideValuesString(
              config,
              app.appName,
              app.release,
              dbCluster,
              nodepoolName,
              userEmail,
              app.customEnvironmentVariables,
              ksaName,
              namespaceName,
              nfsDisk,
              postgresDiskName,
              appMachineType,
              galaxyRestore
            )

            last <- streamFUntilDone(
              appDao.isProxyAvailable(googleProject, dbApp.app.appName, ServiceName("galaxy")),
              config.monitorConfig.updateApp.maxAttempts,
              config.monitorConfig.updateApp.interval
            ).interruptAfter(config.monitorConfig.updateApp.interruptAfter).compile.lastOrError

          } yield (chartValues.mkString(","), last)
        case AppType.Cromwell =>
          for {

            last <- streamFUntilDone(
              config.cromwellAppConfig.services
                .map(_.name)
                .traverse(s => appDao.isProxyAvailable(googleProject, app.appName, s)),
              config.monitorConfig.createApp.maxAttempts,
              config.monitorConfig.createApp.interval
            ).interruptAfter(config.monitorConfig.createApp.interruptAfter).compile.lastOrError.map(x => x.isDone)

            chartValues = buildCromwellAppChartOverrideValuesString(
              config,
              app.appName,
              dbCluster,
              nodepool,
              namespaceName,
              nfsDisk,
              ksaName,
              gsa,
              app.customEnvironmentVariables
            )

          } yield (chartValues.mkString(","), last)
        case AppType.Allowed =>
          for {
            // Create the throwaway staging bucket to be used by Welder
            _ <- bucketHelper
              .createStagingBucket(userEmail, googleProject, stagingBucketName, gsa)
              .compile
              .drain

            allowedChart <- F.fromOption(
              AllowedChartName.fromChartName(app.chart.name),
              new RuntimeException(s"invalid chart name for ALLOWED app: ${app.chart.name}")
            )

            chartValues = buildAllowedAppChartOverrideValuesString(
              config,
              allowedChart,
              app.appName,
              dbCluster,
              nodepool,
              namespaceName,
              nfsDisk,
              ksaName,
              userEmail,
              stagingBucketName,
              app.customEnvironmentVariables,
              app.autopilot
            )

            last <- streamFUntilDone(
              config.allowedAppConfig.services
                .map(_.name)
                .traverse(s => appDao.isProxyAvailable(googleProject, dbApp.app.appName, s)),
              config.monitorConfig.updateApp.maxAttempts,
              config.monitorConfig.updateApp.interval
            ).interruptAfter(config.monitorConfig.updateApp.interruptAfter).compile.lastOrError.map(x => x.isDone)

          } yield (chartValues.mkString(","), last)
        case AppType.Custom =>
          for {

            desc <- F.fromOption(dbApp.app.descriptorPath, AppRequiresDescriptorException(dbApp.app.id))
            descriptor <- appDescriptorDAO.getDescriptor(desc).adaptError { case e =>
              AppUpdateException(
                s"Failed to process descriptor: $desc. Please ensure it is a valid descriptor, and that the remote file is valid yaml following the schema detailed here: https://github.com/DataBiosphere/terra-app#app-schema. \n\tOriginal message: ${e.getMessage}",
                Some(ctx.traceId)
              )
            }

            (serviceName, serviceConfig) = descriptor.services.head

            chartValues = buildCustomChartOverrideValuesString(
              config,
              params.appName,
              app.release,
              nodepool,
              serviceName,
              dbCluster,
              namespaceName,
              serviceConfig,
              app.extraArgs,
              nfsDisk,
              ksaName,
              serviceConfig.environment ++ app.customEnvironmentVariables
            )

            last <- streamFUntilDone(
              descriptor.services.keys.toList.traverse(s =>
                appDao.isProxyAvailable(googleProject, dbApp.app.appName, ServiceName(s))
              ),
              config.monitorConfig.updateApp.maxAttempts,
              config.monitorConfig.updateApp.interval
            ).interruptAfter(config.monitorConfig.updateApp.interruptAfter).compile.lastOrError.map(x => x.isDone)

          } yield (chartValues, last)
        case _ =>
          F.raiseError[(String, Boolean)](
            AppUpdateException(s"App type ${app.appType} not supported on GCP", Some(ctx.traceId))
          )
      }

      (chartOverrideValues, preUpdateAppOk) <- chartOverridesAndAppOkF

      // Authenticate helm client
      helmAuthContext <- getHelmAuthContext(googleCluster, dbCluster, namespaceName)

      // Fail if apps are not live before update attempt
      _ <-
        if (preUpdateAppOk)
          F.unit
        else
          F.raiseError[Unit](
            AppUpdatePollingException(
              s"App ${params.appName.value} is not live in cluster ${googleCluster} in cloud context ${CloudContext.Gcp(googleProject).asString}, failing prior to upgrade attempt",
              Some(ctx.traceId)
            )
          )

      // Change app status to updating
      _ <- appQuery.updateStatus(app.id, AppStatus.Updating).transaction

      // Upgrade app chart version and explicitly pass the values
      _ <- helmClient
        .upgradeChart(
          app.release,
          app.chart.name,
          params.appChartVersion,
          org.broadinstitute.dsp.Values(chartOverrideValues)
        )
        .run(helmAuthContext)

      // Fail if apps are not live after update attempt
      (_, postUpdateAppOk) <- chartOverridesAndAppOkF
      _ <-
        if (postUpdateAppOk)
          F.unit
        else
          F.raiseError[Unit](
            AppUpdatePollingException(
              s"App ${params.appName.value} failed to update in cluster ${googleCluster} in cloud context ${CloudContext.Gcp(googleProject).asString}",
              Some(ctx.traceId)
            )
          )

      _ <- logger.info(
        s"Update app operation has finished for app ${app.appName.value} in cluster ${googleCluster}"
      )

      // Update app chart version in the DB
      _ <- appQuery.updateChart(app.id, Chart(app.chart.name, params.appChartVersion)).transaction
      // Put app status back to running
      _ <- appQuery.updateStatus(app.id, AppStatus.Running).transaction

      _ <- logger.info(s"Done updating app ${params.appName} in project ${params.googleProject}")
    } yield ()

  override def deleteAndPollCluster(params: DeleteClusterParams)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      dbClusterOpt <- kubernetesClusterQuery.getMinimalClusterById(params.clusterId).transaction
      dbCluster <- F.fromOption(
        dbClusterOpt,
        KubernetesClusterNotFoundException(s"Cluster with id ${params.clusterId} not found in database")
      )
      // the operation will be None if the cluster is not found and we have already deleted it
      operationOpt <- gkeService.deleteCluster(dbCluster.getClusterId)
      lastOp <- operationOpt
        .traverse(op =>
          gkeService
            .pollOperation(
              KubernetesOperationId(params.googleProject, dbCluster.location, op.getName),
              config.monitorConfig.clusterDelete.interval,
              config.monitorConfig.clusterDelete.maxAttempts
            )
        )
        .compile
        .lastOrError
      _ <- lastOp.traverse_ { op =>
        if (op.isDone)
          logger.info(ctx.loggingCtx)(
            s"Delete cluster operation has finished for cluster ${params.clusterId}"
          )
        else
          logger.error(ctx.loggingCtx)(
            s"Delete cluster operation has failed or timed out for cluster ${params.clusterId}"
          ) >>
            F.raiseError[Unit](ClusterDeletionException(params.clusterId))
      }
      _ <- operationOpt.traverse(_ => kubernetesClusterQuery.markAsDeleted(params.clusterId, ctx.now).transaction)
    } yield ()

  override def deleteAndPollNodepool(
    params: DeleteNodepoolParams
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      dbNodepoolOpt <- nodepoolQuery.getMinimalById(params.nodepoolId).transaction
      dbNodepool <- F.fromOption(dbNodepoolOpt, NodepoolNotFoundException(params.nodepoolId))
      dbClusterOpt <- kubernetesClusterQuery.getMinimalClusterById(dbNodepool.clusterId).transaction
      dbCluster <- F.fromOption(
        dbClusterOpt,
        KubernetesClusterNotFoundException(s"Cluster with id ${dbNodepool.clusterId.id} not found in database")
      )

      _ <- logger.info(ctx.loggingCtx)(
        s"Beginning nodepool deletion for nodepool ${dbNodepool.nodepoolName.value} in cluster ${dbCluster.getClusterId.toString}"
      )

      _ <- nodepoolLock.withKeyLock(dbCluster.getClusterId) {
        for {
          operationOpt <- gkeService.deleteNodepool(
            NodepoolId(dbCluster.getClusterId, dbNodepool.nodepoolName)
          )
          lastOp <- operationOpt
            .traverse(op =>
              gkeService
                .pollOperation(
                  KubernetesOperationId(params.googleProject, dbCluster.location, op.getName),
                  config.monitorConfig.nodepoolDelete.interval,
                  config.monitorConfig.nodepoolDelete.maxAttempts
                )
            )
            .compile
            .lastOrError
          _ <- lastOp.traverse_ { op =>
            if (op.isDone)
              logger.info(ctx.loggingCtx)(
                s"Delete nodepool operation has finished for nodepool ${params.nodepoolId}"
              )
            else
              logger.error(
                ctx.loggingCtx(
                  s"Delete nodepool operation has failed or timed out for nodepool ${params.nodepoolId}"
                )
              ) >>
                F.raiseError[Unit](NodepoolDeletionException(params.nodepoolId))
          }
        } yield operationOpt
      }

      _ <- nodepoolQuery.markAsDeleted(params.nodepoolId, ctx.now).transaction
    } yield ()

  // This function DOES NOT update the app status to deleted after polling is complete
  // It decouples the AppStatus from the kubernetes entity, and makes it more representative of the app from the user's perspective
  // Currently, the only caller of this function updates the status after the nodepool is also deleted
  override def deleteAndPollApp(params: DeleteAppParams)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask

      dbAppOpt <- KubernetesServiceDbQueries
        .getFullAppById(CloudContext.Gcp(params.googleProject), params.appId)
        .transaction
      dbApp <- F.fromOption(dbAppOpt,
                            AppNotFoundException(CloudContext.Gcp(params.googleProject),
                                                 params.appName,
                                                 ctx.traceId,
                                                 "No active app found in DB"
                            )
      )

      app = dbApp.app
      namespaceName = app.appResources.namespace
      dbCluster = dbApp.cluster
      gkeClusterId = dbCluster.getClusterId

      _ <- logger.info(ctx.loggingCtx)(
        s"Beginning app deletion for app ${app.appName.value} in cluster ${gkeClusterId.toString}"
      )

      // Resolve the cluster in Google
      googleClusterOpt <- gkeService.getCluster(gkeClusterId)

      _ <- googleClusterOpt
        .traverse { googleCluster =>
          val uninstallCharts = for {
            helmAuthContext <- getHelmAuthContext(googleCluster, dbCluster, namespaceName)

            _ <- logger.info(ctx.loggingCtx)(
              s"Uninstalling release ${app.release.asString} for ${app.appType.toString} app ${app.appName.value} in cluster ${dbCluster.getClusterId.toString}"
            )

            // helm uninstall the app chart and wait
            _ <- helmClient
              .uninstall(app.release, config.galaxyAppConfig.uninstallKeepHistory)
              .run(helmAuthContext)

            last <- streamFUntilDone(
              kubeService.listPodStatus(dbCluster.getClusterId, KubernetesNamespace(namespaceName)),
              config.monitorConfig.deleteApp.maxAttempts,
              config.monitorConfig.deleteApp.interval
            ).compile.lastOrError

            _ <-
              if (!podDoneCheckable.isDone(last)) {
                val msg =
                  s"Helm deletion has failed or timed out for app ${app.appName.value} in cluster ${dbCluster.getClusterId.toString}. The following pods are not in a terminal state: ${last
                      .filterNot(isPodDone)
                      .map(_.name.value)
                      .mkString(", ")}"
                logger.error(ctx.loggingCtx)(msg) >>
                  F.raiseError[Unit](AppDeletionException(msg))
              } else F.unit

            // helm uninstall the setup chart
            _ <- helmClient
              .uninstall(
                getTerraAppSetupChartReleaseName(app.release),
                config.galaxyAppConfig.uninstallKeepHistory
              )
              .run(helmAuthContext)
          } yield ()

          uninstallCharts.handleErrorWith { e =>
            logger.info(ctx.loggingCtx)(
              s"Uninstalling release ${app.release.asString} for ${app.appType.toString} app ${app.appName.value} in cluster ${dbCluster.getClusterId.toString} failed with error ${e.getMessage}"
            )
          }
        }

      // delete the namespace only after the helm uninstall completes
      _ <- kubeService.deleteNamespace(dbApp.cluster.getClusterId,
                                       KubernetesNamespace(dbApp.app.appResources.namespace)
      )

      fa = kubeService
        .namespaceExists(dbApp.cluster.getClusterId, KubernetesNamespace(dbApp.app.appResources.namespace))
        .map(!_) // mapping to inverse because booleanDoneCheckable defines `Done` when it becomes `true`...In this case, the namespace will exists for a while, and eventually becomes non-existent

      _ <- streamUntilDoneOrTimeout(fa, 60, 5 seconds, "delete namespace timed out")
      _ <- logger.info(ctx.loggingCtx)(
        s"Delete app operation has finished for app ${app.appName.value} in cluster ${gkeClusterId.toString}"
      )

      appRestore: Option[AppRestore.GalaxyRestore] = dbApp.app.appResources.disk.flatMap(_.appRestore).flatMap {
        case a: AppRestore.GalaxyRestore => Some(a)
        case _: AppRestore.Other         => None
      }
      _ <- appRestore.traverse { restore =>
        for {
          _ <- kubeService.deletePv(dbCluster.getClusterId, PvName(s"pvc-${restore.galaxyPvcId.asString}"))
        } yield ()
      }

      _ <-
        if (!params.errorAfterDelete) {
          F.unit
        } else {
          appQuery.updateStatus(dbApp.app.id, AppStatus.Error).transaction.void
        }
    } yield ()

  override def stopAndPollApp(params: StopAppParams)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask

      dbAppOpt <- KubernetesServiceDbQueries
        .getFullAppById(CloudContext.Gcp(params.googleProject), params.appId)
        .transaction
      dbApp <- F.fromOption(dbAppOpt,
                            AppNotFoundException(CloudContext.Gcp(params.googleProject),
                                                 params.appName,
                                                 ctx.traceId,
                                                 "No active app found in DB"
                            )
      )

      _ <- logger.info(ctx.loggingCtx)(
        s"Stopping app ${dbApp.app.appName.value} in cluster ${dbApp.cluster.getClusterId.toString}"
      )

      _ <- dbApp.app.numOfReplicas match {
        case Some(_) =>
          // If the app has a numOfReplicas field, we'll stop it by scaling down replicas to 0
          for {
            // Scale the nodepool to zero nodes
            attemptToStop <- kubeService
              .patchReplicas(
                dbApp.cluster.getClusterId,
                KubernetesNamespace(dbApp.app.appResources.namespace),
                KubernetesDeployment(dbApp.app.appName.value), // appNames are the same as deployments
                0
              )
              .attempt

            // Update nodepool status to Running and app status to Stopped
            _ <- attemptToStop match {
              case Left(e) =>
                // This updates the APP back to `RUNNING` status instead of putting it into `ERROR` status. This
                // can be confusing to users since they will notice the APP is not stoppable.
                // Currently, Leo doesn't have a good way to inform users about "an error happened during Stopping",
                // which I think we should spend some effort design this out.
                // For now, I think this is better behavior than putting the APP into ERROR status, which will make the APP
                // unusable.
                // TODO: think about update appUsage
                logger.info(ctx.loggingCtx, e)("Failed to stop app") >> appQuery
                  .updateStatus(params.appId, AppStatus.Running)
                  .transaction
              case Right(_) => F.unit
            }
          } yield ()
        case None =>
          // If the app does not a numOfReplicas field, we'll stop it by scaling down nodepool
          scaleDownNodepool(dbApp.app.id, params.googleProject, dbApp.nodepool, dbApp.cluster)
      }

      _ <- appQuery.updateStatus(params.appId, AppStatus.Stopped).transaction
    } yield F.unit

  override def startAndPollApp(params: StartAppParams)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask

      dbAppOpt <- KubernetesServiceDbQueries
        .getFullAppById(CloudContext.Gcp(params.googleProject), params.appId)
        .transaction
      dbApp <- F.fromOption(dbAppOpt,
                            AppNotFoundException(CloudContext.Gcp(params.googleProject),
                                                 params.appName,
                                                 ctx.traceId,
                                                 "No active app found in DB"
                            )
      )
      dbCluster = dbApp.cluster

      _ <- logger.info(ctx.loggingCtx)(
        s"Starting app ${dbApp.app.appName.value} in cluster ${dbCluster.getClusterId.toString}"
      )

      _ <- dbApp.app.numOfReplicas match {
        case Some(count) =>
          for {
            attemptToStart <- kubeService
              .patchReplicas(
                dbApp.cluster.getClusterId,
                KubernetesNamespace(dbApp.app.appResources.namespace),
                KubernetesDeployment(dbApp.app.appName.value), // appNames are the same as deployments
                count
              )
              .attempt

            // Update nodepool status to Running and app status to Stopped
            _ <- attemptToStart match {
              case Left(e) =>
                // This updates the APP back to `RUNNNING` status instead of putting it into `ERROR` status. This
                // can be confusing to users since they will notice the APP is not stoppable.
                // Currently, Leo doesn't have a good way to inform users about "an error happened during Stopping",
                // which I think we should spend some effort design this out.
                // For now, I think this is better behavior than putting the APP into ERROR status, which will make the APP
                // unusable.
                logger.info(ctx.loggingCtx, e)("Failed to start app") >> appQuery
                  .updateStatus(params.appId, AppStatus.Stopped)
                  .transaction
              case Right(_) => F.unit
            }
          } yield ()
        case None => scaleUpNodepool(params.googleProject, dbApp.nodepool, dbApp.cluster)
      }

      isUp <- dbApp.app.appType match {
        case AppType.Galaxy =>
          streamFUntilDone(
            appDao.isProxyAvailable(params.googleProject, dbApp.app.appName, ServiceName("galaxy")),
            config.monitorConfig.startApp.maxAttempts,
            config.monitorConfig.startApp.interval
          ).interruptAfter(config.monitorConfig.startApp.interruptAfter).compile.lastOrError
        case AppType.Cromwell =>
          streamFUntilDone(
            config.cromwellAppConfig.services
              .map(_.name)
              .traverse(s => appDao.isProxyAvailable(params.googleProject, dbApp.app.appName, s)),
            config.monitorConfig.startApp.maxAttempts,
            config.monitorConfig.startApp.interval
          ).interruptAfter(config.monitorConfig.startApp.interruptAfter).compile.lastOrError.map(x => x.isDone)
        case AppType.Allowed =>
          streamFUntilDone(
            config.allowedAppConfig.services
              .map(_.name)
              .traverse(s => appDao.isProxyAvailable(params.googleProject, dbApp.app.appName, s)),
            config.monitorConfig.startApp.maxAttempts,
            config.monitorConfig.startApp.interval
          ).interruptAfter(config.monitorConfig.startApp.interruptAfter).compile.lastOrError.map(x => x.isDone)
        case AppType.Custom =>
          for {
            desc <- F.fromOption(dbApp.app.descriptorPath, AppRequiresDescriptorException(dbApp.app.id))
            descriptor <- appDescriptorDAO.getDescriptor(desc).adaptError { case e =>
              AppStartException(
                s"Failed to process descriptor: $desc. Please ensure it is a valid descriptor, and that the remote file is valid yaml following the schema detailed here: https://github.com/DataBiosphere/terra-app#app-schema. \n\tOriginal message: ${e.getMessage}"
              )
            }
            last <- streamFUntilDone(
              descriptor.services.keys.toList.traverse(s =>
                appDao.isProxyAvailable(params.googleProject, dbApp.app.appName, ServiceName(s))
              ),
              config.monitorConfig.startApp.maxAttempts,
              config.monitorConfig.startApp.interval
            ).interruptAfter(config.monitorConfig.startApp.interruptAfter).compile.lastOrError
          } yield last.isDone
        case AppType.Wds | AppType.HailBatch | AppType.WorkflowsApp | AppType.CromwellRunnerApp =>
          F.raiseError(AppCreationException(s"App type ${dbApp.app.appType} not supported on GCP"))
      }

      _ <-
        if (!isUp) {
          // If starting timed out, persist an error and attempt to stop the app again.
          // We don't want to move the app to Error status because that status is unrecoverable by the user.
          val msg =
            s"${dbApp.app.appType.toString} startup has failed or timed out for app ${dbApp.app.appName.value} in cluster ${dbCluster.getClusterId.toString}"
          for {
            _ <- logger.error(ctx.loggingCtx)(msg)
            _ <- dbRef.inTransaction {
              appErrorQuery.save(dbApp.app.id, AppError(msg, ctx.now, ErrorAction.StartApp, ErrorSource.App, None)) >>
                appQuery.updateStatus(dbApp.app.id, AppStatus.Stopping)
            }
            _ <- stopAndPollApp(StopAppParams.fromStartAppParams(params))
          } yield ()
        } else {
          for {
            startTime <- F.realTimeInstant
            // The app is Running at this point and can be used
            _ <- appQuery.updateStatus(dbApp.app.id, AppStatus.Running).transaction
            _ <- appUsageQuery.recordStart(dbApp.app.id, startTime)
            // If autoscaling should be enabled, enable it now. Galaxy can still be used while this is in progress
            _ <-
              if (dbApp.app.numOfReplicas.isEmpty && dbApp.nodepool.autoscalingEnabled) {
                dbApp.nodepool.autoscalingConfig.traverse_ { autoscalingConfig =>
                  nodepoolLock.withKeyLock(dbCluster.getClusterId) {
                    for {
                      op <- gkeService.setNodepoolAutoscaling(
                        nodepoolId = NodepoolId(dbCluster.getClusterId, dbApp.nodepool.nodepoolName),
                        NodePoolAutoscaling
                          .newBuilder()
                          .setEnabled(true)
                          .setMinNodeCount(autoscalingConfig.autoscalingMin.amount)
                          .setMaxNodeCount(autoscalingConfig.autoscalingMax.amount)
                          .build
                      )
                      _ <- F.sleep(config.monitorConfig.scalingUpNodepool.initialDelay)
                      lastOp <- gkeService
                        .pollOperation(
                          KubernetesOperationId(params.googleProject, dbCluster.location, op.getName),
                          config.monitorConfig.scalingUpNodepool.interval,
                          config.monitorConfig.scalingUpNodepool.maxAttempts
                        )
                        .compile
                        .lastOrError
                      _ <-
                        if (lastOp.isDone)
                          logger.info(ctx.loggingCtx)(
                            s"setNodepoolAutoscaling operation has finished for nodepool ${dbApp.nodepool.id}"
                          )
                        else
                          logger.error(ctx.loggingCtx)(
                            s"setNodepoolAutoscaling operation has failed or timed out for nodepool ${dbApp.nodepool.id}"
                          ) >>
                            F.raiseError[Unit](NodepoolStartException(dbApp.nodepool.id))
                    } yield ()
                  }
                }
              } else F.unit
          } yield ()
        }
    } yield ()

  private[leonardo] def getGalaxyPostgresDisk(diskName: DiskName,
                                              namespaceName: NamespaceName,
                                              project: GoogleProject,
                                              zone: ZoneName
  )(implicit traceId: Ask[F, AppContext]): F[Option[Disk]] =
    for {
      postgresDiskOpt <- googleDiskService
        .getDisk(
          project,
          zone,
          getGalaxyPostgresDiskName(diskName, config.galaxyDiskConfig.postgresDiskNameSuffix)
        )
      res <- postgresDiskOpt match {
        case Some(disk) => F.pure(Some(disk))
        case None =>
          googleDiskService.getDisk(
            project,
            zone,
            getOldStyleGalaxyPostgresDiskName(namespaceName, config.galaxyDiskConfig.postgresDiskNameSuffix)
          )
      }
    } yield res

  private[util] def installNginx(dbCluster: KubernetesCluster, googleCluster: Cluster)(implicit
    ev: Ask[F, AppContext]
  ): F[IP] =
    for {
      ctx <- ev.ask

      _ <- logger.info(ctx.loggingCtx)(
        s"Installing ingress helm chart ${config.ingressConfig.chart} in cluster ${dbCluster.getClusterId.toString}"
      )

      helmAuthContext <- getHelmAuthContext(googleCluster, dbCluster, config.ingressConfig.namespace)

      // Invoke helm
      _ <- helmClient
        .installChart(
          config.ingressConfig.release,
          config.ingressConfig.chartName,
          config.ingressConfig.chartVersion,
          org.broadinstitute.dsp.Values(config.ingressConfig.values.map(_.value).mkString(",")),
          true
        )
        .run(helmAuthContext)

      // Monitor nginx until public IP is accessible
      loadBalancerIpOpt <- streamFUntilDone(
        kubeService.getServiceExternalIp(dbCluster.getClusterId,
                                         KubernetesNamespace(config.ingressConfig.namespace),
                                         config.ingressConfig.loadBalancerService
        ),
        config.monitorConfig.createIngress.maxAttempts,
        config.monitorConfig.createIngress.interval
      ).compile.lastOrError

      loadBalancerIp <- F.fromOption(
        loadBalancerIpOpt,
        ClusterCreationException(
          ctx.traceId,
          s"Load balancer IP did not become available after ${config.monitorConfig.createIngress.totalDuration} in cluster ${dbCluster.getClusterId.toString} | trace id: ${ctx.traceId}"
        )
      )

      _ <- logger.info(ctx.loggingCtx)(
        s"Successfully obtained public IP ${loadBalancerIp.asString} for cluster ${dbCluster.getClusterId.toString}"
      )
    } yield loadBalancerIp

  private[util] def installGalaxy(helmAuthContext: AuthContext,
                                  appName: AppName,
                                  release: Release,
                                  chart: Chart,
                                  dbCluster: KubernetesCluster,
                                  nodepoolName: NodepoolName,
                                  namespaceName: NamespaceName,
                                  userEmail: WorkbenchEmail,
                                  customEnvironmentVariables: Map[String, String],
                                  kubernetesServiceAccount: ServiceAccountName,
                                  nfsDisk: PersistentDisk,
                                  machineType: AppMachineType,
                                  galaxyRestore: Option[AppRestore.GalaxyRestore]
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask

      _ <- logger.info(ctx.loggingCtx)(
        s"Installing helm chart $chart for app ${appName.value} in cluster ${dbCluster.getClusterId.toString}"
      )
      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(nfsDisk.cloudContext),
        new RuntimeException("this should never happen. Galaxy disk's cloud context should be a google project")
      )
      postgresDiskNameOpt <- for {
        disk <- getGalaxyPostgresDisk(nfsDisk.name, namespaceName, googleProject, nfsDisk.zone)
      } yield disk.map(x => DiskName(x.getName))

      postgresDiskName <- F.fromOption(
        postgresDiskNameOpt,
        AppCreationException(s"No postgres disk found in google for app ${appName.value} ", traceId = Some(ctx.traceId))
      )

      chartValues = buildGalaxyChartOverrideValuesString(
        config,
        appName,
        release,
        dbCluster,
        nodepoolName,
        userEmail,
        customEnvironmentVariables,
        kubernetesServiceAccount,
        namespaceName,
        nfsDisk,
        postgresDiskName,
        machineType,
        galaxyRestore
      )

      _ <- logger.info(ctx.loggingCtx)(
        s"Chart override values are: ${chartValues.map(s =>
            if (s.contains("galaxyDatabasePassword")) "persistence.postgres.galaxyDatabasePassword=<redacted>"
            else s
          )}"
      )

      // Invoke helm
      helmInstall = helmClient
        .installChart(
          release,
          chart.name,
          chart.version,
          org.broadinstitute.dsp.Values(chartValues.mkString(",")),
          false
        )
        .run(helmAuthContext)

      // Currently we always retry.
      // The main failure mode here is helm install, which does not have easily interpretable error codes
      retryConfig = RetryPredicates.retryAllConfig
      _ <- tracedRetryF(retryConfig)(
        helmInstall,
        s"helm install for app ${appName.value} in project ${dbCluster.cloudContext.asString}"
      ).compile.lastOrError

      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(dbCluster.cloudContext),
        new RuntimeException("trying to create an azure runtime in GKEInterpreter. This should never happen")
      )
      // Poll galaxy until it starts up
      // TODO potentially add other status checks for pod readiness, beyond just HTTP polling the galaxy-web service
      // Wait a bit before starting polling for the app status check as the certificates might not be quite ready yet
      // This seems to only impact galaxy, See https://broadworkbench.atlassian.net/browse/IA-4551
      _ <- F.sleep(60 seconds)
      isDone <- streamFUntilDone(
        appDao.isProxyAvailable(googleProject, appName, ServiceName("galaxy")),
        config.monitorConfig.createApp.maxAttempts,
        config.monitorConfig.createApp.interval
      ).interruptAfter(config.monitorConfig.createApp.interruptAfter).compile.lastOrError

      _ <-
        if (!isDone) {
          val msg =
            s"Galaxy installation has failed or timed out for app ${appName.value} in cluster ${dbCluster.getClusterId.toString}"
          logger.error(ctx.loggingCtx)(msg) >>
            F.raiseError[Unit](AppCreationException(msg))
        } else F.unit

    } yield ()

  private[util] def installCromwellApp(
    helmAuthContext: AuthContext,
    appName: AppName,
    release: Release,
    cluster: KubernetesCluster,
    nodepoolName: Option[NodepoolName],
    namespaceName: NamespaceName,
    disk: PersistentDisk,
    ksaName: ServiceAccountName,
    gsa: WorkbenchEmail,
    customEnvironmentVariables: Map[String, String]
  )(implicit ev: Ask[F, AppContext]): F[Unit] = {
    // TODO: Use the chart from the database instead of re-looking it up in config:
    val chart = config.cromwellAppConfig.chart

    for {
      ctx <- ev.ask

      _ <- logger.info(ctx.loggingCtx)(
        s"Installing helm chart for Cromwell app ${appName.value} in cluster ${cluster.getClusterId.toString}"
      )

      chartValues = buildCromwellAppChartOverrideValuesString(config,
                                                              appName,
                                                              cluster,
                                                              nodepoolName,
                                                              namespaceName,
                                                              disk,
                                                              ksaName,
                                                              gsa,
                                                              customEnvironmentVariables
      )
      _ <- logger.info(ctx.loggingCtx)(s"Chart override values are: $chartValues")

      // Invoke helm
      helmInstall = helmClient
        .installChart(
          release,
          chart.name,
          chart.version,
          org.broadinstitute.dsp.Values(chartValues.mkString(",")),
          false
        )
        .run(helmAuthContext)

      // Currently we always retry.
      // The main failure mode here is helm install, which does not have easily interpretable error codes
      retryConfig = RetryPredicates.retryAllConfig
      _ <- tracedRetryF(retryConfig)(
        helmInstall,
        s"helm install for CROMWELL app ${appName.value} in project ${cluster.cloudContext.asString}"
      ).compile.lastOrError

      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(cluster.cloudContext),
        new RuntimeException("trying to create an azure runtime in GKEInterpreter. This should never happen")
      )
      // Poll the app until it starts up
      last <- streamFUntilDone(
        config.cromwellAppConfig.services
          .map(_.name)
          .traverse(s => appDao.isProxyAvailable(googleProject, appName, s)),
        config.monitorConfig.createApp.maxAttempts,
        config.monitorConfig.createApp.interval
      ).interruptAfter(config.monitorConfig.createApp.interruptAfter).compile.lastOrError

      _ <-
        if (!last.isDone) {
          val msg =
            s"Cromwell app installation has failed or timed out for app ${appName.value} in cluster ${cluster.getClusterId.toString}"
          logger.error(ctx.loggingCtx)(msg) >>
            F.raiseError[Unit](AppCreationException(msg))
        } else F.unit

    } yield ()
  }

  private[util] def installAllowedApp(
    helmAuthContext: AuthContext,
    appId: AppId,
    appName: AppName,
    release: Release,
    chart: Chart,
    cluster: KubernetesCluster,
    nodepoolName: Option[NodepoolName],
    namespaceName: NamespaceName,
    disk: PersistentDisk,
    ksaName: ServiceAccountName,
    gsa: WorkbenchEmail,
    userEmail: WorkbenchEmail,
    customEnvironmentVariables: Map[String, String],
    autopilot: Option[Autopilot]
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask

      _ <- logger.info(ctx.loggingCtx)(
        s"Installing helm chart for Allowed app ${appName.value} in cluster ${cluster.getClusterId.toString}"
      )

      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(cluster.cloudContext),
        new RuntimeException("trying to create an azure runtime in GKEInterpreter. This should never happen")
      )

      // Create the staging bucket to be used by Welder
      stagingBucketName = buildAppStagingBucketName(disk.name)

      _ <- bucketHelper
        .createStagingBucket(userEmail, googleProject, stagingBucketName, gsa)
        .compile
        .drain

      allowedChart <- F.fromOption(
        AllowedChartName.fromChartName(chart.name),
        new RuntimeException(s"invalid chart name for ALLOWED app: ${chart.name}")
      )

      chartValues = buildAllowedAppChartOverrideValuesString(config,
                                                             allowedChart,
                                                             appName,
                                                             cluster,
                                                             nodepoolName,
                                                             namespaceName,
                                                             disk,
                                                             ksaName,
                                                             userEmail,
                                                             stagingBucketName,
                                                             customEnvironmentVariables,
                                                             autopilot
      )
      _ <- logger.info(ctx.loggingCtx)(s"Chart override values are: $chartValues")

      // Invoke helm
      helmInstall = helmClient
        .installChart(
          release,
          chart.name,
          chart.version,
          org.broadinstitute.dsp.Values(chartValues.mkString(",")),
          false
        )
        .run(helmAuthContext)

      // Currently we always retry.
      // The main failure mode here is helm install, which does not have easily interpretable error codes
      retryConfig = RetryPredicates.retryAllConfig
      _ <- tracedRetryF(retryConfig)(
        helmInstall,
        s"helm install for ALLOWED app ${appName.value} in project ${cluster.cloudContext.asString}"
      ).compile.lastOrError

      // Poll the app until it starts up
      last <- streamFUntilDone(
        config.allowedAppConfig.services
          .map(_.name)
          .traverse(s => appDao.isProxyAvailable(googleProject, appName, s)),
        config.monitorConfig.createApp.maxAttempts,
        config.monitorConfig.createApp.interval
      ).interruptAfter(config.monitorConfig.createApp.interruptAfter).compile.lastOrError

      _ <-
        if (!last.isDone) {
          val msg =
            s"AoU app installation has failed or timed out for app ${appName.value} in cluster ${cluster.getClusterId.toString}"
          logger.error(ctx.loggingCtx)(msg) >>
            F.raiseError[Unit](AppCreationException(msg))
        } else F.unit

    } yield ()

  private[util] def installCustomApp(appId: AppId,
                                     appName: AppName,
                                     release: Release,
                                     dbCluster: KubernetesCluster,
                                     googleCluster: Cluster,
                                     nodepoolName: Option[NodepoolName],
                                     namespaceName: NamespaceName,
                                     disk: PersistentDisk,
                                     descriptorOpt: Option[Uri],
                                     extraArgs: List[String],
                                     ksaName: ServiceAccountName,
                                     customEnvironmentVariables: Map[String, String]
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask

      _ <- logger.info(ctx.loggingCtx)(
        s"Installing helm chart ${config.customAppConfig.chart} for custom app ${appName.value} in cluster ${dbCluster.getClusterId.toString}"
      )

      desc <- F.fromOption(descriptorOpt, AppRequiresDescriptorException(appId))

      _ <- logger.info(ctx.loggingCtx)(
        s"about to process descriptor for app ${appName.value} in cluster ${dbCluster.getClusterId.toString}"
      )

      descriptor <- appDescriptorDAO.getDescriptor(desc).adaptError { case e =>
        AppCreationException(
          s"Failed to process descriptor: $desc. Please ensure it is a valid descriptor, and that the remote file is valid yaml following the schema detailed here: https://github.com/DataBiosphere/terra-app#app-schema. \n\tOriginal message: ${e.getMessage}"
        )
      }

      _ <- logger.info(ctx.loggingCtx)(
        s"Finished processing descriptor for app ${appName.value} in cluster ${dbCluster.getClusterId.toString}"
      )

      // TODO we're only handling 1 service for now
      (serviceName, serviceConfig) = descriptor.services.head

      // Save the service in the DB
      _ <- serviceQuery
        .saveForApp(
          appId,
          KubernetesService(
            ServiceId(-1),
            ServiceConfig(ServiceName(serviceName),
                          org.broadinstitute.dsde.workbench.leonardo.KubernetesServiceKindName("ClusterIP")
            )
          )
        )
        .transaction

      helmAuthContext <- getHelmAuthContext(googleCluster, dbCluster, namespaceName)

      chartValues = buildCustomChartOverrideValuesString(
        config,
        appName,
        release,
        nodepoolName,
        serviceName,
        dbCluster,
        namespaceName,
        serviceConfig,
        extraArgs,
        disk,
        ksaName,
        serviceConfig.environment ++ customEnvironmentVariables
      )

      _ <- logger.info(ctx.loggingCtx)(
        s"Chart override values are: ${chartValues} | trace id: ${ctx.traceId}"
      )

      // Invoke helm
      helmInstall = helmClient
        .installChart(
          release,
          config.customAppConfig.chartName, // TODO: Use the chart from the database instead of re-looking it up in config?
          config.customAppConfig.chartVersion,
          org.broadinstitute.dsp.Values(chartValues)
        )
        .run(helmAuthContext)

      // Currently we always retry.
      // The main failure mode here is helm install, which does not have easily interpretable error codes
      retryConfig = RetryPredicates.retryAllConfig

      _ <- tracedRetryF(retryConfig)(
        helmInstall,
        s"helm install for app ${appName.value} in project ${dbCluster.cloudContext.asString}"
      ).compile.lastOrError
      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(dbCluster.cloudContext),
        new RuntimeException("trying to create an azure runtime in GKEInterpreter. This should never happen")
      )
      // Poll app until it starts up
      last <- streamFUntilDone(
        descriptor.services.keys.toList.traverse(s => appDao.isProxyAvailable(googleProject, appName, ServiceName(s))),
        config.monitorConfig.createApp.maxAttempts,
        config.monitorConfig.createApp.interval
      ).interruptAfter(config.monitorConfig.createApp.interruptAfter).compile.lastOrError

      _ <-
        if (!last.isDone) {
          val msg =
            s"App installation has failed or timed out for app ${appName.value} in cluster ${dbCluster.getClusterId.toString} | trace id: ${ctx.traceId}"
          logger.error(msg) >>
            F.raiseError[Unit](AppCreationException(msg))
        } else F.unit

    } yield ()

  private[util] def getHelmAuthContext(
    googleCluster: Cluster,
    dbCluster: KubernetesCluster,
    namespaceName: NamespaceName
  )(implicit ev: Ask[F, AppContext]): F[AuthContext] =
    for {
      ctx <- ev.ask

      // The helm client requires a Google access token
      _ <- F.delay(credentials.refreshIfExpired())

      // Don't use AppContext.now for the tmp file name because we want it to be unique
      // for each helm invocation
      now <- nowInstant

      // The helm client requires the ca cert passed as a file - hence writing a temp file before helm invocation.
      caCertFile <- writeTempFile(s"gke_ca_cert_${dbCluster.id}_${now.toEpochMilli}",
                                  Base64.getDecoder.decode(googleCluster.getMasterAuth.getClusterCaCertificate)
      )

      helmAuthContext = AuthContext(
        org.broadinstitute.dsp.Namespace(namespaceName.value),
        org.broadinstitute.dsp.KubeToken(credentials.getAccessToken.getTokenValue),
        org.broadinstitute.dsp.KubeApiServer("https://" + googleCluster.getEndpoint),
        org.broadinstitute.dsp.CaCertFile(caCertFile.toAbsolutePath)
      )

      _ <- logger.info(ctx.loggingCtx)(
        s"Helm auth context for cluster ${dbCluster.getClusterId.toString}: ${helmAuthContext
            .copy(kubeToken = org.broadinstitute.dsp.KubeToken("<redacted>"))}"
      )

    } yield helmAuthContext

  private[util] def getNodepoolServiceAccount(projectLabels: Option[Map[String, String]],
                                              googleProject: GoogleProject
  ): Option[String] =
    projectLabels.flatMap { x =>
      x.get("gke-default-sa").map(v => s"${v}@${googleProject.value}.iam.gserviceaccount.com")
    }

  private[util] def buildGoogleNodepool(
    nodepool: Nodepool,
    googleProject: GoogleProject,
    projectLabels: Option[Map[String, String]]
  ): com.google.container.v1.NodePool = {
    val serviceAccount = getNodepoolServiceAccount(projectLabels, googleProject)

    val nodepoolBuilder = NodePool
      .newBuilder()
      .setInitialNodeCount(nodepool.numNodes.amount)
      .setName(nodepool.nodepoolName.value)
      .setManagement(
        NodeManagement
          .newBuilder()
          .setAutoUpgrade(true)
          .setAutoRepair(true)
      )

    val nodepoolBuilderWithSa = serviceAccount match {
      case Some(sa) =>
        nodepoolBuilder.setConfig(
          NodeConfig
            .newBuilder()
            .addTags(config.vpcNetworkTag.value)
            .setMachineType(nodepool.machineType.value)
            .setServiceAccount(sa)
        )
      case _ =>
        nodepoolBuilder.setConfig(
          NodeConfig
            .newBuilder()
            .setMachineType(nodepool.machineType.value)
            .addTags(config.vpcNetworkTag.value)
        )
    }

    val builderWithAutoscaling = nodepool.autoscalingConfig.fold(nodepoolBuilderWithSa)(config =>
      nodepool.autoscalingEnabled match {
        case true =>
          nodepoolBuilderWithSa.setAutoscaling(
            NodePoolAutoscaling
              .newBuilder()
              .setEnabled(true)
              .setMinNodeCount(config.autoscalingMin.amount)
              .setMaxNodeCount(config.autoscalingMax.amount)
          )
        case false => nodepoolBuilderWithSa
      }
    )

    builderWithAutoscaling.build()
  }

  private[util] def buildLegacyGoogleNodepool(
    nodepool: Nodepool,
    googleProject: GoogleProject,
    projectLabels: Option[Map[String, String]]
  ): com.google.api.services.container.model.NodePool = {
    val serviceAccount = getNodepoolServiceAccount(projectLabels, googleProject)

    val legacyGoogleNodepool = new com.google.api.services.container.model.NodePool()
      .setInitialNodeCount(nodepool.numNodes.amount)
      .setName(nodepool.nodepoolName.value)
      .setManagement(
        new com.google.api.services.container.model.NodeManagement().setAutoUpgrade(true).setAutoRepair(true)
      )

    val legacyGoogleNodepoolWithSa = serviceAccount match {
      case Some(sa) =>
        legacyGoogleNodepool.setConfig(
          new com.google.api.services.container.model.NodeConfig()
            .setMachineType(nodepool.machineType.value)
            .setTags(List(config.vpcNetworkTag.value).asJava)
            .setServiceAccount(sa)
        )
      case _ =>
        legacyGoogleNodepool.setConfig(
          new com.google.api.services.container.model.NodeConfig()
            .setMachineType(nodepool.machineType.value)
            .setTags(List(config.vpcNetworkTag.value).asJava)
        )
    }

    nodepool.autoscalingConfig.fold(legacyGoogleNodepoolWithSa)(config =>
      nodepool.autoscalingEnabled match {
        case true =>
          legacyGoogleNodepoolWithSa.setAutoscaling(
            new com.google.api.services.container.model.NodePoolAutoscaling()
              .setEnabled(true)
              .setMinNodeCount(config.autoscalingMin.amount)
              .setMaxNodeCount(config.autoscalingMax.amount)
          )
        case false => legacyGoogleNodepoolWithSa
      }
    )
  }

  private def scaleDownNodepool(appId: AppId,
                                googleProject: GoogleProject,
                                nodepool: Nodepool,
                                dbCluster: KubernetesCluster
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] = for {
    ctx <- ev.ask
    _ <- nodepoolQuery.updateStatus(nodepool.id, NodepoolStatus.Provisioning).transaction
    nodepoolId = NodepoolId(dbCluster.getClusterId, nodepool.nodepoolName)
    // If autoscaling is enabled, disable it first
    _ <-
      if (nodepool.autoscalingEnabled) {
        nodepoolLock.withKeyLock(dbCluster.getClusterId) {
          for {
            opOrError <- gkeService
              .setNodepoolAutoscaling(
                nodepoolId,
                NodePoolAutoscaling.newBuilder().setEnabled(false).build()
              )
              .attempt
            _ <- opOrError match {
              case Left(e: com.google.api.gax.rpc.NotFoundException) =>
                // Mark the app as `DELETED` instead of bubbling this error up to generic error handler
                for {
                  _ <- appErrorQuery
                    .save(
                      appId,
                      AppError(e.getMessage, ctx.now, ErrorAction.StopApp, ErrorSource.App, None, Some(ctx.traceId))
                    )
                    .transaction
                  _ <- appQuery.markAsDeleted(appId, ctx.now).transaction
                  _ <-
                    nodepoolQuery.markAsDeleted(nodepool.id, ctx.now).transaction
                } yield ()
              case Left(e) => F.raiseError(e)
              case Right(op) =>
                for {
                  _ <- F.sleep(config.monitorConfig.scalingDownNodepool.initialDelay)
                  lastOp <- gkeService
                    .pollOperation(
                      KubernetesOperationId(googleProject, dbCluster.location, op.getName),
                      config.monitorConfig.scalingDownNodepool.interval,
                      config.monitorConfig.scalingDownNodepool.maxAttempts
                    )
                    .compile
                    .lastOrError
                  _ <-
                    if (lastOp.isDone)
                      logger.info(ctx.loggingCtx)(
                        s"setNodepoolAutoscaling operation has finished for nodepool ${nodepool.id}"
                      )
                    else
                      logger.error(ctx.loggingCtx)(
                        s"setNodepoolAutoscaling operation has failed or timed out for nodepool ${nodepool.id}"
                      ) >>
                        F.raiseError[Unit](NodepoolStopException(nodepool.id))
                } yield ()
            }
          } yield ()
        }
      } else F.unit
    _ <- nodepoolLock.withKeyLock(dbCluster.getClusterId) {
      for {
        op <- gkeService.setNodepoolSize(nodepoolId, 0)
        _ <- F.sleep(config.monitorConfig.scalingDownNodepool.initialDelay)
        lastOp <- gkeService
          .pollOperation(
            KubernetesOperationId(googleProject, dbCluster.location, op.getName),
            config.monitorConfig.scalingDownNodepool.interval,
            config.monitorConfig.scalingDownNodepool.maxAttempts
          )
          .compile
          .lastOrError
        _ <-
          if (lastOp.isDone)
            logger.info(ctx.loggingCtx)(
              s"setNodepoolSize operation has finished for nodepool ${nodepool.id}"
            )
          else
            logger.error(ctx.loggingCtx)(
              s"setNodepoolSize operation has failed or timed out for nodepool ${nodepool.id}"
            ) >>
              F.raiseError[Unit](NodepoolStopException(nodepool.id))
      } yield ()
    }
    _ <- nodepoolQuery.updateStatus(nodepool.id, NodepoolStatus.Running).transaction
  } yield ()

  private def scaleUpNodepool(googleProject: GoogleProject, nodepool: Nodepool, dbCluster: KubernetesCluster)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] = for {
    ctx <- ev.ask
    _ <- nodepoolQuery.updateStatus(nodepool.id, NodepoolStatus.Provisioning).transaction
    nodepoolId = NodepoolId(dbCluster.getClusterId, nodepool.nodepoolName)
    // First scale the node pool to > 0 nodes
    _ <- nodepoolLock.withKeyLock(dbCluster.getClusterId) {
      for {
        op <- gkeService.setNodepoolSize(
          nodepoolId,
          nodepool.numNodes.amount
        )
        _ <- F.sleep(config.monitorConfig.scalingUpNodepool.initialDelay)
        lastOp <- gkeService
          .pollOperation(
            KubernetesOperationId(googleProject, dbCluster.location, op.getName),
            config.monitorConfig.scalingUpNodepool.interval,
            config.monitorConfig.scalingUpNodepool.maxAttempts
          )
          .compile
          .lastOrError
        _ <-
          if (lastOp.isDone)
            logger.info(ctx.loggingCtx)(
              s"setNodepoolSize operation has finished for nodepool ${nodepool.id}"
            )
          else
            logger.error(ctx.loggingCtx)(
              s"setNodepoolSize operation has failed or timed out for nodepool ${nodepool.id}"
            ) >>
              F.raiseError[Unit](NodepoolStartException(nodepool.id))
      } yield ()
    }

    // Finally update the nodepool status to Running
    _ <- nodepoolQuery.updateStatus(nodepool.id, NodepoolStatus.Running).transaction
  } yield ()

  private def getTerraAppSetupChartReleaseName(appReleaseName: Release): Release =
    Release(s"${appReleaseName.asString}-setup-rls")

  private[util] def isPodDone(pod: KubernetesPodStatus): Boolean =
    pod.podStatus == PodStatus.Failed || pod.podStatus == PodStatus.Succeeded
}

sealed trait AppProcessingException extends Exception {
  def getMessage: String
}

final case class ClusterCreationException(traceId: TraceId, message: String) extends AppProcessingException {
  override def getMessage: String = message
}

final case class ClusterDeletionException(clusterId: KubernetesClusterLeoId) extends AppProcessingException {
  override def getMessage: String = s"Failed to poll cluster deletion operation to completion for cluster $clusterId"
}

final case class NodepoolCreationException(nodepoolId: NodepoolLeoId) extends AppProcessingException {
  override def getMessage: String = s"Failed to poll nodepool creation operation to completion for nodepool $nodepoolId"
}

final case class NodepoolDeletionException(nodepoolId: NodepoolLeoId) extends AppProcessingException {
  override def getMessage: String = s"Failed to poll nodepool deletion operation to completion for nodepool $nodepoolId"
}

final case class NodepoolStopException(nodepoolId: NodepoolLeoId) extends AppProcessingException {
  override def getMessage: String = s"Failed to poll nodepool stop operation to completion for nodepool $nodepoolId"
}

final case class NodepoolStartException(nodepoolId: NodepoolLeoId) extends AppProcessingException {
  override def getMessage: String = s"Failed to poll nodepool start operation to completion for nodepool $nodepoolId"
}

final case class AppCreationException(message: String, traceId: Option[TraceId] = None) extends AppProcessingException {
  override def getMessage: String = message
}

final case class AppRequiresDescriptorException(appId: AppId) extends AppProcessingException {
  override def getMessage: String =
    s"Cannot processing creation for custom app $appId because no descriptor was provided"
}

final case class AppDeletionException(message: String) extends AppProcessingException {
  override def getMessage: String = message
}

final case class AppStartException(message: String) extends AppProcessingException {
  override def getMessage: String = message
}

final case class AppUpdateException(message: String, traceId: Option[TraceId] = None) extends AppProcessingException {
  override def getMessage: String = message
}

// This should only be used in exactly one place, when polling after an app update call. Using this will signal to pubsub processing to transition app to error state
// Any other exception besides a `HelmException` during app upgrades will result in an error being saved to the db, but NOT an `ERROR` state app to preserve usage
final case class AppUpdatePollingException(message: String, traceId: Option[TraceId] = None)
    extends AppProcessingException {
  override def getMessage: String = message
}

final case class DiskNotFoundForAppException(appId: AppId, traceId: TraceId)
    extends LeoException(s"No persistent disk found for ${appId}", traceId = Some(traceId))

final case class DeleteNodepoolResult(nodepoolId: NodepoolLeoId,
                                      operation: com.google.container.v1.Operation,
                                      getAppResult: GetAppResult
)

final case class GKEInterpreterConfig(leoUrlBase: URL,
                                      vpcNetworkTag: NetworkTag,
                                      terraAppSetupChartConfig: TerraAppSetupChartConfig,
                                      ingressConfig: KubernetesIngressConfig,
                                      galaxyAppConfig: GalaxyAppConfig,
                                      cromwellAppConfig: CromwellAppConfig,
                                      customAppConfig: CustomAppConfig,
                                      allowedAppConfig: AllowedAppConfig,
                                      monitorConfig: AppMonitorConfig,
                                      clusterConfig: KubernetesClusterConfig,
                                      proxyConfig: ProxyConfig,
                                      galaxyDiskConfig: GalaxyDiskConfig
)

final case class TerraAppSetupChartConfig(
  chartName: ChartName,
  chartVersion: ChartVersion
)
