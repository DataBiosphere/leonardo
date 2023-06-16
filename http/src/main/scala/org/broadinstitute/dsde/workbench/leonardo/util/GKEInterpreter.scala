package org.broadinstitute.dsde.workbench
package leonardo
package util

import _root_.org.typelevel.log4cats.StructuredLogger
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.compute.v1.Disk
import com.google.container.v1._
import org.broadinstitute.dsde.workbench.DoneCheckableInstances._
import org.broadinstitute.dsde.workbench.DoneCheckableSyntax._
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
import org.broadinstitute.dsde.workbench.leonardo.AppRestore.{CromwellRestore, GalaxyRestore, RStudioRestore}
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.dao.{AppDAO, AppDescriptorDAO, CustomAppService}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppNotFoundException
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError.PubsubKubernetesError
import org.broadinstitute.dsde.workbench.leonardo.util.GKEAlgebra._
import org.broadinstitute.dsde.workbench.model.google.{generateUniqueBucketName, GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{IP, TraceId, WorkbenchEmail}
import org.broadinstitute.dsp._
import org.http4s.Uri

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
)(implicit val executionContext: ExecutionContext, logger: StructuredLogger[F], dbRef: DbReference[F], F: Async[F])
    extends GKEAlgebra[F] {
  // DoneCheckable instances
  implicit private def optionDoneCheckable[A]: DoneCheckable[Option[A]] = (a: Option[A]) => a.isDefined
  implicit private def booleanDoneCheckable: DoneCheckable[Boolean] = identity[Boolean]
  implicit private def podDoneCheckable: DoneCheckable[List[KubernetesPodStatus]] =
    (ps: List[KubernetesPodStatus]) => ps.forall(isPodDone)
  implicit private def listDoneCheckable[A: DoneCheckable]: DoneCheckable[List[A]] = as => as.forall(_.isDone)

  override def createCluster(params: CreateClusterParams)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[CreateClusterResult]] =
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
      nodepools = dbCluster.nodepools
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

      legacyCreateClusterRec = new com.google.api.services.container.model.Cluster()
        .setName(dbCluster.clusterName.value)
        .setInitialClusterVersion(config.clusterConfig.version.value)
        .setNodePools(nodepools.asJava)
        .setLegacyAbac(new com.google.api.services.container.model.LegacyAbac().setEnabled(false))
        .setNetwork(kubeNetwork.idString)
        .setSubnetwork(kubeSubNetwork.idString)
        .setResourceLabels(Map("leonardo" -> "true").asJava)
        .setNetworkPolicy(
          new com.google.api.services.container.model.NetworkPolicy().setEnabled(true)
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

      // Submit request to GKE
      req = KubernetesCreateClusterRequest(googleProject, dbCluster.location, legacyCreateClusterRec)
      // the Operation will be none if we get a 409, indicating we have already created this cluster
      operationOpt <- gkeService.createCluster(req)

    } yield operationOpt.map(op =>
      CreateClusterResult(KubernetesOperationId(googleProject, dbCluster.location, op.getName),
                          kubeNetwork,
                          kubeSubNetwork
      )
    )

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
                              new RegionNotSupportedException(dbCluster.region, ctx.traceId)
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
      namespaceName = app.appResources.namespace.name
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
      ksaName: ServiceAccountName <- F.fromOption(
        app.appResources.kubernetesServiceAccountName,
        AppCreationException(
          s"Kubernetes Service Account not found in DB for app ${app.appName.value} | trace id: ${ctx.traceId}"
        )
      )
      gsa = dbApp.app.googleServiceAccount

      // Resolve the cluster in Google
      googleClusterOpt <- gkeService.getCluster(gkeClusterId)
      googleCluster <- F.fromOption(
        googleClusterOpt,
        ClusterCreationException(ctx.traceId,
                                 s"Cluster not found in Google: ${gkeClusterId} | trace id: ${ctx.traceId}"
        )
      )

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
      galaxyRestore: Option[GalaxyRestore] = appRestore.flatMap {
        case a: GalaxyRestore   => Some(a)
        case _: CromwellRestore => None
        case _: RStudioRestore  => None
      }

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
              dbApp.nodepool.nodepoolName,
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
            dbApp.nodepool.nodepoolName,
            namespaceName,
            nfsDisk,
            ksaName,
            gsa,
            app.customEnvironmentVariables
          )
        case AppType.RStudio =>
          installRStudioApp(
            helmAuthContext,
            app.appName,
            app.release,
            dbCluster,
            dbApp.nodepool.nodepoolName,
            namespaceName,
            nfsDisk,
            ksaName,
            gsa,
            app.auditInfo.creator
          )
        case AppType.Custom =>
          installCustomApp(
            app.id,
            app.appName,
            app.release,
            dbCluster,
            googleCluster,
            dbApp.nodepool.nodepoolName,
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
                                                             KubernetesNamespace(app.appResources.namespace.name)
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
                  val galaxyDiskRestore = GalaxyRestore(
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
        case AppType.RStudio  => persistentDiskQuery.updateLastUsedBy(diskId, app.id).transaction
        case AppType.Custom   => F.unit
        case _ =>
          F.raiseError(AppCreationException(s"App type ${app.appType} not supported on GCP"))
      }

      _ <- appQuery.updateStatus(params.appId, AppStatus.Running).transaction
    } yield ()

  override def updateAndPollApp(params: UpdateAppParams)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask

      googleProject <- F.fromOption(
        params.googleProject,
        AppUpdateException(
          s"${params.appName} must have a google project in the GCP cloud context | trace id: ${ctx.traceId}"
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
      _ <- logger.info(ctx.loggingCtx)(s"Updating app $params.appName in project $googleProject")

      // Grab all of the values that we need to resend to the helm update command for each app
      dbCluster = dbApp.cluster
      nodepoolName = dbApp.nodepool.nodepoolName
      machineTypeName = dbApp.nodepool.machineType
      gkeClusterId = dbCluster.getClusterId

      app = dbApp.app
      namespaceName = app.appResources.namespace.name
      gsa = app.googleServiceAccount
      nfsDisk <- F.fromOption(
        dbApp.app.appResources.disk,
        AppUpdateException(s"NFS disk not found in DB for app ${app.appName.value} | trace id: ${ctx.traceId}")
      )
      ksaName <- F.fromOption(
        app.appResources.kubernetesServiceAccountName,
        AppUpdateException(
          s"Kubernetes Service Account not found in DB for app ${app.appName.value} | trace id: ${ctx.traceId}"
        )
      )
      userEmail = app.auditInfo.creator
      stagingBucketName = generateUniqueBucketName("leostaging-" + params.appName.value)

      // Resolve the cluster in Google
      googleClusterOpt <- gkeService.getCluster(gkeClusterId)
      googleCluster <- F.fromOption(
        googleClusterOpt,
        AppUpdateException(s"Cluster not found in Google: ${gkeClusterId} | trace id: ${ctx.traceId}")
      )

      (chartOverrideValues, appOk) <- app.appType match {
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
            galaxyRestore: Option[GalaxyRestore] = appRestore.flatMap {
              case a: GalaxyRestore   => Some(a)
              case _: CromwellRestore => None
              case _: RStudioRestore  => None
            }

            machineType <- computeService
              .getMachineType(googleProject,
                              ZoneName("us-central1-a"),
                              machineTypeName
              ) // TODO: if use non `us-central1-a` zone for galaxy, this needs to be udpated
              .flatMap(opt =>
                F.fromOption(
                  opt,
                  new AppUpdateException(s"Unknown machine type for ${machineTypeName.value}",
                                         traceId = Some(ctx.traceId)
                  )
                )
              )

            appMachineType = AppMachineType(machineType.getMemoryMb / 1024, machineType.getGuestCpus)

            chartValues = buildGalaxyChartOverrideValuesString(
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
              app.appName,
              dbCluster,
              nodepoolName,
              namespaceName,
              nfsDisk,
              ksaName,
              gsa,
              app.customEnvironmentVariables
            )

          } yield (chartValues.mkString(","), last)
        case AppType.RStudio =>
          for {

            // Create the throwaway staging bucket to be used by Welder
            _ <- bucketHelper
              .createStagingBucket(userEmail, googleProject, stagingBucketName, gsa)
              .compile
              .drain

            chartValues = buildRStudioAppChartOverrideValuesString(
              app.appName,
              dbCluster,
              nodepoolName,
              namespaceName,
              nfsDisk,
              ksaName,
              userEmail,
              stagingBucketName
            )

            last <- streamFUntilDone(
              config.rStudioAppConfig.services
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
                s"Failed to process descriptor: $desc. Please ensure it is a valid descriptor, and that the remote file is valid yaml following the schema detailed here: https://github.com/DataBiosphere/terra-app#app-schema. \n\tOriginal message: ${e.getMessage}"
              )
            }

            (serviceName, serviceConfig) = descriptor.services.head

            chartValues = buildCustomChartOverrideValuesString(
              params.appName,
              app.release,
              nodepoolName,
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
        case _ => F.raiseError(AppUpdateException(s"App type ${app.appType} not supported on GCP"))
      }

      // TODO Scale replicas up (1 -> 2)?

      // Authenticate helm client
      helmAuthContext <- getHelmAuthContext(googleCluster, dbCluster, namespaceName)

      // Upgrade app chart version and explicitly pass the values
      _ <- helmClient
        .upgradeChart(
          app.release,
          app.chart.name,
          params.appChartVersion,
          org.broadinstitute.dsp.Values(chartOverrideValues)
        )
        .run(helmAuthContext)

      // TODO Scale replicas down

      // Fail if apps are not live
      _ <-
        if (appOk)
          F.unit
        else
          F.raiseError[Unit](
            AppUpdateException(
              s"App ${params.appName.value} failed to update in cluster ${googleCluster} in cloud context ${CloudContext.Gcp(googleProject).asString}",
              Some(ctx.traceId)
            )
          )

      _ <- logger.info(
        s"Update app operation has finished for app ${app.appName.value} in cluster ${googleCluster}"
      )

      // Update app chart version in the DB
      _ <- appQuery.updateChart(app.id, Chart(app.chart.name, params.appChartVersion)).transaction

      _ <- logger.info(s"Done updating app $params.appName in workspace $params.workspaceId")
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
      namespaceName = app.appResources.namespace.name
      dbCluster = dbApp.cluster
      gkeClusterId = dbCluster.getClusterId

      _ <- logger.info(ctx.loggingCtx)(
        s"Beginning app deletion for app ${app.appName.value} in cluster ${gkeClusterId.toString}"
      )

      // Resolve the cluster in Google
      googleClusterOpt <- gkeService.getCluster(gkeClusterId)

      _ <- googleClusterOpt.traverse(googleCluster =>
        for {
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
      )

      // delete the namespace only after the helm uninstall completes
      _ <- kubeService.deleteNamespace(dbApp.cluster.getClusterId,
                                       KubernetesNamespace(dbApp.app.appResources.namespace.name)
      )

      fa = kubeService
        .namespaceExists(dbApp.cluster.getClusterId, KubernetesNamespace(dbApp.app.appResources.namespace.name))
        .map(!_) // mapping to inverse because booleanDoneCheckable defines `Done` when it becomes `true`...In this case, the namespace will exists for a while, and eventually becomes non-existent

      _ <- streamUntilDoneOrTimeout(fa, 30, 5 seconds, "delete namespace timed out")
      _ <- logger.info(ctx.loggingCtx)(
        s"Delete app operation has finished for app ${app.appName.value} in cluster ${gkeClusterId.toString}"
      )

      appRestore: Option[GalaxyRestore] = dbApp.app.appResources.disk.flatMap(_.appRestore).flatMap {
        case a: GalaxyRestore   => Some(a)
        case _: CromwellRestore => None
        case _: RStudioRestore  => None

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
      dbNodepool = dbApp.nodepool
      dbCluster = dbApp.cluster
      nodepoolId = NodepoolId(dbCluster.getClusterId, dbNodepool.nodepoolName)

      _ <- logger.info(ctx.loggingCtx)(
        s"Stopping app ${dbApp.app.appName.value} in cluster ${dbCluster.getClusterId.toString}"
      )

      _ <- nodepoolQuery.updateStatus(dbNodepool.id, NodepoolStatus.Provisioning).transaction

      // If autoscaling is enabled, disable it first
      _ <-
        if (dbNodepool.autoscalingEnabled) {
          nodepoolLock.withKeyLock(dbCluster.getClusterId) {
            for {
              op <- gkeService.setNodepoolAutoscaling(
                nodepoolId,
                NodePoolAutoscaling.newBuilder().setEnabled(false).build()
              )
              _ <- F.sleep(config.monitorConfig.scalingDownNodepool.initialDelay)
              lastOp <- gkeService
                .pollOperation(
                  KubernetesOperationId(params.googleProject, dbCluster.location, op.getName),
                  config.monitorConfig.scalingDownNodepool.interval,
                  config.monitorConfig.scalingDownNodepool.maxAttempts
                )
                .compile
                .lastOrError
              _ <-
                if (lastOp.isDone)
                  logger.info(ctx.loggingCtx)(
                    s"setNodepoolAutoscaling operation has finished for nodepool ${dbNodepool.id}"
                  )
                else
                  logger.error(ctx.loggingCtx)(
                    s"setNodepoolAutoscaling operation has failed or timed out for nodepool ${dbNodepool.id}"
                  ) >>
                    F.raiseError[Unit](NodepoolStopException(dbNodepool.id))
            } yield ()
          }
        } else F.unit

      // Scale the nodepool to zero nodes
      _ <- nodepoolLock.withKeyLock(dbCluster.getClusterId) {
        for {
          op <- gkeService.setNodepoolSize(nodepoolId, 0)
          _ <- F.sleep(config.monitorConfig.scalingDownNodepool.initialDelay)
          lastOp <- gkeService
            .pollOperation(
              KubernetesOperationId(params.googleProject, dbCluster.location, op.getName),
              config.monitorConfig.scalingDownNodepool.interval,
              config.monitorConfig.scalingDownNodepool.maxAttempts
            )
            .compile
            .lastOrError
          _ <-
            if (lastOp.isDone)
              logger.info(ctx.loggingCtx)(
                s"setNodepoolSize operation has finished for nodepool ${dbNodepool.id}"
              )
            else
              logger.error(ctx.loggingCtx)(
                s"setNodepoolSize operation has failed or timed out for nodepool ${dbNodepool.id}"
              ) >>
                F.raiseError[Unit](NodepoolStopException(dbNodepool.id))
        } yield ()
      }

      // Update nodepool status to Running and app status to Stopped
      _ <- dbRef.inTransaction {
        nodepoolQuery.updateStatus(dbNodepool.id, NodepoolStatus.Running) >>
          appQuery.updateStatus(params.appId, AppStatus.Stopped)
      }
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
      dbNodepool = dbApp.nodepool
      dbCluster = dbApp.cluster
      nodepoolId = NodepoolId(dbCluster.getClusterId, dbNodepool.nodepoolName)

      _ <- logger.info(ctx.loggingCtx)(
        s"Starting app ${dbApp.app.appName.value} in cluster ${dbCluster.getClusterId.toString}"
      )

      _ <- nodepoolQuery.updateStatus(dbNodepool.id, NodepoolStatus.Provisioning).transaction

      // First scale the node pool to > 0 nodes
      _ <- nodepoolLock.withKeyLock(dbCluster.getClusterId) {
        for {
          op <- gkeService.setNodepoolSize(
            nodepoolId,
            dbNodepool.numNodes.amount
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
                s"setNodepoolSize operation has finished for nodepool ${dbNodepool.id}"
              )
            else
              logger.error(ctx.loggingCtx)(
                s"setNodepoolSize operation has failed or timed out for nodepool ${dbNodepool.id}"
              ) >>
                F.raiseError[Unit](NodepoolStartException(dbNodepool.id))
        } yield ()
      }

      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(dbCluster.cloudContext),
        new RuntimeException("trying to create an azure runtime in GKEInterpreter. This should never happen")
      )

      isUp <- dbApp.app.appType match {
        case AppType.Galaxy =>
          streamFUntilDone(
            appDao.isProxyAvailable(googleProject, dbApp.app.appName, ServiceName("galaxy")),
            config.monitorConfig.startApp.maxAttempts,
            config.monitorConfig.startApp.interval
          ).interruptAfter(config.monitorConfig.startApp.interruptAfter).compile.lastOrError
        case AppType.Cromwell =>
          streamFUntilDone(
            config.cromwellAppConfig.services
              .map(_.name)
              .traverse(s => appDao.isProxyAvailable(googleProject, dbApp.app.appName, s)),
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
                appDao.isProxyAvailable(googleProject, dbApp.app.appName, ServiceName(s))
              ),
              config.monitorConfig.startApp.maxAttempts,
              config.monitorConfig.startApp.interval
            ).interruptAfter(config.monitorConfig.startApp.interruptAfter).compile.lastOrError
          } yield last.isDone
        case _ =>
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
            // The app is Running at this point and Galaxy can be used
            _ <- appQuery.updateStatus(params.appId, AppStatus.Running).transaction

            // If autoscaling should be enabled, enable it now. Galaxy can still be used while this is in progress
            _ <-
              if (dbNodepool.autoscalingEnabled) {
                dbNodepool.autoscalingConfig.traverse_ { autoscalingConfig =>
                  nodepoolLock.withKeyLock(dbCluster.getClusterId) {
                    for {
                      op <- gkeService.setNodepoolAutoscaling(
                        nodepoolId,
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
                            s"setNodepoolAutoscaling operation has finished for nodepool ${dbNodepool.id}"
                          )
                        else
                          logger.error(ctx.loggingCtx)(
                            s"setNodepoolAutoscaling operation has failed or timed out for nodepool ${dbNodepool.id}"
                          ) >>
                            F.raiseError[Unit](NodepoolStartException(dbNodepool.id))
                    } yield ()
                  }
                }
              } else F.unit

            // Finally update the nodepool status to Running
            _ <- nodepoolQuery.updateStatus(dbNodepool.id, NodepoolStatus.Running).transaction
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
                                  galaxyRestore: Option[GalaxyRestore]
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask

      _ <- logger.info(ctx.loggingCtx)(
        s"Installing helm chart ${config.galaxyAppConfig.chart} for app ${appName.value} in cluster ${dbCluster.getClusterId.toString}"
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
    nodepoolName: NodepoolName,
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

      chartValues = buildCromwellAppChartOverrideValuesString(appName,
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

  private[util] def installRStudioApp(
    helmAuthContext: AuthContext,
    appName: AppName,
    release: Release,
    cluster: KubernetesCluster,
    nodepoolName: NodepoolName,
    namespaceName: NamespaceName,
    disk: PersistentDisk,
    ksaName: ServiceAccountName,
    gsa: WorkbenchEmail,
    userEmail: WorkbenchEmail
  )(implicit ev: Ask[F, AppContext]): F[Unit] = {
    val chart = config.rStudioAppConfig.chart

    for {
      ctx <- ev.ask

      _ <- logger.info(ctx.loggingCtx)(
        s"Installing helm chart for RStudio app ${appName.value} in cluster ${cluster.getClusterId.toString}"
      )

      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(cluster.cloudContext),
        new RuntimeException("trying to create an azure runtime in GKEInterpreter. This should never happen")
      )

      // Create the staging bucket to be used by Welder
      stagingBucketName = generateUniqueBucketName("leostaging-" + appName.value)

      _ <- bucketHelper
        .createStagingBucket(userEmail, googleProject, stagingBucketName, gsa)
        .compile
        .drain

      chartValues = buildRStudioAppChartOverrideValuesString(appName,
                                                             cluster,
                                                             nodepoolName,
                                                             namespaceName,
                                                             disk,
                                                             ksaName,
                                                             userEmail,
                                                             stagingBucketName
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
        s"helm install for RSTUDIO app ${appName.value} in project ${cluster.cloudContext.asString}"
      ).compile.lastOrError

      // Poll the app until it starts up
      last <- streamFUntilDone(
        config.rStudioAppConfig.services
          .map(_.name)
          .traverse(s => appDao.isProxyAvailable(googleProject, appName, s)),
        config.monitorConfig.createApp.maxAttempts,
        config.monitorConfig.createApp.interval
      ).interruptAfter(config.monitorConfig.createApp.interruptAfter).compile.lastOrError

      _ <-
        if (!last.isDone) {
          val msg =
            s"RStudio app installation has failed or timed out for app ${appName.value} in cluster ${cluster.getClusterId.toString}"
          logger.error(ctx.loggingCtx)(msg) >>
            F.raiseError[Unit](AppCreationException(msg))
        } else F.unit

    } yield ()
  }

  private[util] def installCustomApp(appId: AppId,
                                     appName: AppName,
                                     release: Release,
                                     dbCluster: KubernetesCluster,
                                     googleCluster: Cluster,
                                     nodepoolName: NodepoolName,
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
    import scala.jdk.CollectionConverters._
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

  private[util] def buildCromwellAppChartOverrideValuesString(
    appName: AppName,
    cluster: KubernetesCluster,
    nodepoolName: NodepoolName,
    namespaceName: NamespaceName,
    disk: PersistentDisk,
    ksaName: ServiceAccountName,
    gsa: WorkbenchEmail,
    customEnvironmentVariables: Map[String, String]
  ): List[String] = {
    val proxyPath = s"/proxy/google/v1/apps/${cluster.cloudContext.asString}/${appName.value}/cromwell-service"
    val k8sProxyHost = kubernetesProxyHost(cluster, config.proxyConfig.proxyDomain).address
    val leoProxyhost = config.proxyConfig.getProxyServerHostName
    val gcsBucket = customEnvironmentVariables.getOrElse("WORKSPACE_BUCKET", "<no workspace bucket defined>")

    val rewriteTarget = "$2"
    val ingress = List(
      raw"""ingress.enabled=true""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=https://${k8sProxyHost}""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-to=${leoProxyhost}""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/rewrite-target=/${rewriteTarget}""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/auth-tls-secret=${namespaceName.value}/ca-secret""",
      raw"""ingress.path=${proxyPath}""",
      raw"""ingress.hosts[0].host=${k8sProxyHost}""",
      raw"""ingress.hosts[0].paths[0]=${proxyPath}${"(/|$)(.*)"}""",
      raw"""ingress.tls[0].secretName=tls-secret""",
      raw"""ingress.tls[0].hosts[0]=${k8sProxyHost}""",
      raw"""db.password=${config.cromwellAppConfig.dbPassword.value}"""
    )

    List(
      // Node selector
      raw"""nodeSelector.cloud\.google\.com/gke-nodepool=${nodepoolName.value}""",
      // Persistence
      raw"""persistence.size=${disk.size.gb.toString}G""",
      raw"""persistence.gcePersistentDisk=${disk.name.value}""",
      raw"""env.swaggerBasePath=$proxyPath/cromwell""",
      // cromwellConfig
      raw"""config.gcsProject=${cluster.cloudContext.asString}""",
      raw"""config.gcsBucket=$gcsBucket/cromwell-execution""",
      // Service Account
      raw"""config.serviceAccount.name=${ksaName.value}""",
      raw"""config.serviceAccount.annotations.gcpServiceAccount=${gsa.value}"""
    ) ++ ingress
  }

  private[util] def buildGalaxyChartOverrideValuesString(appName: AppName,
                                                         release: Release,
                                                         cluster: KubernetesCluster,
                                                         nodepoolName: NodepoolName,
                                                         userEmail: WorkbenchEmail,
                                                         customEnvironmentVariables: Map[String, String],
                                                         ksa: ServiceAccountName,
                                                         namespaceName: NamespaceName,
                                                         nfsDisk: PersistentDisk,
                                                         postgresDiskName: DiskName,
                                                         machineType: AppMachineType,
                                                         galaxyRestore: Option[GalaxyRestore]
  ): List[String] = {
    val k8sProxyHost = kubernetesProxyHost(cluster, config.proxyConfig.proxyDomain).address
    val leoProxyhost = config.proxyConfig.getProxyServerHostName
    val ingressPath = s"/proxy/google/v1/apps/${cluster.cloudContext.asString}/${appName.value}/galaxy"
    val workspaceName = customEnvironmentVariables.getOrElse("WORKSPACE_NAME", "")
    val workspaceNamespace = customEnvironmentVariables.getOrElse("WORKSPACE_NAMESPACE", "")
    // Machine type info
    val maxLimitMemory = machineType.memorySizeInGb
    val maxLimitCpu = machineType.numOfCpus
    val maxRequestMemory = maxLimitMemory - 22
    val maxRequestCpu = maxLimitCpu - 6

    // Custom EV configs
    val configs = customEnvironmentVariables.toList.zipWithIndex.flatMap { case ((k, v), i) =>
      List(
        raw"""configs.$k=$v""",
        raw"""extraEnv[$i].name=$k""",
        raw"""extraEnv[$i].valueFrom.configMapKeyRef.name=${release.asString}-galaxykubeman-configs""",
        raw"""extraEnv[$i].valueFrom.configMapKeyRef.key=$k"""
      )
    }

    val galaxyRestoreSettings = galaxyRestore.fold(List.empty[String])(g =>
      List(
        raw"""restore.persistence.nfs.galaxy.pvcID=${g.galaxyPvcId.asString}""",
        raw"""galaxy.persistence.existingClaim=${release.asString}-galaxy-galaxy-pvc"""
      )
    )
    // Using the string interpolator raw""" since the chart keys include quotes to escape Helm
    // value override special characters such as '.'
    // https://helm.sh/docs/intro/using_helm/#the-format-and-limitations-of---set
    List(
      // Storage class configs
      raw"""nfs.storageClass.name=nfs-${release.asString}""",
      raw"""galaxy.persistence.storageClass=nfs-${release.asString}""",
      // Node selector config: this ensures the app is run on the user's nodepool
      raw"""galaxy.nodeSelector.cloud\.google\.com/gke-nodepool=${nodepoolName.value}""",
      raw"""nfs.nodeSelector.cloud\.google\.com/gke-nodepool=${nodepoolName.value}""",
      raw"""galaxy.configs.job_conf\.yml.runners.k8s.k8s_node_selector=cloud.google.com/gke-nodepool: ${nodepoolName.value}""",
      raw"""galaxy.postgresql.master.nodeSelector.cloud\.google\.com/gke-nodepool=${nodepoolName.value}""",
      // Ingress configs
      raw"""galaxy.ingress.path=${ingressPath}""",
      raw"""galaxy.ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=https://${k8sProxyHost}""",
      raw"""galaxy.ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-to=${leoProxyhost}""",
      raw"""galaxy.ingress.hosts[0].host=${k8sProxyHost}""",
      raw"""galaxy.ingress.hosts[0].paths[0].path=${ingressPath}""",
      raw"""galaxy.ingress.tls[0].hosts[0]=${k8sProxyHost}""",
      raw"""galaxy.ingress.tls[0].secretName=tls-secret""",
      // CVMFS configs
      raw"""cvmfs.cvmfscsi.cache.alien.pvc.storageClass=nfs-${release.asString}""",
      raw"""cvmfs.cvmfscsi.cache.alien.pvc.name=cvmfs-alien-cache""",
      // Galaxy configs
      raw"""galaxy.configs.galaxy\.yml.galaxy.single_user=${userEmail.value}""",
      raw"""galaxy.configs.galaxy\.yml.galaxy.admin_users=${userEmail.value}""",
      raw"""galaxy.terra.launch.workspace=${workspaceName}""",
      raw"""galaxy.terra.launch.namespace=${workspaceNamespace}""",
      raw"""galaxy.terra.launch.apiURL=${config.galaxyAppConfig.orchUrl.value}""",
      raw"""galaxy.terra.launch.drsURL=${config.galaxyAppConfig.drsUrl.value}""",
      // Tusd ingress configs
      raw"""galaxy.tusd.ingress.hosts[0].host=${k8sProxyHost}""",
      raw"""galaxy.tusd.ingress.hosts[0].paths[0].path=${ingressPath}/api/upload/resumable_upload""",
      raw"""galaxy.tusd.ingress.tls[0].hosts[0]=${k8sProxyHost}""",
      raw"""galaxy.tusd.ingress.tls[0].secretName=tls-secret""",
      // Set RabbitMQ storage class
      raw"""galaxy.rabbitmq.persistence.storageClassName=nfs-${release.asString}""",
      // Set Machine Type specs
      raw"""galaxy.jobs.maxLimits.memory=${maxLimitMemory}""",
      raw"""galaxy.jobs.maxLimits.cpu=${maxLimitCpu}""",
      raw"""galaxy.jobs.maxRequests.memory=${maxRequestMemory}""",
      raw"""galaxy.jobs.maxRequests.cpu=${maxRequestCpu}""",
      raw"""galaxy.jobs.rules.tpv_rules_local\.yml.destinations.k8s.max_mem=${maxRequestMemory}""",
      raw"""galaxy.jobs.rules.tpv_rules_local\.yml.destinations.k8s.max_cores=${maxRequestCpu}""",
      // RBAC configs
      raw"""galaxy.serviceAccount.create=false""",
      raw"""galaxy.serviceAccount.name=${ksa.value}""",
      raw"""rbac.serviceAccount=${ksa.value}""",
      // Persistence configs
      raw"""persistence.nfs.name=${namespaceName.value}-${config.galaxyDiskConfig.nfsPersistenceName}""",
      raw"""persistence.nfs.persistentVolume.extraSpec.gcePersistentDisk.pdName=${nfsDisk.name.value}""",
      raw"""persistence.nfs.size=${nfsDisk.size.gb.toString}Gi""",
      raw"""persistence.postgres.name=${namespaceName.value}-${config.galaxyDiskConfig.postgresPersistenceName}""",
      raw"""galaxy.postgresql.galaxyDatabasePassword=${config.galaxyAppConfig.postgresPassword.value}""",
      raw"""persistence.postgres.persistentVolume.extraSpec.gcePersistentDisk.pdName=${postgresDiskName.value}""",
      raw"""persistence.postgres.size=${config.galaxyDiskConfig.postgresDiskSizeGB.gb.toString}Gi""",
      raw"""nfs.persistence.existingClaim=${namespaceName.value}-${config.galaxyDiskConfig.nfsPersistenceName}-pvc""",
      raw"""nfs.persistence.size=${nfsDisk.size.gb.toString}Gi""",
      raw"""galaxy.postgresql.persistence.existingClaim=${namespaceName.value}-${config.galaxyDiskConfig.postgresPersistenceName}-pvc""",
      // Note Galaxy pvc claim is the nfs disk size minus 50G
      raw"""galaxy.persistence.size=${(nfsDisk.size.gb - 50).toString}Gi"""
    ) ++ configs ++ galaxyRestoreSettings
  }

  private[util] def buildRStudioAppChartOverrideValuesString(
    appName: AppName,
    cluster: KubernetesCluster,
    nodepoolName: NodepoolName,
    namespaceName: NamespaceName,
    disk: PersistentDisk,
    ksaName: ServiceAccountName,
    userEmail: WorkbenchEmail,
    stagingBucket: GcsBucketName
  ): List[String] = {
    val rstudioIngressPath = s"/proxy/google/v1/apps/${cluster.cloudContext.asString}/${appName.value}/rstudio-service"
    val welderIngressPath = s"/proxy/google/v1/apps/${cluster.cloudContext.asString}/${appName.value}/welder-service"
    val k8sProxyHost = kubernetesProxyHost(cluster, config.proxyConfig.proxyDomain).address
    val leoProxyhost = config.proxyConfig.getProxyServerHostName

    val rewriteTarget = "$2"
    val ingress = List(
      raw"""ingress.enabled=true""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/auth-tls-secret=${namespaceName.value}/ca-secret""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=https://${k8sProxyHost}""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-to=${leoProxyhost}${rstudioIngressPath}""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/rewrite-target=/${rewriteTarget}""",
      raw"""ingress.host=${k8sProxyHost}""",
      raw"""ingress.rstudio.path=${rstudioIngressPath}${"(/|$)(.*)"}""",
      raw"""ingress.welder.path=${welderIngressPath}${"(/|$)(.*)"}""",
      raw"""ingress.tls[0].secretName=tls-secret""",
      raw"""ingress.tls[0].hosts[0]=${k8sProxyHost}"""
    )

    val welder = List(
      raw"""welder.extraEnv[0].name=GOOGLE_PROJECT""",
      raw"""welder.extraEnv[0].value=${cluster.cloudContext.asString}""",
      raw"""welder.extraEnv[1].name=STAGING_BUCKET""",
      raw"""welder.extraEnv[1].value=${stagingBucket.value}""",
      raw"""welder.extraEnv[2].name=CLUSTER_NAME""",
      raw"""welder.extraEnv[2].value=${appName.value}""",
      raw"""welder.extraEnv[3].name=OWNER_EMAIL""",
      raw"""welder.extraEnv[3].value=${userEmail.value}"""
    )

    List(
      // Node selector
      raw"""nodeSelector.cloud\.google\.com/gke-nodepool=${nodepoolName.value}""",
      // Persistence
      raw"""persistence.size=${disk.size.gb.toString}G""",
      raw"""persistence.gcePersistentDisk=${disk.name.value}""",
      // Service Account
      raw"""serviceAccount.name=${ksaName.value}"""
    ) ++ ingress ++ welder
  }

  private def getTerraAppSetupChartReleaseName(appReleaseName: Release): Release =
    Release(s"${appReleaseName.asString}-setup-rls")

  private[util] def buildCustomChartOverrideValuesString(appName: AppName,
                                                         release: Release,
                                                         nodepoolName: NodepoolName,
                                                         serviceName: String,
                                                         cluster: KubernetesCluster,
                                                         namespaceName: NamespaceName,
                                                         service: CustomAppService,
                                                         extraArgs: List[String],
                                                         disk: PersistentDisk,
                                                         ksaName: ServiceAccountName,
                                                         customEnvironmentVariables: Map[String, String]
  ): String = {
    val k8sProxyHost = kubernetesProxyHost(cluster, config.proxyConfig.proxyDomain).address
    val leoProxyhost = config.proxyConfig.getProxyServerHostName
    val ingressPath = s"/proxy/google/v1/apps/${cluster.cloudContext.asString}/${appName.value}/${serviceName}"

    // Command and args
    val command = service.command.zipWithIndex.map { case (c, i) =>
      raw"""image.command[$i]=$c"""
    }
    val args = service.args.zipWithIndex.map { case (a, i) =>
      raw"""image.args[$i]=$a"""
    } ++ extraArgs.zipWithIndex.map { case (a, i) =>
      raw"""image.args[${i + service.args.length}]=$a"""
    }

    // Custom EVs
    val configs = customEnvironmentVariables.toList.zipWithIndex.flatMap { case ((k, v), i) =>
      List(
        raw"""extraEnv[$i].name=$k""",
        raw"""extraEnv[$i].value=$v"""
      )
    }

    val rewriteTarget = "$2"
    // These nginx an ingress rules are condition.
    // Some apps do not like behind behind a reverse proxy in this way, and require routing specified via this baseUrl
    // The two methods are mutually exclusive
    val ingress = service.baseUrl match {
      case "/" =>
        List(
          raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=https://${k8sProxyHost}""",
          raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-to=${leoProxyhost}${ingressPath}""",
          raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/rewrite-target=/${rewriteTarget}""",
          raw"""ingress.hosts[0].paths[0]=${ingressPath}${"(/|$)(.*)"}"""
        )
      case _ => List(raw"""ingress.hosts[0].paths[0]=${service.baseUrl}""")
    }

    (List(
      raw"""nameOverride=${serviceName}""",
      // Image
      raw"""image.image=${service.image.imageUrl}""",
      raw"""image.port=${service.port}""",
      raw"""image.baseUrl=${service.baseUrl}""",
      // Ingress
      raw"""ingress.hosts[0].host=${k8sProxyHost}""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/auth-tls-secret=${namespaceName.value}/ca-secret""",
      raw"""ingress.tls[0].secretName=tls-secret""",
      raw"""ingress.tls[0].hosts[0]=${k8sProxyHost}""",
      // Node selector
      raw"""nodeSelector.cloud\.google\.com/gke-nodepool=${nodepoolName.value}""",
      // Persistence
      raw"""persistence.size=${disk.size.gb.toString}G""",
      raw"""persistence.gcePersistentDisk=${disk.name.value}""",
      raw"""persistence.mountPath=${service.pdMountPath}""",
      raw"""persistence.accessMode=${service.pdAccessMode}""",
      raw"""serviceAccount.name=${ksaName.value}"""
    ) ++ command ++ args ++ configs ++ ingress).mkString(",")
  }

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

final case class DiskNotFoundForAppException(appId: AppId, traceId: TraceId)
    extends LeoException(s"No persistent disk found for ${appId}", traceId = Some(traceId))

final case class DeleteNodepoolResult(nodepoolId: NodepoolLeoId,
                                      operation: com.google.container.v1.Operation,
                                      getAppResult: GetAppResult
)

final case class GKEInterpreterConfig(vpcNetworkTag: NetworkTag,
                                      terraAppSetupChartConfig: TerraAppSetupChartConfig,
                                      ingressConfig: KubernetesIngressConfig,
                                      galaxyAppConfig: GalaxyAppConfig,
                                      cromwellAppConfig: CromwellAppConfig,
                                      customAppConfig: CustomAppConfig,
                                      rStudioAppConfig: RStudioAppConfig,
                                      monitorConfig: AppMonitorConfig,
                                      clusterConfig: KubernetesClusterConfig,
                                      proxyConfig: ProxyConfig,
                                      galaxyDiskConfig: GalaxyDiskConfig
)

final case class TerraAppSetupChartConfig(
  chartName: ChartName,
  chartVersion: ChartVersion
)
