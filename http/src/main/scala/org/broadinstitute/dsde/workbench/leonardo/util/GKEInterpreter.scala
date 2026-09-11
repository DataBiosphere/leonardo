package org.broadinstitute.dsde.workbench.leonardo
package util

import _root_.org.typelevel.log4cats.StructuredLogger
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.compute.v1.{
  AccessConfig,
  Allowed,
  AttachedDisk,
  AttachedDiskInitializeParams,
  Firewall,
  Instance,
  Items,
  Metadata,
  NetworkInterface,
  ServiceAccount,
  Tags
}
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
  FirewallRuleName,
  GoogleComputeService,
  GoogleDiskService,
  GoogleResourceService,
  KubernetesClusterNotFoundException,
  NetworkName,
  RegionName,
  SubnetworkName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.util2.InstanceName
import org.broadinstitute.dsde.workbench.leonardo.dao.{AppDAO, AppDescriptorDAO}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppNotFoundException
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{buildMachineTypeUri, buildSubnetworkUri}
import org.broadinstitute.dsde.workbench.leonardo.util.BuildHelmChartValues.{
  buildAllowedAppChartOverrideValuesString,
  buildCromwellAppChartOverrideValuesString,
  buildCustomChartOverrideValuesString
}
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.leonardo.util.GKEAlgebra._
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject, ServiceAccountDisplayName}
import org.broadinstitute.dsde.workbench.model.google.iam.IamMemberTypes
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

      // Get labels and service account to pass in the create cluster request
      projectLabels <- googleResourceService.getLabels(params.googleProject)
      serviceAccount <- getNodepoolServiceAccount(params.googleProject)

      _ <- logger.info(ctx.loggingCtx)(
        s"[AN-276] Project labels: ${projectLabels.getOrElse("None")}"
      )

      _ <- logger.info(s"[AN-276] Building GKE Nodepool with service account: ${serviceAccount.getOrElse("default")}")

      nodepools =
        if (params.autopilot) List.empty
        else
          dbCluster.nodepools
            .filter(n => params.nodepoolsToCreate.contains(n.id))
            .map(np => buildLegacyGoogleNodepool(np, serviceAccount))

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
        new RuntimeException("trying to create a non google runtime in GKEInterpreter. This should never happen")
      )
      kubeNetwork = KubernetesNetwork(googleProject, network)
      kubeSubNetwork = KubernetesSubNetwork(googleProject, dbCluster.region, subnetwork)

      networkConfig = new com.google.api.services.container.model.NetworkConfig()
        .setEnableIntraNodeVisibility(params.enableIntraNodeVisibility)

      networkTag = new com.google.api.services.container.model.NetworkTags()
        .setTags(List(config.vpcNetworkTag.value).asJava)
      nodepoolConfig = new com.google.api.services.container.model.NodePoolAutoConfig().setNetworkTags(networkTag)
      networkPolicy =
        if (params.autopilot) null else new com.google.api.services.container.model.NetworkPolicy().setEnabled(true)

      autoscaling =
        if (params.autopilot) {
          serviceAccount match {
            case Some(sa) =>
              new com.google.api.services.container.model.ClusterAutoscaling().setAutoprovisioningNodePoolDefaults(
                new com.google.api.services.container.model.AutoprovisioningNodePoolDefaults().setServiceAccount(sa)
              )
            case None => null
          }

        } else null

      legacyCreateClusterRec = new com.google.api.services.container.model.Cluster()
        .setName(dbCluster.clusterName.value)
        .setInitialClusterVersion(config.clusterConfig.version.value)
        .setNodePools(nodepools.asJava)
        .setAutopilot(autopilot)
        .setAutoscaling(autoscaling)
        .setAddonsConfig(
          new com.google.api.services.container.model.AddonsConfig()
            .setGcsFuseCsiDriverConfig(
              new com.google.api.services.container.model.GcsFuseCsiDriverConfig().setEnabled(true)
            )
        )
        .setNodePoolAutoConfig(nodepoolConfig)
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
            .setGcpPublicCidrsAccessEnabled(true)
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
      serviceAccount <- getNodepoolServiceAccount(params.googleProject)

      _ <- logger.info(ctx.loggingCtx)(
        s"Beginning nodepool creation for nodepool ${dbNodepool.nodepoolName.value} in cluster ${dbCluster.getClusterId.toString}"
      )

      _ <- logger.info(s"[AN-276] Building GKE Nodepool with service account: ${serviceAccount.getOrElse("default")}")

      req = KubernetesCreateNodepoolRequest(
        dbCluster.getClusterId,
        buildGoogleNodepool(dbNodepool, serviceAccount)
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
      dbCluster = dbApp.cluster
      googleProject = params.googleProject
      // Idempotency: if the app is already Running, skip creation to avoid double-recording usage
      _ <-
        if (app.status == AppStatus.Running)
          logger.info(ctx.loggingCtx)(
            s"App ${app.appName.value} is already Running, skipping creation (idempotent)"
          )
        else
          for {
            diskOpt <- appQuery.getDiskId(app.id).transaction
            diskId <- F.fromOption(diskOpt, DiskNotFoundForAppException(app.id, ctx.traceId))

            _ <- logger.info(ctx.loggingCtx)(s"Begin App(${app.appName.value}) Creation.")

            nfsDisk <- F.fromOption(
              dbApp.app.appResources.disk,
              AppCreationException(
                s"NFS disk not found in DB for app ${app.appName.value} | trace id: ${ctx.traceId}"
              )
            )

            // Galaxy uses a VM-based deployment; all other app types use the GKE/Helm path.
            _ <- app.appType match {
              case AppType.Galaxy =>
                installGalaxyVm(dbCluster, app, nfsDisk, googleProject, params.restore) >>
                  persistentDiskQuery.updateLastUsedBy(diskId, app.id).transaction.void

              case _ =>
                createAndPollAppViaHelm(params, dbApp, app, dbCluster, nfsDisk, diskId, googleProject, ctx)
            }

            _ <- logger.info(ctx.loggingCtx)(
              s"Finished app creation for app ${app.appName.value}"
            )

            readyTime <- F.realTimeInstant
            _ <- appUsageQuery.recordStart(params.appId, readyTime)
            _ <- appQuery.updateStatus(params.appId, AppStatus.Running).transaction
          } yield ()
    } yield ()

  // GKE/Helm path for non-Galaxy app types (Cromwell, Allowed, Custom).
  private def createAndPollAppViaHelm(
    params: CreateAppParams,
    dbApp: GetAppResult,
    app: App,
    dbCluster: KubernetesCluster,
    nfsDisk: PersistentDisk,
    diskId: DiskId,
    googleProject: GoogleProject,
    ctx: AppContext
  )(implicit ev: Ask[F, AppContext]): F[Unit] = {
    val namespaceName = app.appResources.namespace
    val gkeClusterId = dbCluster.getClusterId
    for {
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
      _ <- appQuery.updateKubernetesServiceAccount(app.id, ksaName).transaction

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
      retryConfig = RetryPredicates.retryConfigWithPredicates(when409)
      _ <- tracedRetryF(retryConfig)(
        call,
        s"googleIamDAO.addIamPolicyBindingOnServiceAccount for GSA ${gsa.value} & KSA ${ksaName.value}"
      ).compile.lastOrError

      nodepool = if (app.autopilot.isDefined) None else Some(dbApp.nodepool.nodepoolName)

      _ <- app.appType match {
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
            app.autopilot,
            params.bucketNameToMount
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

      _ <- app.appType match {
        case AppType.Cromwell => persistentDiskQuery.updateLastUsedBy(diskId, app.id).transaction
        case AppType.Allowed  => persistentDiskQuery.updateLastUsedBy(diskId, app.id).transaction
        case AppType.Custom   => F.unit
        case _ =>
          F.raiseError(AppCreationException(s"App type ${app.appType} not supported on GCP"))
      }
    } yield ()
  }

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

      _ <- app.appType match {
        case AppType.Galaxy =>
          // Galaxy runs on a VM: delete the GCE instance. Disks are retained for persistence.
          for {
            gp <- F.fromOption(
              LeoLenses.cloudContextToGoogleProject.get(dbCluster.cloudContext),
              new RuntimeException("Galaxy app cloud context should be a google project")
            )
            instanceName = InstanceName(s"galaxy-${app.appName.value}")
            zone = dbApp.app.appResources.disk.map(_.zone).getOrElse(ZoneName(config.clusterConfig.location.value))
            _ <- computeService
              .deleteInstance(gp, zone, instanceName)
              .void
              .handleErrorWith { e =>
                logger.warn(ctx.loggingCtx)(
                  s"Failed to delete Galaxy VM ${instanceName.value}: ${e.getMessage}. Continuing with app deletion."
                )
              }
            // Mark the cluster DB record as deleted so future app creation in this project is not blocked.
            // For Galaxy, the "cluster" is a pure DB abstraction (no real GKE cluster); it must be
            // cleaned up here because no separate cluster-deletion pubsub message is sent.
            _ <- kubernetesClusterQuery.markAsDeleted(dbCluster.id, ctx.now).transaction
          } yield ()

        case _ =>
          // GKE/Helm path for all other app types
          googleClusterOpt.traverse { googleCluster =>
            val uninstallCharts = for {
              helmAuthContext <- getHelmAuthContext(googleCluster, dbCluster, namespaceName)

              _ <- logger.info(ctx.loggingCtx)(
                s"Uninstalling release ${app.release.asString} for ${app.appType.toString} app ${app.appName.value} in cluster ${dbCluster.getClusterId.toString}"
              )

              _ <- helmClient
                .uninstall(app.release, true)
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

              _ <- helmClient
                .uninstall(getTerraAppSetupChartReleaseName(app.release), true)
                .run(helmAuthContext)
            } yield ()

            uninstallCharts.handleErrorWith { e =>
              logger.info(ctx.loggingCtx)(
                s"Uninstalling release ${app.release.asString} for ${app.appType.toString} app ${app.appName.value} in cluster ${dbCluster.getClusterId.toString} failed with error ${e.getMessage}"
              )
            }
          }.void >>
            kubeService
              .deleteNamespace(dbApp.cluster.getClusterId, KubernetesNamespace(dbApp.app.appResources.namespace)) >>
            streamUntilDoneOrTimeout(
              kubeService
                .namespaceExists(dbApp.cluster.getClusterId, KubernetesNamespace(dbApp.app.appResources.namespace))
                .map(!_),
              60,
              5 seconds,
              "delete namespace timed out"
            )
      }

      _ <- logger.info(ctx.loggingCtx)(
        s"Delete app operation has finished for app ${app.appName.value}"
      )

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
            appDao.isProxyAvailable(params.googleProject, dbApp.app.appName, ServiceName("galaxy"), ctx.traceId),
            config.monitorConfig.startApp.maxAttempts,
            config.monitorConfig.startApp.interval
          ).interruptAfter(config.monitorConfig.startApp.interruptAfter).compile.lastOrError
        case AppType.Cromwell =>
          streamFUntilDone(
            config.cromwellAppConfig.services
              .map(_.name)
              .traverse(s => appDao.isProxyAvailable(params.googleProject, dbApp.app.appName, s, ctx.traceId)),
            config.monitorConfig.startApp.maxAttempts,
            config.monitorConfig.startApp.interval
          ).interruptAfter(config.monitorConfig.startApp.interruptAfter).compile.lastOrError.map(x => x.isDone)
        case AppType.Allowed =>
          streamFUntilDone(
            config.allowedAppConfig.services
              .map(_.name)
              .traverse(s => appDao.isProxyAvailable(params.googleProject, dbApp.app.appName, s, ctx.traceId)),
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
                appDao.isProxyAvailable(params.googleProject, dbApp.app.appName, ServiceName(s), ctx.traceId)
              ),
              config.monitorConfig.startApp.maxAttempts,
              config.monitorConfig.startApp.interval
            ).interruptAfter(config.monitorConfig.startApp.interruptAfter).compile.lastOrError
          } yield last.isDone
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

  private[util] def installGalaxyVm(
    dbCluster: KubernetesCluster,
    app: App,
    nfsDisk: PersistentDisk,
    googleProject: GoogleProject,
    restore: Boolean
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask

      _ <- logger.info(ctx.loggingCtx)(
        s"Installing Galaxy VM for app ${app.appName.value} in project ${googleProject.value}"
      )

      zoneParam = nfsDisk.zone
      regionParam = RegionName(zoneParam.value.dropRight(2))

      // Set up VPC and firewall
      (network, subnetwork) <- vpcAlg.setUpProjectNetworkAndFirewalls(
        SetUpProjectNetworkParams(googleProject, regionParam)
      )

      // Load the startup-script passed to the GCE Guest Agent via the "startup-script" metadata key.
      // The Guest Agent executes it on every boot, unlike cloud-init user-data which the pre-baked
      // galaxy-k8s-boot image treats as already-run and skips.
      // To update, sync manually from https://github.com/galaxyproject/galaxy-k8s-boot/blob/anvil/bin/user_data.sh
      startupScriptContent <- F.fromTry(
        scala.util.Using(scala.io.Source.fromResource("init-resources/galaxy-user-data.sh"))(_.mkString)
      )

      // Derive postgres disk name using the same naming convention as the subscriber
      postgresDiskName = GKEAlgebra.getGalaxyPostgresDiskName(nfsDisk.name,
                                                              config.galaxyDiskConfig.postgresDiskNameSuffix
      )

      // Persistent-volume-size passed to ansible-pull.
      // nfsDisk.size.gb is in decimal GB; convert to binary GiB before subtracting filesystem overhead.
      // Example: a 500 GB disk = (500 * 10^9) / 2^30 ≈ 465 GiB, so we request 465 - 11 = 454 GiB.
      // Using raw gb - 11 (treating GB as GiB) overestimates by ~23 GiB on a 500 GB disk and
      // causes the NFS provisioner to fail with "insufficient available space".
      diskSizeGiB = (nfsDisk.size.gb.toLong * 1000L * 1000L * 1000L) / (1024L * 1024L * 1024L)
      pvSizeGi = math.max(1, diskSizeGiB - 11)
      pvSize = s"${pvSizeGi}Gi"

      // Get or create the galaxy-batch-runner SA in the user's project.
      gcpBatchSa <- F
        .fromFuture(
          F.delay(
            googleIamDAO.getOrCreateServiceAccount(
              googleProject,
              org.broadinstitute.dsde.workbench.model.google.ServiceAccountName("galaxy-batch-runner"),
              ServiceAccountDisplayName("Galaxy Batch Runner")
            )
          )
        )
        .map(sa => sa.email.value)

      galaxyUrlPrefix = s"/proxy/google/v1/apps/${googleProject.value}/${app.appName.value}/galaxy"

      // Disks — data and postgres disks are always pre-existing by the time this method runs
      // (created by createDiskOp / createSecondDiskOp, or retained from a previous app).
      // Use setSource to attach existing disks; only the boot disk is created fresh.
      bootDisk = AttachedDisk
        .newBuilder()
        .setBoot(true)
        .setAutoDelete(true)
        .setInitializeParams(
          AttachedDiskInitializeParams
            .newBuilder()
            .setSourceImage(config.galaxyVmConfig.sourceImage.asString)
            .setDiskSizeGb(config.galaxyVmConfig.bootDiskSizeGb.gb)
            .putAllLabels(Map("leonardo" -> "true").asJava)
            .build()
        )
        .build()

      // Galaxy data disk — device name must match what the bootstrap script expects
      dataDisk = AttachedDisk
        .newBuilder()
        .setBoot(false)
        .setDeviceName("galaxy-data")
        .setAutoDelete(false)
        .setSource(
          s"projects/${googleProject.value}/zones/${zoneParam.value}/disks/${nfsDisk.name.value}"
        )
        .build()

      // PostgreSQL disk — device name must match what the bootstrap script expects
      postgresDisk = AttachedDisk
        .newBuilder()
        .setBoot(false)
        .setDeviceName("galaxy-postgres-data")
        .setAutoDelete(false)
        .setSource(
          s"projects/${googleProject.value}/zones/${zoneParam.value}/disks/${postgresDiskName.value}"
        )
        .build()

      // Network interface with external IP
      networkInterface = NetworkInterface
        .newBuilder()
        .setSubnetwork(
          buildSubnetworkUri(googleProject, regionParam, subnetwork)
        )
        .addAccessConfigs(AccessConfig.newBuilder().setName("Leonardo Galaxy VM external IP").build())
        .build()

      instanceName = InstanceName(s"galaxy-${app.appName.value}")

      instance = Instance
        .newBuilder()
        .setName(instanceName.value)
        .setDescription("Leonardo Galaxy VM")
        .setTags(Tags.newBuilder().addItems(config.vpcNetworkTag.value).build())
        // Prefer the machine type stored on the app's nodepool (set from the request at creation
        // time); fall back to the configured default.
        .setMachineType(
          buildMachineTypeUri(zoneParam,
                              dbCluster.nodepools
                                .find(_.id == app.nodepoolId)
                                .map(_.machineType)
                                .getOrElse(config.galaxyVmConfig.machineType)
          )
        )
        .addNetworkInterfaces(networkInterface)
        .addAllDisks(List(bootDisk, dataDisk, postgresDisk).asJava)
        .addServiceAccounts(
          ServiceAccount
            .newBuilder()
            .setEmail(app.googleServiceAccount.value)
            .addAllScopes(
              List(
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/logging.write",
                "https://www.googleapis.com/auth/userinfo.email",
                "https://www.googleapis.com/auth/userinfo.profile"
              ).asJava
            )
            .build()
        )
        .setMetadata(
          Metadata
            .newBuilder()
            .addItems(Items.newBuilder().setKey("startup-script").setValue(startupScriptContent).build())
            .addItems(Items.newBuilder().setKey("google-logging-enabled").setValue("true").build())
            .addItems(Items.newBuilder().setKey("gcp_batch_service_account_email").setValue(gcpBatchSa).build())
            .addItems(Items.newBuilder().setKey("persistent-volume-size").setValue(pvSize).build())
            .addItems(Items.newBuilder().setKey("restore_galaxy").setValue(restore.toString).build())
            .addItems(Items.newBuilder().setKey("git-repo").setValue(config.galaxyVmConfig.gitRepo).build())
            .addItems(Items.newBuilder().setKey("git-branch").setValue(config.galaxyVmConfig.gitBranch).build())
            .addItems(Items.newBuilder().setKey("gcp-region").setValue(regionParam.value).build())
            .addItems(Items.newBuilder().setKey("gcp-network").setValue(network.value).build())
            .addItems(Items.newBuilder().setKey("gcp-subnet").setValue(subnetwork.value).build())
            .addItems(
              Items
                .newBuilder()
                .setKey("terra-workspace")
                .setValue(app.customEnvironmentVariables.getOrElse(WORKSPACE_NAME_KEY, ""))
                .build()
            )
            .addItems(
              Items
                .newBuilder()
                .setKey("terra-namespace")
                .setValue(app.customEnvironmentVariables.getOrElse(WORKSPACE_NAMESPACE_KEY, ""))
                .build()
            )
            .addItems(Items.newBuilder().setKey("gcp-project-id").setValue(googleProject.value).build())
            .addItems(Items.newBuilder().setKey("terra-drs-url").setValue(config.galaxyVmConfig.drsUrl).build())
            .addItems(Items.newBuilder().setKey("terra-api-url").setValue(config.galaxyVmConfig.orchUrl).build())
            // Galaxy admin user email — used by the post-install job to create the initial Galaxy admin.
            .addItems(
              Items.newBuilder().setKey("galaxy-user-email").setValue(app.auditInfo.creator.value).build()
            )
            // Leo proxy path prefix passed to ansible-pull as galaxy_prefix so Galaxy's nginx ingress
            // is configured at the correct subpath. Without this, Galaxy generates links rooted at /
            // which the browser resolves against Leo's host and gets 404s → blank page.
            .addItems(
              Items
                .newBuilder()
                .setKey("galaxy-url-prefix")
                .setValue(galaxyUrlPrefix)
                .build()
            )
            // Full HTTPS base URL of the Leo proxy (scheme + host). Combined with galaxy-url-prefix
            // in the startup script to form galaxy_infrastructure_url, which tells Galaxy to generate
            // https:// absolute links. Without this Galaxy uses its internal http:// connection and
            // produces http:// links (e.g. history export URLs) that browsers reject for the Secure cookie.
            .addItems(
              Items
                .newBuilder()
                .setKey("galaxy-proxy-base-url")
                .setValue(config.leoUrlBase.toString.stripSuffix("/"))
                .build()
            )
            .build()
        )
        .putAllLabels(Map("leonardo" -> "true").asJava)
        .build()

      _ <- computeService.createInstance(googleProject, zoneParam, instance)

      // Grant the pet SA permission to submit and monitor GCP Batch jobs in this project.
      // Galaxy uses the VM's attached SA (pet SA) to call the Batch API.
      _ <- {
        val call = F.fromFuture(
          F.delay(
            googleIamDAO
              .addRoles(googleProject,
                        app.googleServiceAccount,
                        IamMemberTypes.ServiceAccount,
                        Set("roles/batch.jobsEditor")
              )
              .void
          )
        )
        val retryConfig = RetryPredicates.retryConfigWithPredicates(when409, whenGroupDoesNotExist)
        tracedRetryF(retryConfig)(
          call,
          s"googleIamDAO.addRoles(batch.jobsEditor) for pet SA ${app.googleServiceAccount.value} in project ${googleProject.value}"
        ).compile.lastOrError
      }

      // Grant the pet SA serviceAccountUser on the Batch SA so it can specify it as the job runner identity.
      // Only attempted when the Batch SA lives in the same project as the user (i.e. not a shared platform SA).
      // For cross-project Batch SAs, this binding must be set up externally (e.g. via Terraform).
      gcpBatchSaProject = GoogleProject(
        gcpBatchSa.split("@").lift(1).map(_.stripSuffix(".iam.gserviceaccount.com")).getOrElse("")
      )
      _ <-
        if (gcpBatchSaProject == googleProject)
          F.fromFuture(
            F.delay(
              googleIamDAO.addIamPolicyBindingOnServiceAccount(
                googleProject,
                WorkbenchEmail(gcpBatchSa),
                app.googleServiceAccount,
                Set("roles/iam.serviceAccountUser")
              )
            )
          )
        else
          logger.info(ctx.loggingCtx)(
            s"Batch SA $gcpBatchSa is in a different project ($gcpBatchSaProject) than ${googleProject.value}; " +
              s"skipping serviceAccountUser binding — must be configured externally"
          )

      // Grant the Batch SA the project-level roles it needs to run jobs and attach a service account to Batch VMs.
      // See https://github.com/galaxyproject/galaxy-k8s-boot?tab=readme-ov-file#prerequisites
      _ <- {
        val call = F.fromFuture(
          F.delay(
            googleIamDAO
              .addRoles(
                googleProject,
                WorkbenchEmail(gcpBatchSa),
                IamMemberTypes.ServiceAccount,
                Set("roles/batch.jobsEditor",
                    "roles/iam.serviceAccountUser",
                    "roles/batch.agentReporter",
                    "roles/logging.logWriter"
                )
              )
              .void
          )
        )
        val retryConfig = RetryPredicates.retryConfigWithPredicates(when409, whenGroupDoesNotExist)
        tracedRetryF(retryConfig)(
          call,
          s"googleIamDAO.addRoles(batch.jobsEditor, iam.serviceAccountUser) for Batch SA $gcpBatchSa in project ${googleProject.value}"
        ).compile.lastOrError
      }

      // Create an NFS firewall rule so GCP Batch VMs can reach the Galaxy VM's NFS server.
      // Idempotent: skipped if the rule already exists.
      nfsFwName = FirewallRuleName("leonardo-galaxy-allow-nfs-for-batch")
      nfsFwExists <- computeService.getFirewallRule(googleProject, nfsFwName)
      _ <-
        if (nfsFwExists.isEmpty) {
          val nfsFirewall = Firewall
            .newBuilder()
            .setName(nfsFwName.value)
            .setNetwork(s"projects/${googleProject.value}/global/networks/${network.value}")
            .addSourceRanges("10.0.0.0/8")
            .addTargetTags(config.vpcNetworkTag.value)
            .addAllowed(Allowed.newBuilder().setIPProtocol("tcp").addPorts("2049").build())
            .addAllowed(Allowed.newBuilder().setIPProtocol("udp").addPorts("2049").build())
            .addAllowed(Allowed.newBuilder().setIPProtocol("tcp").addPorts("111").build())
            .addAllowed(Allowed.newBuilder().setIPProtocol("udp").addPorts("111").build())
            .build()
          computeService
            .addFirewallRule(googleProject, nfsFirewall)
            .flatMap(op => F.blocking(op.get()).void)
        } else
          logger.info(ctx.loggingCtx)(
            s"NFS firewall rule ${nfsFwName.value} already exists, skipping creation"
          )

      _ <- logger.info(ctx.loggingCtx)(
        s"Galaxy VM instance ${instanceName.value} submitted for project ${googleProject.value}; polling for external IP"
      )

      // Poll until the instance has an external IP assigned (needed for both proxy routing and readiness check).
      // We store the external IP because Leo's GKE cluster and the Galaxy VM are in different GCP projects
      // whose VPCs are not peered, making the internal IP unreachable from Leo's pod.
      ipPairOpt <- streamFUntilDone(
        computeService.getInstance(googleProject, zoneParam, instanceName).map { instanceOpt =>
          instanceOpt.flatMap { inst =>
            import scala.jdk.CollectionConverters._
            for {
              iface <- Option(inst.getNetworkInterfacesList).flatMap(_.asScala.headOption)
              internalIp = IP(iface.getNetworkIP)
              cfg <- Option(iface.getAccessConfigsList).flatMap(_.asScala.headOption)
              natIp <- Option(cfg.getNatIP).filter(_.nonEmpty)
            } yield (internalIp, IP(natIp))
          }
        },
        config.monitorConfig.createApp.maxAttempts,
        config.monitorConfig.createApp.interval
      ).compile.lastOrError

      (internalIp, externalIp) <- F.fromOption(
        ipPairOpt,
        AppCreationException(
          s"Galaxy VM ${instanceName.value} did not obtain an IP after ${config.monitorConfig.createApp.interruptAfter}",
          traceId = Some(ctx.traceId)
        )
      )

      _ <- logger.info(ctx.loggingCtx)(
        s"Galaxy VM ${instanceName.value} has internal IP ${internalIp.asString} / external IP ${externalIp.asString}; storing external IP in cluster async fields for proxy access"
      )

      // Store the VM's external IP as the cluster load balancer IP consumed by KubernetesDnsCache.
      // The proxy will connect to this IP via HTTP on port 80 (Galaxy VM serves HTTP, not HTTPS).
      // We use the external IP because Leo's GKE cluster is in Leo's GCP project while the Galaxy VM
      // is in the user's workspace project — the two VPCs are not peered, so the internal IP is
      // not routable from Leo's pod. The leonardo-allow-http firewall rule (0.0.0.0/0 → port 80,
      // targeting VMs with the "leonardo" tag) allows Leo to reach the VM on its external IP.
      _ <- kubernetesClusterQuery
        .updateAsyncFields(
          dbCluster.id,
          KubernetesClusterAsyncFields(
            externalIp,
            IP(""),
            NetworkFields(NetworkName(""), SubnetworkName(""), IpRange(""))
          )
        )
        .transaction
      _ <- kubernetesClusterQuery.updateStatus(dbCluster.id, KubernetesClusterStatus.Running).transaction

      _ <- logger.info(ctx.loggingCtx)(
        s"Polling Galaxy readiness for app ${app.appName.value} via proxy (backend: ${externalIp.asString}:80)"
      )

      // Poll /api/version rather than the bare prefix path: nginx can return a 301 redirect
      // (trailing-slash normalisation) for the bare path while Galaxy pods are still starting,
      // which would satisfy < 400 and mark the app Running too early. /api/version requires
      // Galaxy's Python API to be fully initialised and only returns 200 at that point.
      isDone <- streamFUntilDone(
        appDao.isVmReachable(externalIp, 80, ctx.traceId, s"$galaxyUrlPrefix/api/version"),
        config.monitorConfig.createApp.maxAttempts,
        config.monitorConfig.createApp.interval
      ).interruptAfter(config.monitorConfig.createApp.interruptAfter).compile.lastOrError

      _ <-
        if (!isDone) {
          val msg =
            s"Galaxy VM installation has failed or timed out for app ${app.appName.value} in project ${googleProject.value}"
          logger.error(ctx.loggingCtx)(msg) >>
            F.raiseError[Unit](AppCreationException(msg, traceId = Some(ctx.traceId)))
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
    val chart = config.cromwellAppConfig.chart

    for {
      ctx <- ev.ask

      _ <- logger.info(ctx.loggingCtx)(
        s"Installing helm chart ${chart} for Cromwell app ${appName.value} in cluster ${cluster.getClusterId.toString}"
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
        new RuntimeException("trying to create a non google runtime in GKEInterpreter. This should never happen")
      )
      // Poll the app until it starts up
      last <- streamFUntilDone(
        config.cromwellAppConfig.services
          .map(_.name)
          .traverse(s => appDao.isProxyAvailable(googleProject, appName, s, ctx.traceId)),
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
    autopilot: Option[Autopilot],
    bucketNameToMount: Option[GcsBucketName]
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask

      _ <- logger.info(ctx.loggingCtx)(
        s"Installing helm chart for Allowed app ${appName.value} in cluster ${cluster.getClusterId.toString}"
      )

      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(cluster.cloudContext),
        new RuntimeException("trying to create a non google runtime in GKEInterpreter. This should never happen")
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

      chartValues = buildAllowedAppChartOverrideValuesString(
        config,
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
        autopilot,
        bucketNameToMount
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
          .traverse(s => appDao.isProxyAvailable(googleProject, appName, s, ctx.traceId)),
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
        new RuntimeException("trying to create a non google runtime in GKEInterpreter. This should never happen")
      )
      // Poll app until it starts up
      last <- streamFUntilDone(
        descriptor.services.keys.toList.traverse(s =>
          appDao.isProxyAvailable(googleProject, appName, ServiceName(s), ctx.traceId)
        ),
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

  private[util] def getNodepoolServiceAccount(
    googleProject: GoogleProject
  )(implicit ev: Ask[F, AppContext]): F[Option[String]] = {
    val defaultSaEmail = s"gke-node-default-sa@${googleProject.value}.iam.gserviceaccount.com"

    for {
      ctx <- ev.ask

      // Check if the default service account exists in Google IAM
      serviceAccountExists <- F.fromFuture(
        F.delay(
          googleIamDAO
            .findServiceAccount(googleProject, WorkbenchEmail(defaultSaEmail))
            .map(_.isDefined)
            .recover { case _ => false }
        )
      )
      _ <- logger.info(ctx.loggingCtx)(
        s"[AN-276] Service account exists in project ${googleProject.value}: $serviceAccountExists"
      )

      // If service account exists, use it. Otherwise use default compute SA
    } yield
      if (serviceAccountExists) {
        Some(defaultSaEmail)
      } else None
  }

  private[util] def buildGoogleNodepool(
    nodepool: Nodepool,
    serviceAccount: Option[String]
  ): com.google.container.v1.NodePool = {

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
    serviceAccount: Option[String]
  ): com.google.api.services.container.model.NodePool = {
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
                                      cromwellAppConfig: CromwellAppConfig,
                                      customAppConfig: CustomAppConfig,
                                      allowedAppConfig: AllowedAppConfig,
                                      monitorConfig: AppMonitorConfig,
                                      clusterConfig: KubernetesClusterConfig,
                                      proxyConfig: ProxyConfig,
                                      galaxyDiskConfig: GalaxyDiskConfig,
                                      galaxyVmConfig: GalaxyVmConfig
)

final case class TerraAppSetupChartConfig(
  chartName: ChartName,
  chartVersion: ChartVersion
)
