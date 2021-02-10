package org.broadinstitute.dsde.workbench
package leonardo
package util

import java.util.Base64
import _root_.io.chrisdavenport.log4cats.StructuredLogger
import cats.Parallel
import cats.effect.{Async, Blocker, ConcurrentEffect, ContextShift, IO, Timer}
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
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.{
  streamFUntilDone,
  tracedRetryGoogleF,
  DiskName,
  KubernetesClusterNotFoundException,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.dao.GalaxyDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, kubernetesClusterQuery, nodepoolQuery, _}
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppNotFoundException
import org.broadinstitute.dsde.workbench.model.{IP, WorkbenchEmail}
import org.broadinstitute.dsp.{AuthContext, ChartName, ChartVersion, HelmAlgebra, Release}

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

class GKEInterpreter[F[_]: Parallel: ContextShift: Timer](
  config: GKEInterpreterConfig,
  vpcAlg: VPCAlgebra[F],
  gkeService: org.broadinstitute.dsde.workbench.google2.GKEService[F],
  kubeService: org.broadinstitute.dsde.workbench.google2.KubernetesService[F],
  helmClient: HelmAlgebra[F],
  galaxyDAO: GalaxyDAO[F],
  credentials: GoogleCredentials,
  googleIamDAO: GoogleIamDAO,
  blocker: Blocker,
  nodepoolLock: KeyLock[F, KubernetesClusterId]
)(implicit val executionContext: ExecutionContext,
  contextShift: ContextShift[IO],
  logger: StructuredLogger[F],
  dbRef: DbReference[F],
  F: ConcurrentEffect[F])
    extends GKEAlgebra[F] {

  override def createCluster(params: CreateClusterParams)(
    implicit ev: Ask[F, AppContext]
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
        s"Beginning cluster creation for cluster ${dbCluster.getGkeClusterId.toString}"
      )

      // Get nodepools to pass in the create cluster request
      nodepools = dbCluster.nodepools
        .filter(n => params.nodepoolsToCreate.contains(n.id))
        .map(buildLegacyGoogleNodepool)

      _ <- if (nodepools.size != params.nodepoolsToCreate.size)
        F.raiseError[Unit](
          ClusterCreationException(
            s"CreateCluster was called with nodepools that are not present in the database for cluster ${dbCluster.getGkeClusterId.toString} | trace id: ${ctx.traceId}"
          )
        )
      else F.unit

      // Set up VPC and firewall
      (network, subnetwork) <- vpcAlg.setUpProjectNetwork(
        SetUpProjectNetworkParams(params.googleProject)
      )
      _ <- vpcAlg.setUpProjectFirewalls(
        SetUpProjectFirewallsParams(params.googleProject, network)
      )

      kubeNetwork = KubernetesNetwork(dbCluster.googleProject, network)
      kubeSubNetwork = KubernetesSubNetwork(dbCluster.googleProject, dbCluster.region, subnetwork)

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
      req = KubernetesCreateClusterRequest(dbCluster.googleProject, dbCluster.location, legacyCreateClusterRec)
      //the Operation will be none if we get a 409, indicating we have already created this cluster
      operationOpt <- gkeService.createCluster(req)

    } yield operationOpt.map(op =>
      CreateClusterResult(KubernetesOperationId(dbCluster.googleProject, dbCluster.location, op.getName),
                          kubeNetwork,
                          kubeSubNetwork)
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
        s"Polling cluster creation for cluster ${dbCluster.getGkeClusterId.toString}"
      )

      defaultNodepool <- F.fromOption(dbCluster.nodepools.find(_.isDefault),
                                      DefaultNodepoolNotFoundException(dbCluster.id))

      // Poll GKE until completion
      lastOp <- gkeService
        .pollOperation(
          params.createResult.op,
          config.monitorConfig.clusterCreate.interval,
          config.monitorConfig.clusterCreate.maxAttempts
        )
        .compile
        .lastOrError

      _ <- if (lastOp.isDone)
        logger.info(ctx.loggingCtx)(
          s"Create cluster operation has finished for cluster ${dbCluster.getGkeClusterId.toString}"
        )
      else
        logger.error(ctx.loggingCtx)(
          s"Create cluster operation has failed for cluster ${dbCluster.getGkeClusterId.toString}"
        ) >>
          // Note LeoPubsubMessageSubscriber will transition things to Error status if an exception is thrown
          F.raiseError[Unit](
            ClusterCreationException(
              s"Failed to poll cluster creation operation to completion for cluster ${dbCluster.getGkeClusterId.toString} | trace id: ${ctx.traceId}"
            )
          )

      // Resolve the cluster in Google
      googleClusterOpt <- gkeService.getCluster(dbCluster.getGkeClusterId)
      googleCluster <- F.fromOption(
        googleClusterOpt,
        ClusterCreationException(
          s"Cluster not found in Google: ${dbCluster.getGkeClusterId.toString} | trace id: ${ctx.traceId}"
        )
      )

      _ <- logger.info(ctx.loggingCtx)(
        s"Successfully created cluster ${dbCluster.getGkeClusterId.toString}!"
      )

      // TODO: Handle the case where currently, if ingress installation fails, the cluster is marked as `Error`ed
      // and users can no longer create apps in the cluster's project
      // helm install nginx
      loadBalancerIp <- installNginx(dbCluster, googleCluster)

      _ <- kubernetesClusterQuery
        .updateAsyncFields(
          dbCluster.id,
          KubernetesClusterAsyncFields(
            IP(loadBalancerIp.asString),
            IP(googleCluster.getEndpoint),
            NetworkFields(
              params.createResult.network.name,
              params.createResult.subnetwork.name,
              Config.vpcConfig.subnetworkIpRange
            )
          )
        )
        .transaction
      _ <- kubernetesClusterQuery.updateStatus(dbCluster.id, KubernetesClusterStatus.Running).transaction
      _ <- if (params.isNodepoolPrecreate) {
        (nodepoolQuery.updateStatus(defaultNodepool.id, NodepoolStatus.Running) >>
          nodepoolQuery.updateStatuses(dbCluster.nodepools.filterNot(_.isDefault).map(_.id), NodepoolStatus.Unclaimed)).transaction
      } else {
        nodepoolQuery.updateStatuses(dbCluster.nodepools.map(_.id), NodepoolStatus.Running).transaction
      }

    } yield ()

  override def createAndPollNodepool(params: CreateNodepoolParams)(
    implicit ev: Ask[F, AppContext]
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
        s"Beginning nodepool creation for nodepool ${dbNodepool.nodepoolName.value} in cluster ${dbCluster.getGkeClusterId.toString}"
      )

      req = KubernetesCreateNodepoolRequest(
        dbCluster.getGkeClusterId,
        buildGoogleNodepool(dbNodepool)
      )

      operationOpt <- nodepoolLock.withKeyLock(dbCluster.getGkeClusterId) {
        for {
          opOpt <- gkeService.createNodepool(req)
          lastOpOpt <- opOpt.traverse { op =>
            gkeService
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

  override def createAndPollApp(params: CreateAppParams)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask

      // Grab records from the database
      dbAppOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(params.googleProject, params.appName).transaction
      dbApp <- F.fromOption(dbAppOpt, AppNotFoundException(params.googleProject, params.appName, ctx.traceId))

      app = dbApp.app
      namespaceName = app.appResources.namespace.name
      dbCluster = dbApp.cluster
      gkeClusterId = dbCluster.getGkeClusterId
      googleProject = params.googleProject

      // Create namespace and secrets
      _ <- logger.info(ctx.loggingCtx)(
        s"Creating namespace ${namespaceName.value} and secrets for app ${app.appName.value} in cluster ${gkeClusterId.toString}"
      )

      _ <- kubeService.createNamespace(gkeClusterId, KubernetesNamespace(namespaceName))

      // Create KSA
      ksaName = config.galaxyAppConfig.serviceAccount
      gsa = dbApp.app.googleServiceAccount

      // Resolve the cluster in Google
      googleClusterOpt <- gkeService.getCluster(gkeClusterId)
      googleCluster <- F.fromOption(
        googleClusterOpt,
        ClusterCreationException(
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
          )
        )
        .run(helmAuthContext)
      // update KSA in DB
      _ <- appQuery.updateKubernetesServiceAccount(app.id, ksaName).transaction

      // Associate GSA to newly created KSA
      // This string is constructed based on Google requirements to associate a GSA to a KSA
      // (https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity#creating_a_relationship_between_ksas_and_gsas)
      ksaToGsa = s"${googleProject.value}.svc.id.goog[${namespaceName.value}/${ksaName.value}]"
      call = Async[F].liftIO(
        IO.fromFuture(
          IO(
            googleIamDAO.addIamPolicyBindingOnServiceAccount(googleProject,
                                                             gsa,
                                                             WorkbenchEmail(ksaToGsa),
                                                             Set("roles/iam.workloadIdentityUser"))
          )
        )
      )
      retryConfig = RetryPredicates.retryConfigWithPredicates(
        when409
      )
      _ <- tracedRetryGoogleF(retryConfig)(
        call,
        s"googleIamDAO.addIamPolicyBindingOnServiceAccount for GSA ${gsa.value} & KSA ${ksaName.value}"
      ).compile.lastOrError

      // helm install galaxy and wait
      _ <- installGalaxy(
        helmAuthContext,
        app.appName,
        app.release,
        dbCluster,
        dbApp.nodepool.nodepoolName,
        namespaceName,
        app.auditInfo.creator,
        app.customEnvironmentVariables,
        ksaName,
        nfsDisk
      )

      _ <- logger.info(ctx.loggingCtx)(
        s"Finished app creation for app ${app.appName.value} in cluster ${gkeClusterId.toString}"
      )
      _ <- appQuery.updateStatus(params.appId, AppStatus.Running).transaction
    } yield ()

  override def deleteAndPollCluster(params: DeleteClusterParams)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      dbClusterOpt <- kubernetesClusterQuery.getMinimalClusterById(params.clusterId).transaction
      dbCluster <- F.fromOption(
        dbClusterOpt,
        KubernetesClusterNotFoundException(s"Cluster with id ${params.clusterId} not found in database")
      )
      //the operation will be None if the cluster is not found and we have already deleted it
      operationOpt <- gkeService.deleteCluster(dbCluster.getGkeClusterId)
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
        s"Beginning nodepool deletion for nodepool ${dbNodepool.nodepoolName.value} in cluster ${dbCluster.getGkeClusterId.toString}"
      )

      operationOpt <- nodepoolLock.withKeyLock(dbCluster.getGkeClusterId) {
        for {
          operationOpt <- gkeService.deleteNodepool(
            NodepoolId(dbCluster.getGkeClusterId, dbNodepool.nodepoolName)
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

      _ <- operationOpt.traverse(_ => nodepoolQuery.markAsDeleted(params.nodepoolId, ctx.now).transaction)
    } yield ()

  // This function DOES NOT update the app status to deleted after polling is complete
  // It decouples the AppStatus from the kubernetes entity, and makes it more representative of the app from the user's perspective
  // Currently, the only caller of this function updates the status after the nodepool is also deleted
  override def deleteAndPollApp(params: DeleteAppParams)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask

      dbAppOpt <- KubernetesServiceDbQueries.getFullAppByName(params.googleProject, params.appId).transaction
      dbApp <- F.fromOption(dbAppOpt, AppNotFoundException(params.googleProject, params.appName, ctx.traceId))

      app = dbApp.app
      namespaceName = app.appResources.namespace.name
      dbCluster = dbApp.cluster
      gkeClusterId = dbCluster.getGkeClusterId

      _ <- logger.info(ctx.loggingCtx)(
        s"Beginning app deletion for app ${app.appName.value} in cluster ${gkeClusterId.toString}"
      )

      // Resolve the cluster in Google
      googleClusterOpt <- gkeService.getCluster(gkeClusterId)

      // helm uninstall galaxy and wait
      _ <- googleClusterOpt.traverse(googleCluster =>
        for {
          helmAuthContext <- getHelmAuthContext(googleCluster, dbCluster, namespaceName)
          _ <- helmClient
            .uninstall(
              getTerraAppSetupChartReleaseName(app.release),
              config.galaxyAppConfig.uninstallKeepHistory
            )
            .run(helmAuthContext)
          _ <- uninstallGalaxy(helmAuthContext, dbCluster, app.appName, app.release, namespaceName)
        } yield ()
      )

      // delete the namespace only after the helm uninstall completes
      _ <- kubeService.deleteNamespace(dbApp.cluster.getGkeClusterId,
                                       KubernetesNamespace(dbApp.app.appResources.namespace.name))
      _ <- logger.info(ctx.loggingCtx)(
        s"Delete app operation has finished for app ${app.appName.value} in cluster ${gkeClusterId.toString}"
      )

      _ <- if (!params.errorAfterDelete) {
        F.unit
      } else {
        appQuery.updateStatus(dbApp.app.id, AppStatus.Error).transaction.void
      }
    } yield ()

  override def stopAndPollApp(params: StopAppParams)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask

      dbAppOpt <- KubernetesServiceDbQueries.getFullAppByName(params.googleProject, params.appId).transaction
      dbApp <- F.fromOption(dbAppOpt, AppNotFoundException(params.googleProject, params.appName, ctx.traceId))
      dbNodepool = dbApp.nodepool
      dbCluster = dbApp.cluster
      nodepoolId = NodepoolId(dbCluster.getGkeClusterId, dbNodepool.nodepoolName)

      _ <- logger.info(ctx.loggingCtx)(
        s"Stopping app ${dbApp.app.appName.value} in cluster ${dbCluster.getGkeClusterId.toString}"
      )

      _ <- nodepoolQuery.updateStatus(dbNodepool.id, NodepoolStatus.Provisioning).transaction

      // If autoscaling is enabled, disable it first
      _ <- if (dbNodepool.autoscalingEnabled) {
        nodepoolLock.withKeyLock(dbCluster.getGkeClusterId) {
          for {
            op <- gkeService.setNodepoolAutoscaling(
              nodepoolId,
              NodePoolAutoscaling.newBuilder().setEnabled(false).build()
            )
            lastOp <- gkeService
              .pollOperation(
                KubernetesOperationId(params.googleProject, dbCluster.location, op.getName),
                config.monitorConfig.setNodepoolAutoscaling.interval,
                config.monitorConfig.setNodepoolAutoscaling.maxAttempts
              )
              .compile
              .lastOrError
            _ <- if (lastOp.isDone)
              logger.info(
                s"setNodepoolAutoscaling operation has finished for nodepool ${dbNodepool.id} | trace id: ${ctx.traceId}"
              )
            else
              logger.error(
                s"setNodepoolAutoscaling operation has failed or timed out for nodepool ${dbNodepool.id} | trace id: ${ctx.traceId}"
              ) >>
                F.raiseError[Unit](NodepoolStopException(dbNodepool.id))
          } yield ()
        }
      } else F.unit

      // Scale the nodepool to zero nodes
      _ <- nodepoolLock.withKeyLock(dbCluster.getGkeClusterId) {
        for {
          op <- gkeService.setNodepoolSize(nodepoolId, 0)
          lastOp <- gkeService
            .pollOperation(
              KubernetesOperationId(params.googleProject, dbCluster.location, op.getName),
              config.monitorConfig.scaleNodepool.interval,
              config.monitorConfig.scaleNodepool.maxAttempts
            )
            .compile
            .lastOrError
          _ <- if (lastOp.isDone)
            logger.info(
              s"setNodepoolSize operation has finished for nodepool ${dbNodepool.id} | trace id: ${ctx.traceId}"
            )
          else
            logger.error(
              s"setNodepoolSize operation has failed or timed out for nodepool ${dbNodepool.id} | trace id: ${ctx.traceId}"
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

  override def startAndPollApp(params: StartAppParams)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask

      dbAppOpt <- KubernetesServiceDbQueries.getFullAppByName(params.googleProject, params.appId).transaction
      dbApp <- F.fromOption(dbAppOpt, AppNotFoundException(params.googleProject, params.appName, ctx.traceId))
      dbNodepool = dbApp.nodepool
      dbCluster = dbApp.cluster
      nodepoolId = NodepoolId(dbCluster.getGkeClusterId, dbNodepool.nodepoolName)

      _ <- logger.info(
        s"Starting app ${dbApp.app.appName.value} in cluster ${dbCluster.getGkeClusterId.toString} | trace id: ${ctx.traceId}"
      )

      _ <- nodepoolQuery.updateStatus(dbNodepool.id, NodepoolStatus.Provisioning).transaction

      // First scale the node pool to > 0 nodes
      _ <- nodepoolLock.withKeyLock(dbCluster.getGkeClusterId) {
        for {
          op <- gkeService.setNodepoolSize(
            nodepoolId,
            dbNodepool.numNodes.amount
          )
          lastOp <- gkeService
            .pollOperation(
              KubernetesOperationId(params.googleProject, dbCluster.location, op.getName),
              config.monitorConfig.scaleNodepool.interval,
              config.monitorConfig.scaleNodepool.maxAttempts
            )
            .compile
            .lastOrError
          _ <- if (lastOp.isDone)
            logger.info(
              s"setNodepoolSize operation has finished for nodepool ${dbNodepool.id} | trace id: ${ctx.traceId}"
            )
          else
            logger.error(
              s"setNodepoolSize operation has failed or timed out for nodepool ${dbNodepool.id} | trace id: ${ctx.traceId}"
            ) >>
              F.raiseError[Unit](NodepoolStartException(dbNodepool.id))
        } yield ()
      }

      // Poll galaxy until it starts up
      // TODO potentially add other status checks for pod readiness, beyond just HTTP polling the galaxy-web service
      isDone <- streamFUntilDone(
        galaxyDAO.isProxyAvailable(dbCluster.googleProject, dbApp.app.appName),
        config.monitorConfig.startApp.maxAttempts,
        config.monitorConfig.startApp.interval
      ).compile.lastOrError

      _ <- if (!isDone) {
        val msg =
          s"Galaxy startup has failed or timed out for app ${dbApp.app.appName.value} in cluster ${dbCluster.getGkeClusterId.toString} | trace id: ${ctx.traceId}"
        logger.error(msg) >>
          F.raiseError[Unit](AppStartException(msg))
      } else F.unit

      // The app is Running at this point and Galaxy can be used
      _ <- appQuery.updateStatus(params.appId, AppStatus.Running).transaction

      // If autoscaling should be enabled, enable it now. Galaxy can still be used while this is in progress
      _ <- if (dbNodepool.autoscalingEnabled) {
        dbNodepool.autoscalingConfig.traverse_ { autoscalingConfig =>
          nodepoolLock.withKeyLock(dbCluster.getGkeClusterId) {
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

              lastOp <- gkeService
                .pollOperation(
                  KubernetesOperationId(params.googleProject, dbCluster.location, op.getName),
                  config.monitorConfig.setNodepoolAutoscaling.interval,
                  config.monitorConfig.setNodepoolAutoscaling.maxAttempts
                )
                .compile
                .lastOrError
              _ <- if (lastOp.isDone)
                logger.info(
                  s"setNodepoolAutoscaling operation has finished for nodepool ${dbNodepool.id} | trace id: ${ctx.traceId}"
                )
              else
                logger.error(
                  s"setNodepoolAutoscaling operation has failed or timed out for nodepool ${dbNodepool.id} | trace id: ${ctx.traceId}"
                ) >>
                  F.raiseError[Unit](NodepoolStartException(dbNodepool.id))
            } yield ()
          }
        }
      } else F.unit

      // Finally update the nodepool status to Running
      _ <- nodepoolQuery.updateStatus(dbNodepool.id, NodepoolStatus.Running).transaction
    } yield ()

  private[leonardo] def buildGalaxyPostgresDisk(zone: ZoneName, namespaceName: NamespaceName): Disk =
    Disk
      .newBuilder()
      .setName(getGalaxyPostgresDiskName(namespaceName).value)
      .setZone(zone.value)
      .setSizeGb(config.galaxyDiskConfig.postgresDiskSizeGB.gb.toString)
      .setPhysicalBlockSizeBytes(config.galaxyDiskConfig.postgresDiskBlockSize.bytes.toString)
      .build()

  private[leonardo] def getGalaxyPostgresDiskName(namespaceName: NamespaceName): DiskName =
    DiskName(s"${namespaceName.value}-${config.galaxyDiskConfig.postgresDiskNameSuffix}")

  private[util] def installNginx(dbCluster: KubernetesCluster,
                                 googleCluster: Cluster)(implicit ev: Ask[F, AppContext]): F[IP] =
    for {
      ctx <- ev.ask

      // Create namespace for nginx
      _ <- logger.info(
        s"Creating namespace ${config.ingressConfig.namespace.value} in cluster ${dbCluster.getGkeClusterId.toString} | trace id: ${ctx.traceId}"
      )
      _ <- kubeService.createNamespace(dbCluster.getGkeClusterId, KubernetesNamespace(config.ingressConfig.namespace))

      _ <- logger.info(
        s"Installing ingress helm chart ${config.ingressConfig.chart} in cluster ${dbCluster.getGkeClusterId.toString} | trace id: ${ctx.traceId}"
      )

      helmAuthContext <- getHelmAuthContext(googleCluster, dbCluster, config.ingressConfig.namespace)

      // Invoke helm
      _ <- helmClient
        .installChart(
          config.ingressConfig.release,
          config.ingressConfig.chartName,
          config.ingressConfig.chartVersion,
          org.broadinstitute.dsp.Values(config.ingressConfig.values.map(_.value).mkString(","))
        )
        .run(helmAuthContext)

      // Monitor nginx until public IP is accessible
      loadBalancerIpOpt <- streamFUntilDone(
        kubeService.getServiceExternalIp(dbCluster.getGkeClusterId,
                                         KubernetesNamespace(config.ingressConfig.namespace),
                                         config.ingressConfig.loadBalancerService),
        config.monitorConfig.createIngress.maxAttempts,
        config.monitorConfig.createIngress.interval
      ).compile.lastOrError

      loadBalancerIp <- F.fromOption(
        loadBalancerIpOpt,
        ClusterCreationException(
          s"Load balancer IP did not become available after ${config.monitorConfig.createIngress.totalDuration} in cluster ${dbCluster.getGkeClusterId.toString} | trace id: ${ctx.traceId}"
        )
      )

      _ <- logger.info(
        s"Successfully obtained public IP ${loadBalancerIp.asString} for cluster ${dbCluster.getGkeClusterId.toString} | trace id: ${ctx.traceId}"
      )
    } yield loadBalancerIp

  private[util] def installGalaxy(helmAuthContext: AuthContext,
                                  appName: AppName,
                                  release: Release,
                                  dbCluster: KubernetesCluster,
                                  nodepoolName: NodepoolName,
                                  namespaceName: NamespaceName,
                                  userEmail: WorkbenchEmail,
                                  customEnvironmentVariables: Map[String, String],
                                  kubernetesServiceAccount: ServiceAccountName,
                                  nfsDisk: PersistentDisk)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask

      _ <- logger.info(
        s"Installing helm chart ${config.galaxyAppConfig.chart} for app ${appName.value} in cluster ${dbCluster.getGkeClusterId.toString} | trace id: ${ctx.traceId}"
      )

      chartValues = buildGalaxyChartOverrideValuesString(appName,
                                                         release,
                                                         dbCluster,
                                                         nodepoolName,
                                                         userEmail,
                                                         customEnvironmentVariables,
                                                         kubernetesServiceAccount,
                                                         namespaceName,
                                                         nfsDisk)

      _ <- logger.info(ctx.loggingCtx)(
        s"Chart override values are: ${chartValues}"
      )

      // Invoke helm
      _ <- helmClient
        .installChart(
          release,
          config.galaxyAppConfig.chartName,
          config.galaxyAppConfig.chartVersion,
          org.broadinstitute.dsp.Values(chartValues)
        )
        .run(helmAuthContext)

      // Poll galaxy until it starts up
      // TODO potentially add other status checks for pod readiness, beyond just HTTP polling the galaxy-web service
      isDone <- streamFUntilDone(galaxyDAO.isProxyAvailable(dbCluster.googleProject, appName),
                                 config.monitorConfig.createApp.maxAttempts,
                                 config.monitorConfig.createApp.interval).compile.lastOrError

      _ <- if (!isDone) {
        val msg =
          s"Galaxy installation has failed or timed out for app ${appName.value} in cluster ${dbCluster.getGkeClusterId.toString} | trace id: ${ctx.traceId}"
        logger.error(msg) >>
          F.raiseError[Unit](AppCreationException(msg))
      } else F.unit

    } yield ()

  private[util] def uninstallGalaxy(helmAuthContext: AuthContext,
                                    dbCluster: KubernetesCluster,
                                    appName: AppName,
                                    release: Release,
                                    namespaceName: NamespaceName)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask

      _ <- logger.info(ctx.loggingCtx)(
        s"Uninstalling helm chart ${config.galaxyAppConfig.chart} for app ${appName.value} in cluster ${dbCluster.getGkeClusterId.toString} | trace id: ${ctx.traceId}"
      )

      // Invoke helm
      _ <- helmClient
        .uninstall(release, config.galaxyAppConfig.uninstallKeepHistory)
        .run(helmAuthContext)

      last <- streamFUntilDone(
        kubeService.listPodStatus(dbCluster.getGkeClusterId, KubernetesNamespace(namespaceName)),
        config.monitorConfig.deleteApp.maxAttempts,
        config.monitorConfig.deleteApp.interval
      ).compile.lastOrError

      _ <- if (!podDoneCheckable.isDone(last)) {
        val msg =
          s"Galaxy deletion has failed or timed out for app ${appName.value} in cluster ${dbCluster.getGkeClusterId.toString}. The following pods are not in a terminal state: ${last
            .filterNot(isPodDone)
            .map(_.name.value)
            .mkString(", ")} | trace id: ${ctx.traceId}"
        logger.error(msg) >>
          F.raiseError[Unit](AppDeletionException(msg))
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
                                  Base64.getDecoder.decode(googleCluster.getMasterAuth.getClusterCaCertificate),
                                  blocker)

      helmAuthContext = AuthContext(
        org.broadinstitute.dsp.Namespace(namespaceName.value),
        org.broadinstitute.dsp.KubeToken(credentials.getAccessToken.getTokenValue),
        org.broadinstitute.dsp.KubeApiServer("https://" + googleCluster.getEndpoint),
        org.broadinstitute.dsp.CaCertFile(caCertFile.toAbsolutePath)
      )

      _ <- logger.info(s"Helm auth context for cluster ${dbCluster.getGkeClusterId.toString}: ${helmAuthContext
        .copy(kubeToken = org.broadinstitute.dsp.KubeToken("<redacted>"))} | trace id: ${ctx.traceId}")

    } yield helmAuthContext

  private[util] def buildGoogleNodepool(nodepool: Nodepool): com.google.container.v1.NodePool = {
    val nodepoolBuilder = NodePool
      .newBuilder()
      .setConfig(
        NodeConfig
          .newBuilder()
          .setMachineType(nodepool.machineType.value)
      )
      .setInitialNodeCount(nodepool.numNodes.amount)
      .setName(nodepool.nodepoolName.value)
      .setManagement(
        NodeManagement
          .newBuilder()
          .setAutoUpgrade(true)
          .setAutoRepair(true)
      )

    val builderWithAutoscaling = nodepool.autoscalingConfig.fold(nodepoolBuilder)(config =>
      nodepool.autoscalingEnabled match {
        case true =>
          nodepoolBuilder.setAutoscaling(
            NodePoolAutoscaling
              .newBuilder()
              .setEnabled(true)
              .setMinNodeCount(config.autoscalingMin.amount)
              .setMaxNodeCount(config.autoscalingMax.amount)
          )
        case false => nodepoolBuilder
      }
    )

    builderWithAutoscaling.build()
  }

  private[util] def buildLegacyGoogleNodepool(nodepool: Nodepool): com.google.api.services.container.model.NodePool = {
    val legacyGoogleNodepool = new com.google.api.services.container.model.NodePool()
      .setConfig(new com.google.api.services.container.model.NodeConfig().setMachineType(nodepool.machineType.value))
      .setInitialNodeCount(nodepool.numNodes.amount)
      .setName(nodepool.nodepoolName.value)
      .setManagement(
        new com.google.api.services.container.model.NodeManagement().setAutoUpgrade(true).setAutoRepair(true)
      )

    nodepool.autoscalingConfig.fold(legacyGoogleNodepool)(config =>
      nodepool.autoscalingEnabled match {
        case true =>
          legacyGoogleNodepool.setAutoscaling(
            new com.google.api.services.container.model.NodePoolAutoscaling()
              .setEnabled(true)
              .setMinNodeCount(config.autoscalingMin.amount)
              .setMaxNodeCount(config.autoscalingMax.amount)
          )
        case false => legacyGoogleNodepool
      }
    )
  }

  private[util] def buildGalaxyChartOverrideValuesString(appName: AppName,
                                                         release: Release,
                                                         cluster: KubernetesCluster,
                                                         nodepoolName: NodepoolName,
                                                         userEmail: WorkbenchEmail,
                                                         customEnvironmentVariables: Map[String, String],
                                                         ksa: ServiceAccountName,
                                                         namespaceName: NamespaceName,
                                                         nfsDisk: PersistentDisk): String = {
    val k8sProxyHost = kubernetesProxyHost(cluster, config.proxyConfig.proxyDomain).address
    val leoProxyhost = config.proxyConfig.getProxyServerHostName
    val ingressPath = s"/proxy/google/v1/apps/${cluster.googleProject.value}/${appName.value}/galaxy"
    val workspaceName = customEnvironmentVariables.getOrElse("WORKSPACE_NAME", "")

    // Custom EV configs
    val configs = customEnvironmentVariables.toList.zipWithIndex.flatMap {
      case ((k, v), i) =>
        List(
          raw"""configs.$k=$v""",
          raw"""extraEnv[$i].name=$k""",
          raw"""extraEnv[$i].valueFrom.configMapKeyRef.name=${release.asString}-galaxykubeman-configs""",
          raw"""extraEnv[$i].valueFrom.configMapKeyRef.key=$k"""
        )
    }

    // Using the string interpolator raw""" since the chart keys include quotes to escape Helm
    // value override special characters such as '.'
    // https://helm.sh/docs/intro/using_helm/#the-format-and-limitations-of---set
    (List(
      // Storage class configs
      raw"""nfs.storageClass.name=nfs-${release.asString}""",
      raw"""cvmfs.repositories.cvmfs-gxy-data-${release.asString}=data.galaxyproject.org""",
      raw"""cvmfs.repositories.cvmfs-gxy-main-${release.asString}=main.galaxyproject.org""",
      raw"""cvmfs.cache.alienCache.storageClass=nfs-${release.asString}""",
      raw"""galaxy.persistence.storageClass=nfs-${release.asString}""",
      raw"""galaxy.cvmfs.data.pvc.storageClassName=cvmfs-gxy-data-${release.asString}""",
      raw"""galaxy.cvmfs.main.pvc.storageClassName=cvmfs-gxy-main-${release.asString}""",
      // Node selector config: this ensures the app is run on the user's nodepool
      raw"""galaxy.nodeSelector.cloud\.google\.com/gke-nodepool=${nodepoolName.value}""",
      raw"""nfs.nodeSelector.cloud\.google\.com/gke-nodepool=${nodepoolName.value}""",
      raw"""galaxy.configs.job_conf\.yml.runners.k8s.k8s_node_selector=cloud.google.com/gke-nodepool: ${nodepoolName.value}""",
      // Ingress configs
      raw"""galaxy.ingress.path=${ingressPath}""",
      raw"""galaxy.ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=https://${k8sProxyHost}""",
      raw"""galaxy.ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-to=${leoProxyhost}""",
      raw"""galaxy.ingress.hosts[0]=${k8sProxyHost}""",
      // Galaxy configs
      raw"""galaxy.configs.galaxy\.yml.galaxy.single_user=${userEmail.value}""",
      raw"""galaxy.configs.galaxy\.yml.galaxy.admin_users=${userEmail.value}""",
      raw"""galaxy.terra.launch.workspace=${workspaceName}""",
      raw"""galaxy.terra.launch.namespace=${cluster.googleProject.value}""",
      // Note most of the below file_sources configs are specified in galaxykubeman,
      // but helm can't update 1 item in a list if the value is an object.
      // See https://github.com/helm/helm/issues/7569
      raw"""galaxy.configs.file_sources_conf\.yml[0].api_url=${config.galaxyAppConfig.orchUrl}""",
      raw"""galaxy.configs.file_sources_conf\.yml[0].drs_url=${config.galaxyAppConfig.drsUrl}""",
      raw"""galaxy.configs.file_sources_conf\.yml[0].doc=${workspaceName}""",
      raw"""galaxy.configs.file_sources_conf\.yml[0].id=${workspaceName}""",
      raw"""galaxy.configs.file_sources_conf\.yml[0].workspace=${workspaceName}""",
      raw"""galaxy.configs.file_sources_conf\.yml[0].namespace=${cluster.googleProject.value}""",
      raw"""galaxy.configs.file_sources_conf\.yml[0].type=anvil""",
      raw"""galaxy.configs.file_sources_conf\.yml[0].on_anvil=True""",
      // RBAC configs
      raw"""galaxy.rbac.enabled=false""",
      raw"""galaxy.rbac.serviceAccount=${ksa.value}""",
      raw"""rbac.serviceAccount=${ksa.value}""",
      // Persistence configs
      raw"""persistence.nfs.name=${namespaceName.value}-${config.galaxyDiskConfig.nfsPersistenceName}""",
      raw"""persistence.nfs.persistentVolume.extraSpec.gcePersistentDisk.pdName=${nfsDisk.name.value}""",
      raw"""persistence.nfs.size=${nfsDisk.size.gb.toString}Gi""",
      raw"""persistence.postgres.name=${namespaceName.value}-${config.galaxyDiskConfig.postgresPersistenceName}""",
      raw"""persistence.postgres.galaxyDatabasePassword=${config.galaxyAppConfig.postgresPassword}""",
      raw"""persistence.postgres.persistentVolume.extraSpec.gcePersistentDisk.pdName=${getGalaxyPostgresDiskName(
        namespaceName
      ).value}""",
      raw"""persistence.postgres.size=${config.galaxyDiskConfig.postgresDiskSizeGB.gb.toString}Gi""",
      raw"""nfs.persistence.existingClaim=${namespaceName.value}-${config.galaxyDiskConfig.nfsPersistenceName}-pvc""",
      raw"""nfs.persistence.size=${nfsDisk.size.gb.toString}Gi""",
      raw"""galaxy.postgresql.persistence.existingClaim=${namespaceName.value}-${config.galaxyDiskConfig.postgresPersistenceName}-pvc""",
      raw"""galaxy.persistence.size=200Gi"""
    ) ++ configs).mkString(",")
  }

  private def getTerraAppSetupChartReleaseName(appReleaseName: Release): Release =
    Release(s"${appReleaseName.asString}-setup-rls")

  private[util] def isPodDone(pod: KubernetesPodStatus): Boolean =
    pod.podStatus == PodStatus.Failed || pod.podStatus == PodStatus.Succeeded

  // DoneCheckable instances
  implicit private def optionDoneCheckable[A]: DoneCheckable[Option[A]] = (a: Option[A]) => a.isDefined
  implicit private def booleanDoneCheckable: DoneCheckable[Boolean] = identity[Boolean]
  implicit private def podDoneCheckable: DoneCheckable[List[KubernetesPodStatus]] =
    (ps: List[KubernetesPodStatus]) => ps.forall(isPodDone)
}

sealed trait AppProcessingException extends Exception {
  def getMessage: String
}

final case class ClusterCreationException(message: String) extends AppProcessingException {
  override def getMessage: String = message
}

final case class ClusterDeletionException(clusterId: KubernetesClusterLeoId) extends AppProcessingException {
  override def getMessage: String = s"Failed to poll clluster deletion operation to completion for cluster $clusterId"
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

final case class AppCreationException(message: String) extends AppProcessingException {
  override def getMessage: String = message
}

final case class AppDeletionException(message: String) extends AppProcessingException {
  override def getMessage: String = message
}

final case class AppStartException(message: String) extends AppProcessingException {
  override def getMessage: String = message
}

final case class DeleteNodepoolResult(nodepoolId: NodepoolLeoId,
                                      operation: com.google.container.v1.Operation,
                                      getAppResult: GetAppResult)

final case class GKEInterpreterConfig(terraAppSetupChartConfig: TerraAppSetupChartConfig,
                                      ingressConfig: KubernetesIngressConfig,
                                      galaxyAppConfig: GalaxyAppConfig,
                                      monitorConfig: AppMonitorConfig,
                                      clusterConfig: KubernetesClusterConfig,
                                      proxyConfig: ProxyConfig,
                                      galaxyDiskConfig: GalaxyDiskConfig)

final case class TerraAppSetupChartConfig(
  chartName: ChartName,
  chartVersion: ChartVersion
)
