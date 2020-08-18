package org.broadinstitute.dsde.workbench.leonardo
package util

import _root_.io.chrisdavenport.log4cats.StructuredLogger
import cats.Parallel
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.auth.oauth2.GoogleCredentials
import com.google.container.v1.MasterAuthorizedNetworksConfig.CidrBlock
import com.google.container.v1._
import fs2._
import org.broadinstitute.dsde.workbench.DoneCheckableInstances._
import org.broadinstitute.dsde.workbench.DoneCheckableSyntax._
import org.broadinstitute.dsde.workbench.google2.GKEModels._
import org.broadinstitute.dsde.workbench.google2.KubernetesClusterNotFoundException
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.{
  KubernetesNamespace,
  KubernetesSecret,
  KubernetesSecretType
}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.NamespaceName
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, kubernetesClusterQuery, nodepoolQuery, _}
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppNotFoundException
import org.broadinstitute.dsp.{AuthContext, HelmAlgebra}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

final case class GKEInterpreterConfig(securityFiles: SecurityFilesConfig,
                                      ingressConfig: KubernetesIngressConfig,
                                      monitorConfig: AppMonitorConfig,
                                      clusterConfig: KubernetesClusterConfig)
class GKEInterpreter[F[_]: Parallel: ContextShift: Timer](
  config: GKEInterpreterConfig,
  vpcAlg: VPCAlgebra[F],
  gkeService: org.broadinstitute.dsde.workbench.google2.GKEService[F],
  kubeService: org.broadinstitute.dsde.workbench.google2.KubernetesService[F],
  helmClient: HelmAlgebra[F],
  credentials: GoogleCredentials,
  blocker: Blocker
)(implicit val executionContext: ExecutionContext,
  logger: StructuredLogger[F],
  dbRef: DbReference[F],
  F: ConcurrentEffect[F])
    extends GKEAlgebra[F] {

  override def createAndPollCluster(params: CreateClusterParams)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- logger.info(
        s"Beginning cluster creation for cluster ${params.clusterId} in project ${params.googleProject} | trace id: ${ctx.traceId}"
      )

      // Grab records from the database
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(params.clusterId).transaction
      dbCluster <- F.fromOption(
        clusterOpt,
        KubernetesClusterNotFoundException(
          s"Failed kubernetes cluster creation. Cluster with id ${params.clusterId} not found in database"
        )
      )
      nodepools = dbCluster.nodepools
        .filter(n => params.nodepoolsToCreate.contains(n.id))
        .map(getGoogleNodepool)

      _ <- if (nodepools.size != params.nodepoolsToCreate.size)
        F.raiseError[Unit](
          ClusterCreationException(
            s"CreateCluster was called with nodepools that are not present in the database for cluster ${params.clusterId}. Nodepools to create: ${params.nodepoolsToCreate}"
          )
        )
      else F.unit

      defaultNodepool <- F.fromOption(dbCluster.nodepools.filter(_.isDefault).headOption,
                                      DefaultNodepoolNotFoundException(dbCluster.id))

      // Set up VPC and firewall
      (network, subnetwork) <- vpcAlg.setUpProjectNetwork(
        SetUpProjectNetworkParams(params.googleProject)
      )
      _ <- vpcAlg.setUpProjectFirewalls(
        SetUpProjectFirewallsParams(params.googleProject, network)
      )

      kubeNetwork = KubernetesNetwork(dbCluster.googleProject, network).idString
      kubeSubNetwork = KubernetesSubNetwork(dbCluster.googleProject, dbCluster.region, subnetwork).idString

      cidrBuilder = CidrBlock.newBuilder()
      _ <- F.delay(config.clusterConfig.authorizedNetworks.foreach(cidrIP => cidrBuilder.setCidrBlock(cidrIP.value)))

      createClusterReq = Cluster
        .newBuilder()
        .setName(dbCluster.clusterName.value)
        .addAllNodePools(
          nodepools.asJava
        )
        // all the below code corresponds to security recommendations
        .setLegacyAbac(LegacyAbac.newBuilder().setEnabled(false))
        .setNetwork(kubeNetwork)
        .setSubnetwork(kubeSubNetwork)
        .setNetworkPolicy(
          NetworkPolicy
            .newBuilder()
            .setEnabled(true)
        )
        .setMasterAuthorizedNetworksConfig(
          MasterAuthorizedNetworksConfig
            .newBuilder()
            .setEnabled(true)
            .addAllCidrBlocks(
              config.clusterConfig.authorizedNetworks
                .map(ip => CidrBlock.newBuilder().setCidrBlock(ip.value).build())
                .asJava
            )
        )
        .setIpAllocationPolicy( //otherwise it uses the legacy one, which is insecure. See https://cloud.google.com/kubernetes-engine/docs/how-to/alias-ips
          IPAllocationPolicy
            .newBuilder()
            .setUseIpAliases(true)
        )
        .build()

      // Submit request to GKE
      req = KubernetesCreateClusterRequest(dbCluster.googleProject, dbCluster.location, createClusterReq)
      op <- gkeService.createCluster(req)

      // Poll GKE until completion
      lastOp <- gkeService
        .pollOperation(
          KubernetesOperationId(dbCluster.googleProject, dbCluster.location, op),
          config.monitorConfig.clusterCreate.interval,
          config.monitorConfig.clusterCreate.maxAttempts
        )
        .compile
        .lastOrError

      _ <- if (lastOp.isDone)
        logger.info(
          s"Create cluster operation has finished for cluster ${params.clusterId} | trace id: ${ctx.traceId}"
        )
      else
        logger.error(
          s"Create cluster operation has failed for cluster ${params.clusterId} | trace id: ${ctx.traceId}"
        ) >>
          // Note LeoPubsubMessageSubscriber will transition things to Error status if an exception is thrown
          F.raiseError[Unit](
            ClusterCreationException(
              s"Failed to poll cluster creation operation to completion for cluster ${params.clusterId}"
            )
          )

      googleClusterOpt <- gkeService.getCluster(dbCluster.getGkeClusterId)
      googleCluster <- F.fromOption(
        googleClusterOpt,
        ClusterCreationException(
          s"Cluster not found in Google: ${dbCluster.getGkeClusterId} | trace id: ${ctx.traceId}"
        )
      )

      // Install nginx igress controller
      _ <- kubeService.createNamespace(dbCluster.getGkeClusterId, KubernetesNamespace(config.ingressConfig.namespace))

      _ <- F.delay(credentials.refreshIfExpired())

      helmAuthContext = AuthContext(
        org.broadinstitute.dsp.Namespace(config.ingressConfig.namespace.value),
        org.broadinstitute.dsp.KubeToken(credentials.getAccessToken.getTokenValue),
        org.broadinstitute.dsp.KubeApiServer("https://" + googleCluster.getEndpoint)
      )
      _ <- helmClient
        .installChart(
          org.broadinstitute.dsp.Release(config.ingressConfig.release.value),
          org.broadinstitute.dsp.Chart(config.ingressConfig.chart.value),
          org.broadinstitute.dsp.Values(config.ingressConfig.values.map(_.value).mkString(","))
        )
        .run(helmAuthContext)

      // Monitor nginx until public IP is accessible
      loadBalancerIpOpt <- (
        Stream.sleep_(config.monitorConfig.createIngress.interval) ++
          Stream.eval(
            kubeService.getServiceExternalIp(dbCluster.getGkeClusterId,
                                             KubernetesNamespace(config.ingressConfig.namespace),
                                             config.ingressConfig.loadBalancerService)
          )
      ).repeatN(config.monitorConfig.createIngress.maxAttempts)
        .takeThrough(!_.isDefined)
        .compile
        .lastOrError

      loadBalancerIp <- F.fromOption(
        loadBalancerIpOpt,
        ClusterCreationException(
          s"Load balancer IP did not become available after ${config.monitorConfig.createIngress.totalDuration} | trace id: ${ctx.traceId}"
        )
      )

      _ <- kubernetesClusterQuery
        .updateAsyncFields(
          dbCluster.id,
          KubernetesClusterAsyncFields(
            IP(loadBalancerIp.value),
            IP(googleCluster.getEndpoint),
            NetworkFields(
              network,
              subnetwork,
              Config.vpcConfig.subnetworkIpRange
            )
          )
        )
        .transaction
      _ <- kubernetesClusterQuery.updateStatus(dbCluster.id, KubernetesClusterStatus.Running).transaction
      _ <- nodepoolQuery.updateStatus(defaultNodepool.id, NodepoolStatus.Running).transaction
      _ <- if (params.isNodepoolPrecreate)
        nodepoolQuery.markAsUnclaimed(dbCluster.nodepools.filterNot(_.isDefault).map(_.id)).transaction
      else F.unit

    } yield ()

  override def createAndPollNodepool(params: CreateNodepoolParams)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- logger.info(
        s"Beginning nodepool creation for nodepool ${params.nodepoolId} in project ${params.googleProject} | trace id: ${ctx.traceId}"
      )
      dbNodepoolOpt <- nodepoolQuery.getMinimalById(params.nodepoolId).transaction
      dbNodepool <- F.fromOption(dbNodepoolOpt, NodepoolNotFoundException(params.nodepoolId))
      dbClusterOpt <- kubernetesClusterQuery.getMinimalClusterById(dbNodepool.clusterId).transaction
      dbCluster <- F.fromOption(
        dbClusterOpt,
        KubernetesClusterNotFoundException(s"Cluster with id ${dbNodepool.clusterId} not found in database")
      )
      req = KubernetesCreateNodepoolRequest(
        dbCluster.getGkeClusterId,
        getGoogleNodepool(dbNodepool)
      )
      op <- gkeService.createNodepool(req)
      lastOp <- gkeService
        .pollOperation(
          KubernetesOperationId(dbCluster.googleProject, dbCluster.location, op),
          config.monitorConfig.nodepoolCreate.interval,
          config.monitorConfig.nodepoolCreate.maxAttempts
        )
        .compile
        .lastOrError
      _ <- if (lastOp.isDone)
        logger.info(
          s"Nodepool creation operation has finished for nodepool ${params.nodepoolId} | trace id: ${ctx.traceId}"
        )
      else
        logger.error(
          s"Create nodepool operation has failed or timed out for nodepool ${params.nodepoolId} | trace id: ${ctx.traceId}"
        ) >>
          // Note LeoPubsubMessageSubscriber will transition things to Error status if an exception is thrown
          F.raiseError[Unit](NodepoolCreationException(params.nodepoolId))
      _ <- nodepoolQuery.updateStatus(params.nodepoolId, NodepoolStatus.Running).transaction
    } yield ()

  override def createAndPollApp(params: CreateAppParams)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- logger.info(
        s"Beginning app creation for app ${params.appId} in project ${params.googleProject} | trace id: ${ctx.traceId}"
      )
      dbAppOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(params.googleProject, params.appName).transaction
      dbApp <- F.fromOption(dbAppOpt, AppNotFoundException(params.googleProject, params.appName, ctx.traceId))
      gkeClusterId = dbApp.cluster.getGkeClusterId
      namespaceName = dbApp.app.appResources.namespace.name

      _ <- kubeService.createNamespace(gkeClusterId, KubernetesNamespace(namespaceName))
      secrets <- getSecrets(namespaceName)
      _ <- secrets.parTraverse(secret =>
        kubeService.createSecret(gkeClusterId, KubernetesNamespace(namespaceName), secret)
      )
      _ <- logger.info(s"Finished app creation for ${params.appId} | trace id: ${ctx.traceId}")
      //TODO create svc accts
      //TODO helm create galaxy
      _ <- appQuery.updateStatus(params.appId, AppStatus.Running).transaction
    } yield ()

  override def deleteAndPollCluster(params: DeleteClusterParams)(implicit ev: ApplicativeAsk[F, AppContext]): F[Unit] =
    // TODO not yet implemented
    F.unit

  override def deleteAndPollNodepool(
    params: DeleteNodepoolParams
  )(implicit ev: ApplicativeAsk[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- logger.info(
        s"Beginning nodepool deletion for nodepool ${params.nodepoolId} in project ${params.googleProject} | trace id: ${ctx.traceId}"
      )
      dbNodepoolOpt <- nodepoolQuery.getMinimalById(params.nodepoolId).transaction
      dbNodepool <- F.fromOption(dbNodepoolOpt, NodepoolNotFoundException(params.nodepoolId))
      dbClusterOpt <- kubernetesClusterQuery.getMinimalClusterById(dbNodepool.clusterId).transaction
      dbCluster <- F.fromOption(
        dbClusterOpt,
        KubernetesClusterNotFoundException(s"Cluster with id ${dbNodepool.clusterId} not found in database")
      )
      op <- gkeService.deleteNodepool(
        NodepoolId(dbCluster.getGkeClusterId, dbNodepool.nodepoolName)
      )
      lastOp <- gkeService
        .pollOperation(
          KubernetesOperationId(params.googleProject, dbCluster.location, op),
          config.monitorConfig.nodepoolDelete.interval,
          config.monitorConfig.nodepoolDelete.maxAttempts
        )
        .compile
        .lastOrError
      _ <- if (lastOp.isDone)
        logger.info(
          s"Delete nodepool operation has finished for nodepool ${params.nodepoolId} | trace id: ${ctx.traceId}"
        )
      else
        logger.error(
          s"Delete nodepool operation has failed or timed out for nodepool ${params.nodepoolId} | trace id: ${ctx.traceId}"
        ) >>
          F.raiseError[Unit](NodepoolDeletionException(params.nodepoolId))
      _ <- nodepoolQuery.markAsDeleted(params.nodepoolId, ctx.now).transaction
    } yield ()

  override def deleteAndPollApp(params: DeleteAppParams)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- logger.info(
        s"Beginning app deletion for app ${params.appId} in project ${params.googleProject} | trace id: ${ctx.traceId}"
      )
      dbAppOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(params.googleProject, params.appName).transaction
      dbApp <- F.fromOption(dbAppOpt, AppNotFoundException(params.googleProject, params.appName, ctx.traceId))
      // TODO is deleting the namespace enough or should we helm uninstall as well?
      _ <- kubeService.deleteNamespace(dbApp.cluster.getGkeClusterId,
                                       KubernetesNamespace(dbApp.app.appResources.namespace.name))
      // TODO actually monitor for deletion
      _ <- logger.info(
        s"Delete app operation has finished for nodepool ${params.appId} | trace id: ${ctx.traceId}"
      )
      _ <- appQuery.markAsDeleted(dbApp.app.id, ctx.now).transaction
    } yield ()

  private[util] def getSecrets(namespace: NamespaceName): F[List[KubernetesSecret]] =
    config.ingressConfig.secrets.traverse { secret =>
      for {
        secretFiles <- secret.secretFiles
          .traverse { secretFile =>
            for {
              bytes <- readFileToBytes(secretFile.path, blocker)
            } yield (secretFile.name, bytes.toArray)
          }
          .map(_.toMap)
      } yield KubernetesSecret(
        namespace,
        secret.name,
        secretFiles,
        KubernetesSecretType.Generic
      )
    }

  private[util] def getGoogleNodepool(nodepool: Nodepool): com.google.container.v1.NodePool = {
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
}

sealed trait AppProcessingException extends Exception {
  def getMessage: String
}

final case class ClusterCreationException(message: String) extends AppProcessingException {
  override def getMessage: String = message
}

final case class NodepoolCreationException(nodepoolId: NodepoolLeoId) extends AppProcessingException {
  override def getMessage: String = s"Failed to poll nodepool creation operation to completion for nodepool $nodepoolId"
}

final case class NodepoolDeletionException(nodepoolId: NodepoolLeoId) extends AppProcessingException {
  override def getMessage: String = s"Failed to poll nodepool deletion operation to completion for nodepool $nodepoolId"
}

final case class AppCreationException(message: String) extends AppProcessingException {
  override def getMessage: String = message
}

final case class AppDeletionException(message: String) extends AppProcessingException {
  override def getMessage: String = message
}

final case class DeleteNodepoolResult(nodepoolId: NodepoolLeoId,
                                      operation: com.google.container.v1.Operation,
                                      getAppResult: GetAppResult)
