package org.broadinstitute.dsde.workbench.leonardo
package util

import _root_.io.chrisdavenport.log4cats.StructuredLogger
import cats.Parallel
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.container.v1.MasterAuthorizedNetworksConfig.CidrBlock
import com.google.container.v1._
import org.broadinstitute.dsde.workbench.DoneCheckableInstances._
import org.broadinstitute.dsde.workbench.DoneCheckableSyntax._
import org.broadinstitute.dsde.workbench.google2.GKEModels._
import org.broadinstitute.dsde.workbench.google2.KubernetesClusterNotFoundException
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.{KubernetesNamespace, KubernetesSecret, KubernetesSecretType}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.NamespaceName
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, appErrorQuery, kubernetesClusterQuery, nodepoolQuery, _}
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppNotFoundException
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.CreateAppMessage

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

final case class GKEInterpreterConfig(securityFiles: SecurityFilesConfig,
                                      ingressConfig: IngressHelmConfig,
                                      monitorConfig: AppMonitorConfig,
                                      clusterConfig: KubernetesClusterConfig)
class GKEInterpreter[F[_]: Parallel: ContextShift](
  config: GKEInterpreterConfig,
  vpcAlg: VPCAlgebra[F],
  gkeService: org.broadinstitute.dsde.workbench.google2.GKEService[F],
  kubeService: org.broadinstitute.dsde.workbench.google2.KubernetesService[F],
  blocker: Blocker
)(implicit val executionContext: ExecutionContext,
  timer: Timer[F],
  logger: StructuredLogger[F],
  dbRef: DbReference[F],
  F: ConcurrentEffect[F]) {

  def createApp(msg: CreateAppMessage)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] = {
    val createApp = for {
      ctx <- ev.ask
      _ <- logger.info(s"beginning app creation for app ${msg.appId} | trace id : ${ctx.traceId}")
      getAppOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(msg.project, msg.appName).transaction
      getApp <- F.fromOption(getAppOpt, AppNotFoundException(msg.project, msg.appName, ctx.traceId))
      gkeClusterId = getApp.cluster.getGkeClusterId
      namespaceName = getApp.app.appResources.namespace.name

      _ <- kubeService.createNamespace(gkeClusterId, KubernetesNamespace(namespaceName))
      secrets <- getSecrets(namespaceName)
      _ <- secrets.parTraverse(secret =>
        kubeService.createSecret(gkeClusterId, KubernetesNamespace(namespaceName), secret)
      )
      _ <- logger.info(s"finished app creation for ${msg.appId}")
      //TODO create svc accts
      //TODO helm create ingress
      //TODO helm create galaxy
      _ <- appQuery.updateStatus(msg.appId, AppStatus.Running).transaction
    } yield ()

    createApp.onError {
      case e =>
        for {
          now <- nowInstant
          _ <- logger.error(e)(s"Failed to create app ${msg.appId}")
          _ <- appQuery.updateStatus(msg.appId, AppStatus.Error).transaction
          _ <- appErrorQuery
            .save(msg.appId, KubernetesError(e.getMessage(), now, ErrorAction.CreateGalaxyApp, ErrorSource.App, None))
            .transaction
        } yield ()
    }
  }

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

  def createCluster(createMessage: CreateCluster, appId: AppId)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] = {
    val createCluster =
      for {
        _ <- logger.info(s"beginning cluster creation for app ${appId}")
        clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(createMessage.clusterId).transaction
        dbCluster <- F.fromOption(
          clusterOpt,
          KubernetesClusterNotFoundException(s"cluster with id ${createMessage.clusterId} not found in database")
        )
        defaultNodepool <- F.fromOption(dbCluster.nodepools.filter(_.isDefault).headOption,
                                        DefaultNodepoolNotFoundException(dbCluster.id))

        (network, subnetwork) <- vpcAlg.setUpProjectNetwork(
          SetUpProjectNetworkParams(dbCluster.googleProject)
        )

        kubeNetwork = KubernetesNetwork(dbCluster.googleProject, network).idString
        kubeSubNetwork = KubernetesSubNetwork(dbCluster.googleProject, dbCluster.region, subnetwork).idString

        cidrBuilder = CidrBlock.newBuilder()
        _ <- F.delay(config.clusterConfig.authorizedNetworks.foreach(cidrIP => cidrBuilder.setCidrBlock(cidrIP.value)))

        createClusterReq = Cluster
          .newBuilder()
          .setName(dbCluster.clusterName.value)
          .addNodePools(
            getGoogleNodepool(defaultNodepool)
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

        req = KubernetesCreateClusterRequest(dbCluster.googleProject, dbCluster.location, createClusterReq)
        op <- gkeService.createCluster(req)
        lastOp <- gkeService
        //TODO: refactor this to handle interupts
          .pollOperation(
            KubernetesOperationId(dbCluster.googleProject, dbCluster.location, op),
            config.monitorConfig.clusterCreate.interval,
            config.monitorConfig.clusterCreate.maxAttempts
          )
          .compile
          .lastOrError
        _ <- if (lastOp.isDone) logger.info(s"Create cluster operation has finished for cluster ${dbCluster.id}")
        else
          logger.error(s"Create cluster operation has failed for cluster ${dbCluster.id}") >> F.raiseError[Unit](
            ClusterCreationException(
              s"Failed to poll cluster creation operation to completion for cluster ${dbCluster.id} and default nodepool ${defaultNodepool.id}"
            )
          )

        _ <- kubernetesClusterQuery
          .updateAsyncFields(dbCluster.id,
                             KubernetesClusterAsyncFields(
                               IP("0.0.0.0"), //TODO: fill this out after ingress is installed
                               NetworkFields(
                                 network,
                                 subnetwork,
                                 Config.vpcConfig.subnetworkIpRange
                               )
                             ))
          .transaction
        _ <- kubernetesClusterQuery.updateStatus(dbCluster.id, KubernetesClusterStatus.Running).transaction
        _ <- nodepoolQuery.updateStatus(defaultNodepool.id, NodepoolStatus.Running).transaction
      } yield ()

    createCluster.onError {
      case e =>
        for {
          now <- nowInstant
          _ <- logger.error(e)(
            s"Failed to create cluster ${createMessage.clusterId} and nodepool ${createMessage.nodepoolId}"
          )
          _ <- kubernetesClusterQuery.updateStatus(createMessage.clusterId, KubernetesClusterStatus.Error).transaction
          _ <- nodepoolQuery.updateStatus(createMessage.nodepoolId, NodepoolStatus.Error).transaction
          _ <- appErrorQuery
            .save(appId, KubernetesError(e.getMessage(), now, ErrorAction.CreateGalaxyApp, ErrorSource.Cluster, None))
            .transaction
        } yield ()
    }
  }

  def createNodepool(nodepoolId: NodepoolLeoId, appId: AppId)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] = {
    val createNodepool = for {
      _ <- logger.info(s"beginning nodepool creation for app ${appId}")
      dbNodepoolOpt <- nodepoolQuery.getMinimalById(nodepoolId).transaction
      dbNodepool <- F.fromOption(dbNodepoolOpt, NodepoolNotFoundException(nodepoolId))
      dbClusterOpt <- kubernetesClusterQuery.getMinimalClusterById(dbNodepool.clusterId).transaction
      dbCluster <- F.fromOption(
        dbClusterOpt,
        KubernetesClusterNotFoundException(s"cluster with id ${dbNodepool.clusterId} not found in database")
      )
      req = KubernetesCreateNodepoolRequest(
        dbCluster.getGkeClusterId,
        getGoogleNodepool(dbNodepool)
      )
      op <- gkeService.createNodepool(req)
      lastOp <- gkeService
      //TODO: refactor this to handle interupts
        .pollOperation(
          KubernetesOperationId(dbCluster.googleProject, dbCluster.location, op),
          config.monitorConfig.nodepoolCreate.interval,
          config.monitorConfig.nodepoolCreate.maxAttempts
        )
        .compile
        .lastOrError
      _ <- if (lastOp.isDone) logger.info(s"Nodepool creation operation has finished for nodepool ${nodepoolId}")
      else
        logger.error(s"Create nodepool operation has failed or timed out for nodepool $nodepoolId") >> F
          .raiseError[Unit](NodepoolCreationException(nodepoolId))
      _ <- nodepoolQuery.updateStatus(nodepoolId, NodepoolStatus.Running).transaction
    } yield ()

    createNodepool.onError {
      case e =>
        for {
          ctx <- ev.ask
          _ <- logger.error(e)(s"Failed to create nodepool $nodepoolId")
          _ <- nodepoolQuery.updateStatus(nodepoolId, NodepoolStatus.Error).transaction
          _ <- appErrorQuery
            .save(appId, KubernetesError(e.getMessage(), ctx.now, ErrorAction.CreateGalaxyApp, ErrorSource.Nodepool, None))
            .transaction
        } yield ()
    }
  }

  def deleteNodepool(getAppResult: GetAppResult)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] = {
    val nodepoolId = getAppResult.nodepool.id
    val deleteNodepool = for {
      ctx <- ev.ask
      op <- gkeService.deleteNodepool(
        NodepoolId(getAppResult.cluster.getGkeClusterId, getAppResult.nodepool.nodepoolName)
      )
      //TODO: refactor this to handle interupts
      lastOp <- gkeService
        .pollOperation(
          KubernetesOperationId(getAppResult.cluster.googleProject, getAppResult.cluster.location, op),
          config.monitorConfig.nodepoolDelete.interval,
          config.monitorConfig.nodepoolDelete.maxAttempts
        )
        .compile
        .lastOrError
      _ <- if (lastOp.isDone) F.unit
      else
        logger.error(s"Delete nodepool operation has failed or timed out for nodepool ${nodepoolId}") >> F
          .raiseError[Unit](NodepoolDeletionException(nodepoolId))
      _ <- nodepoolQuery.markAsDeleted(nodepoolId, ctx.now).transaction
    } yield ()

    deleteNodepool.onError {
      case e =>
        for {
          now <- nowInstant
          _ <- logger.error(e)(s"Failed to delete nodepool $nodepoolId")
          _ <- nodepoolQuery.updateStatus(nodepoolId, NodepoolStatus.Error).transaction
          _ <- appErrorQuery
            .save(getAppResult.app.id,
                  KubernetesError(e.getMessage(), now, ErrorAction.DeleteGalaxyApp, ErrorSource.Nodepool, None))
            .transaction
        } yield ()
    }
  }

  def deleteApp(getAppResult: GetAppResult)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] = {
    //TODO: this should actually monitor the deletion went through successfully before updating the status
    val deleteApp = for {
      now <- nowInstant
      _ <- kubeService.deleteNamespace(getAppResult.cluster.getGkeClusterId,
                                       KubernetesNamespace(getAppResult.app.appResources.namespace.name))
      _ <- appQuery.markAsDeleted(getAppResult.app.id, now).transaction
    } yield ()

    deleteApp.onError {
      case e =>
        for {
          now <- nowInstant
          _ <- logger.error(e)(s"Failed to delete app ${getAppResult.app.id}")
          _ <- appQuery.updateStatus(getAppResult.app.id, AppStatus.Error).transaction
          _ <- appErrorQuery
            .save(getAppResult.app.id,
                  KubernetesError(e.getMessage(), now, ErrorAction.DeleteGalaxyApp, ErrorSource.App, None))
            .transaction
        } yield ()
    }
  }

  private def logError(description: String): Throwable => F[Unit] =
    t => logger.error(t)(s"Fail to monitor ${description}")
}

sealed trait AppProcessingException extends Exception {
  def getMessage: String
}

case class ClusterCreationException(message: String) extends AppProcessingException {
  override def getMessage: String = message
}

case class NodepoolCreationException(nodepoolId: NodepoolLeoId) extends AppProcessingException {
  override def getMessage: String = s"Failed to poll nodepool creation operation to completion for nodepool $nodepoolId"
}

case class NodepoolDeletionException(nodepoolId: NodepoolLeoId) extends AppProcessingException {
  override def getMessage: String = s"Failed to poll nodepool deletion operation to completion for nodepool $nodepoolId"
}

case class AppCreationException(message: String) extends AppProcessingException {
  override def getMessage: String = message
}

case class AppDeletionException(message: String) extends AppProcessingException {
  override def getMessage: String = message
}
