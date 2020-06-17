package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import cats.Parallel
import cats.effect.Async
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.google2.GKEModels.{KubernetesClusterName, NodepoolName}
import org.broadinstitute.dsde.workbench.leonardo.db.{
  appQuery,
  nodepoolQuery,
  DbReference,
  KubernetesServiceDbQueries,
  SaveApp,
  SaveKubernetesCluster
}
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.config.{
  GalaxyAppConfig,
  KubernetesClusterConfig,
  NodepoolConfig,
  PersistentDiskConfig
}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, ServiceAccountProviderConfig}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import LeonardoService.includeDeletedKey
import io.chrisdavenport.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.google2.KubernetesName
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.NamespaceName
import org.broadinstitute.dsde.workbench.leonardo.AppType.Galaxy
import org.broadinstitute.dsde.workbench.leonardo.SamResource.{AppSamResource}
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeoKubernetesServiceInterp.LeoKubernetesConfig
import org.broadinstitute.dsde.workbench.leonardo.service.KubernetesService

import scala.concurrent.ExecutionContext

class LeoKubernetesServiceInterp[F[_]: Parallel](
  protected val authProvider: LeoAuthProvider[F],
  protected val serviceAccountProvider: ServiceAccountProvider[F],
  protected val leoKubernetesConfig: LeoKubernetesConfig,
  protected val publisherQueue: fs2.concurrent.Queue[F, LeoPubsubMessage]
)(
  implicit F: Async[F],
  log: StructuredLogger[F],
  dbReference: DbReference[F],
  ec: ExecutionContext
) extends KubernetesService[F] {

  override def createApp(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    appName: AppName,
    req: CreateAppRequest
  )(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      //TODO check SAM permissions
      hasPermission <- F.pure(true)
      _ <- if (hasPermission) F.unit else F.raiseError[Unit](AuthorizationError(Some(userInfo.userEmail)))

      appOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(googleProject, appName).transaction
      _ <- appOpt.fold(F.unit)(c =>
        F.raiseError[Unit](AppAlreadyExistsException(googleProject, appName, c.app.status, ctx.traceId))
      )

      samResourceId <- F.delay(AppSamResource(UUID.randomUUID().toString))
//   TODO: notify SAM
//      _ <- authProvider.notifyKubernetesClusterCreated(samResourceId)

      saveCluster <- F.fromEither(getSavableCluster(userInfo, googleProject, ctx.now))
      saveClusterResult <- KubernetesServiceDbQueries.saveOrGetForApp(saveCluster).transaction

      clusterId = saveClusterResult.minimalCluster.id
      saveNodepool <- F.fromEither(getUserNodepool(clusterId, userInfo, req, ctx.now))
      nodepool <- nodepoolQuery.saveForCluster(saveNodepool).transaction

      runtimeServiceAccountOpt <- serviceAccountProvider
        .getClusterServiceAccount(userInfo, googleProject)
      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done Sam call for getClusterServiceAccount")))
      petSA <- F.fromEither(
        runtimeServiceAccountOpt.toRight(new Exception(s"user ${userInfo.userEmail.value} doesn't have a PET SA"))
      )

      diskResultOpt <- req.diskConfig.traverse(diskReq =>
        RuntimeServiceInterp.processPersistentDiskRequest(
          diskReq,
          googleProject,
          userInfo,
          petSA,
          authProvider,
          leoKubernetesConfig.diskConfig
        )
      )

      saveApp <- F.fromEither(
        getSavableApp(googleProject, appName, userInfo, samResourceId, req, diskResultOpt.map(_.disk), nodepool.id, ctx)
      )
      _ <- appQuery.save(saveApp).transaction
////      _ <- publisherQueue.enqueue1() //TODO: queue cluster/nodepool/app creation, queue disk creation
    } yield ()

  override def getApp(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    appName: AppName
  )(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[GetAppResponse] =
    for {
      ctx <- as.ask
      appOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(googleProject, appName).transaction
      app <- F.fromOption(appOpt, AppNotFoundException(googleProject, appName, ctx.traceId))

      //TODO ask SAM
      hasPermission <- F.pure(true)
      _ <- if (hasPermission) F.unit else F.raiseError[Unit](AppNotFoundException(googleProject, appName, ctx.traceId))
    } yield GetAppResponse.fromDbResult(app)

  override def listApp(
    userInfo: UserInfo,
    googleProject: Option[GoogleProject],
    params: Map[String, String]
  ): F[Vector[ListAppResponse]] =
    for {
      params <- F.fromEither(LeonardoService.processListParameters(params))
      allClusters <- KubernetesServiceDbQueries.listFullApps(googleProject, params._1, params._2).transaction
      //TODO: make SAM call
      samVisibleApps <- F.pure(List[(GoogleProject, AppSamResource)]())
    } yield {
      //we construct this list of clusters by first filtering apps the user doesn't have permissions to see
      //then we build back up by filtering nodepools without apps and clusters without nodepools
      allClusters
        .map { c =>
          c.copy(nodepools =
            c.nodepools
              .map { n =>
                n.copy(apps = n.apps.filter { a =>
                  // Making the assumption that users will always be able to access apps that they create
                  // Fix for https://github.com/DataBiosphere/leonardo/issues/821
                  samVisibleApps
                    .contains((c.googleProject, a.samResourceId)) || a.auditInfo.creator == userInfo.userEmail
                })
              }
              .filterNot(_.apps.isEmpty)
          )
        }
        .filterNot(_.nodepools.isEmpty)
        .flatMap(c => ListAppResponse.fromCluster(c))
        .toVector
    }

  override def deleteApp(userInfo: UserInfo, googleProject: GoogleProject, appName: AppName)(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      appOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(googleProject, appName).transaction
      appResult <- F.fromOption(appOpt, AppNotFoundException(googleProject, appName, ctx.traceId))

      //TODO implement SAM check
      hasPermission <- F.pure(true)
      _ <- if (hasPermission) F.unit else F.raiseError[Unit](AuthorizationError(Some(userInfo.userEmail)))

      canDelete = AppStatus.deletableStatuses.contains(appResult.app.status)
      _ <- if (canDelete) F.unit
      else F.raiseError[Unit](AppCannotBeDeletedException(googleProject, appName, appResult.app.status, ctx.traceId))
//
      //TODO: do atomically and send message at the same time?
      _ <- nodepoolQuery.markPendingDeletion(appResult.nodepool.id).transaction
      _ <- appQuery.markPendingDeletion(appResult.app.id).transaction
      //TODO queue disk/cluster/nodepool/app creation
    } yield ()

  private[service] def getSavableCluster(userInfo: UserInfo,
                                         googleProject: GoogleProject,
                                         now: Instant): Either[Throwable, SaveKubernetesCluster] = {
    val auditInfo = AuditInfo(userInfo.userEmail, now, None, now)

    val defaultNodepool = for {
      nodepoolName <- KubernetesNameUtils.getUniqueName(NodepoolName.apply)
    } yield Nodepool(
      NodepoolLeoId(-1),
      clusterId = KubernetesClusterLeoId(-1),
      nodepoolName,
      status = NodepoolStatus.Precreating,
      auditInfo,
      machineType = leoKubernetesConfig.nodepoolConfig.defaultNodepoolConfig.machineType,
      numNodes = leoKubernetesConfig.nodepoolConfig.defaultNodepoolConfig.numNodes,
      autoscalingEnabled = leoKubernetesConfig.nodepoolConfig.defaultNodepoolConfig.autoscalingEnabled,
      autoscalingConfig = None,
      List.empty,
      List()
    )

    for {
      nodepool <- defaultNodepool
      clusterName <- KubernetesNameUtils.getUniqueName(KubernetesClusterName.apply)
    } yield SaveKubernetesCluster(
      googleProject = googleProject,
      clusterName = clusterName,
      location = leoKubernetesConfig.clusterConfig.location,
      status = KubernetesClusterStatus.Precreating,
      serviceAccount = leoKubernetesConfig.serviceAccountConfig.leoServiceAccountEmail,
      auditInfo = auditInfo,
      defaultNodepool = nodepool
    )
  }

  private[service] def getUserNodepool(clusterId: KubernetesClusterLeoId,
                                       userInfo: UserInfo,
                                       req: CreateAppRequest,
                                       now: Instant): Either[Throwable, Nodepool] = {
    val auditInfo = AuditInfo(userInfo.userEmail, now, None, now)

    val machineConfig = req.kubernetesRuntimeConfig.getOrElse(
      KubernetesRuntimeConfig(
        leoKubernetesConfig.nodepoolConfig.galaxyNodepoolConfig.numNodes,
        leoKubernetesConfig.nodepoolConfig.galaxyNodepoolConfig.machineType,
        leoKubernetesConfig.nodepoolConfig.galaxyNodepoolConfig.autoscalingEnabled
      )
    )

    for {
      nodepoolName <- KubernetesNameUtils.getUniqueName(NodepoolName.apply)
    } yield Nodepool(
      NodepoolLeoId(-1),
      clusterId = clusterId,
      nodepoolName,
      status = NodepoolStatus.Precreating,
      auditInfo,
      machineType = machineConfig.machineType,
      numNodes = machineConfig.numNodes,
      autoscalingEnabled = machineConfig.autoscalingEnabled,
      autoscalingConfig = Some(leoKubernetesConfig.nodepoolConfig.galaxyNodepoolConfig.autoscalingConfig),
      List.empty,
      List.empty
    )
  }

  private[service] def getSavableApp(googleProject: GoogleProject,
                                     appName: AppName,
                                     userInfo: UserInfo,
                                     samResourceId: AppSamResource,
                                     req: CreateAppRequest,
                                     diskOpt: Option[PersistentDisk],
                                     nodepoolId: NodepoolLeoId,
                                     ctx: AppContext): Either[Throwable, SaveApp] = {
    val now = ctx.now
    val auditInfo = AuditInfo(userInfo.userEmail, now, None, now)

    val allLabels =
      DefaultKubernetesLabels(googleProject,
                              appName,
                              userInfo.userEmail,
                              leoKubernetesConfig.serviceAccountConfig.leoServiceAccountEmail).toMap ++ req.labels
    for {
      // check the labels do not contain forbidden keys
      labels <- if (allLabels.contains(includeDeletedKey))
        Left(IllegalLabelKeyException(includeDeletedKey))
      else
        Right(allLabels)
      //galaxy apps need a disk
      disk <- if (req.appType == AppType.Galaxy && diskOpt.isEmpty)
        Left(AppRequiresDiskException(googleProject, appName, req.appType, ctx.traceId))
      else Right(diskOpt)
      namespaceName <- KubernetesName.withValidation(
        s"${appName.value}-${leoKubernetesConfig.galaxyAppConfig.namespaceNameSuffix.value}",
        NamespaceName.apply
      )
    } yield SaveApp(
      App(
        AppId(-1),
        nodepoolId,
        req.appType,
        appName,
        AppStatus.Precreating,
        samResourceId,
        auditInfo,
        labels,
        AppResources(
          Namespace(
            NamespaceId(-1),
            namespaceName
          ),
          disk,
          req.appType match {
            case Galaxy =>
              leoKubernetesConfig.galaxyAppConfig.services.map(config => KubernetesService(ServiceId(-1), config))
          }
        ),
        List.empty
      )
    )
  }

}

object LeoKubernetesServiceInterp {
  case class LeoKubernetesConfig(serviceAccountConfig: ServiceAccountProviderConfig,
                                 clusterConfig: KubernetesClusterConfig,
                                 nodepoolConfig: NodepoolConfig,
                                 galaxyAppConfig: GalaxyAppConfig,
                                 diskConfig: PersistentDiskConfig)
}
case class AppNotFoundException(googleProject: GoogleProject, appName: AppName, traceId: TraceId)
    extends LeoException(
      s"Kubernetes cluster ${googleProject.value}/${appName.value} not found. Trace ID: ${traceId.asString}",
      StatusCodes.NotFound
    )

case class AppAlreadyExistsException(googleProject: GoogleProject,
                                     appName: AppName,
                                     status: AppStatus,
                                     traceId: TraceId)
    extends LeoException(
      s"App ${googleProject.value}/${appName.value} already exists in ${status.toString} status. Trace ID: ${traceId.asString}",
      StatusCodes.Conflict
    )

case class AppCannotBeDeletedException(googleProject: GoogleProject,
                                       appName: AppName,
                                       status: AppStatus,
                                       traceId: TraceId)
    extends LeoException(
      s"App ${googleProject.value}/${appName.value} cannot be deleted in ${status} status. Trace ID: ${traceId.asString}",
      StatusCodes.Conflict
    )

case class AppRequiresDiskException(googleProject: GoogleProject, appName: AppName, appType: AppType, traceId: TraceId)
    extends LeoException(
      s"App ${googleProject.value}/${appName.value} cannot be created because the request does not contain a valid disk. Apps of type ${appType} require a disk. Trace ID: ${traceId.asString}",
      StatusCodes.BadRequest
    )
