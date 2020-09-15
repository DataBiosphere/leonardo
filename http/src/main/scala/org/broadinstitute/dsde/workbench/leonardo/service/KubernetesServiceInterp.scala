package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import cats.Parallel
import cats.effect.Async
import cats.mtl.ApplicativeAsk
import io.chrisdavenport.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.google2.GKEModels.{KubernetesClusterName, NodepoolName}
import org.broadinstitute.dsde.workbench.google2.KubernetesName
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.NamespaceName
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.AppType.Galaxy
import org.broadinstitute.dsde.workbench.leonardo.db.{
  appQuery,
  nodepoolQuery,
  ClusterDoesNotExist,
  ClusterExists,
  DbReference,
  KubernetesAppCreationException,
  KubernetesServiceDbQueries,
  SaveApp,
  SaveKubernetesCluster
}
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.config.{
  Config,
  GalaxyAppConfig,
  KubernetesClusterConfig,
  NodepoolConfig,
  PersistentDiskConfig
}
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeoKubernetesServiceInterp.LeoKubernetesConfig
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeonardoService.includeDeletedKey
import org.broadinstitute.dsde.workbench.leonardo.model.SamResourceAction._
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, ServiceAccountProviderConfig, _}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  BatchNodepoolCreateMessage,
  CreateAppMessage,
  DeleteAppMessage
}
import org.broadinstitute.dsde.workbench.leonardo.service.KubernetesService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

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
      hasPermission <- authProvider.hasPermission(ProjectSamResourceId(googleProject),
                                                  ProjectAction.CreateApp,
                                                  userInfo)
      _ <- if (hasPermission) F.unit else F.raiseError[Unit](AuthorizationError(userInfo.userEmail))

      appOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(googleProject, appName).transaction
      _ <- appOpt.fold(F.unit)(c =>
        F.raiseError[Unit](AppAlreadyExistsException(googleProject, appName, c.app.status, ctx.traceId))
      )

      samResourceId <- F.delay(AppSamResourceId(UUID.randomUUID().toString))
      _ <- authProvider
        .notifyResourceCreated(samResourceId, userInfo.userEmail, googleProject)
        .handleErrorWith { t =>
          log.error(t)(
            s"[${ctx.traceId}] Failed to notify the AuthProvider for creation of kubernetes app ${googleProject.value} / ${appName.value}"
          ) >> F.raiseError(t)
        }

      saveCluster <- F.fromEither(getSavableCluster(userInfo, googleProject, ctx.now, None))
      saveClusterResult <- KubernetesServiceDbQueries.saveOrGetForApp(saveCluster).transaction
      _ <- if (saveClusterResult.minimalCluster.status == KubernetesClusterStatus.Error)
        F.raiseError[Unit](
          KubernetesAppCreationException(
            s"You cannot create an app while a cluster is in status ${saveClusterResult.minimalCluster.status}"
          )
        )
      else F.unit

      clusterId = saveClusterResult.minimalCluster.id
      claimedNodepoolOpt <- nodepoolQuery.claimNodepool(clusterId).transaction
      nodepool <- claimedNodepoolOpt match {
        case None =>
          for {
            saveNodepool <- F.fromEither(getUserNodepool(clusterId, userInfo, req.kubernetesRuntimeConfig, ctx.now))
            savedNodepool <- nodepoolQuery.saveForCluster(saveNodepool).transaction
          } yield savedNodepool
        case Some(n) =>
          log.info(s"claimed nodepool ${n.id} in project ${saveClusterResult.minimalCluster.googleProject}") >> F.pure(
            n
          )
      }

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
          FormattedBy.Galaxy,
          authProvider,
          leoKubernetesConfig.diskConfig
        )
      )

      saveApp <- F.fromEither(
        getSavableApp(googleProject,
                      appName,
                      userInfo,
                      samResourceId,
                      req,
                      diskResultOpt.map(_.disk),
                      petSA,
                      nodepool.id,
                      ctx)
      )
      app <- appQuery.save(saveApp).transaction

      createAppMessage = CreateAppMessage(
        saveClusterResult match {
          case _: ClusterExists         => None
          case res: ClusterDoesNotExist => Some(CreateCluster(res.minimalCluster.id, res.defaultNodepool.id))
        },
        app.id,
        app.appName,
        //we don't want to create a nodepool if we already have claimed an existing one
        claimedNodepoolOpt.fold[Option[NodepoolLeoId]](Some(nodepool.id))(_ => None),
        saveClusterResult.minimalCluster.googleProject,
        diskResultOpt.exists(_.creationNeeded),
        req.customEnvironmentVariables,
        Some(ctx.traceId)
      )
      _ <- publisherQueue.enqueue1(createAppMessage)
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

      hasPermission <- authProvider.hasPermission(app.app.samResourceId, AppAction.GetAppStatus, userInfo)
      _ <- if (hasPermission) F.unit else F.raiseError[Unit](AppNotFoundException(googleProject, appName, ctx.traceId))
    } yield GetAppResponse.fromDbResult(app, Config.proxyConfig.proxyUrlBase)

  override def listApp(
    userInfo: UserInfo,
    googleProject: Option[GoogleProject],
    params: Map[String, String]
  )(implicit as: ApplicativeAsk[F, AppContext]): F[Vector[ListAppResponse]] =
    for {
      params <- F.fromEither(LeonardoService.processListParameters(params))
      allClusters <- KubernetesServiceDbQueries.listFullApps(googleProject, params._1, params._2).transaction
      samResources = allClusters.flatMap(_.nodepools.flatMap(_.apps.map(_.samResourceId)))
      samVisibleApps <- authProvider.filterUserVisible(samResources, userInfo)
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
                    .contains(a.samResourceId) || a.auditInfo.creator == userInfo.userEmail
                })
              }
              .filterNot(_.apps.isEmpty)
          )
        }
        .filterNot(_.nodepools.isEmpty)
        .flatMap(c => ListAppResponse.fromCluster(c, Config.proxyConfig.proxyUrlBase))
        .toVector
    }

  override def deleteApp(request: DeleteAppRequest)(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      appOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(request.googleProject, request.appName).transaction
      appResult <- F.fromOption(appOpt, AppNotFoundException(request.googleProject, request.appName, ctx.traceId))

      listOfPermissions <- authProvider.getActions(appResult.app.samResourceId, request.userInfo)

      // throw 404 if no GetAppStatus permission
      hasReadPermission = listOfPermissions.toSet.contains(AppAction.GetAppStatus)
      _ <- if (hasReadPermission) F.unit
      else F.raiseError[Unit](AppNotFoundException(request.googleProject, request.appName, ctx.traceId))

      // throw 403 if no DeleteApp permission
      hasDeletePermission = listOfPermissions.toSet.contains(AppAction.DeleteApp)
      _ <- if (hasDeletePermission) F.unit else F.raiseError[Unit](AuthorizationError(request.userInfo.userEmail))

      canDelete = AppStatus.deletableStatuses.contains(appResult.app.status)
      _ <- if (canDelete) F.unit
      else
        F.raiseError[Unit](
          AppCannotBeDeletedException(request.googleProject, request.appName, appResult.app.status, ctx.traceId)
        )

      _ <- KubernetesServiceDbQueries.markPreDeleting(appResult.nodepool.id, appResult.app.id).transaction
      diskOpt <- if (request.deleteDisk)
        appResult.app.appResources.disk.fold(
          F.raiseError[Option[DiskId]](
            NoDiskForAppException(appResult.cluster.googleProject, appResult.app.appName, ctx.traceId)
          )
        )(d => F.pure(Some(d.id)))
      else F.pure[Option[DiskId]](None)

      deleteMessage = DeleteAppMessage(
        appResult.app.id,
        appResult.app.appName,
        appResult.nodepool.id,
        appResult.cluster.googleProject,
        diskOpt,
        Some(ctx.traceId)
      )
      _ <- publisherQueue.enqueue1(deleteMessage)
    } yield ()

  override def batchNodepoolCreate(userInfo: UserInfo, googleProject: GoogleProject, req: BatchNodepoolCreateRequest)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      hasPermission <- authProvider.hasPermission(ProjectSamResourceId(googleProject),
                                                  ProjectAction.CreateApp,
                                                  userInfo)
      _ <- if (hasPermission) F.unit else F.raiseError[Unit](AuthorizationError(userInfo.userEmail))

      // create default nodepool with size dependant on number of nodes requested
      saveCluster <- F.fromEither(getSavableCluster(userInfo, googleProject, ctx.now, Some(req.numNodepools)))
      saveClusterResult <- KubernetesServiceDbQueries.saveOrGetForApp(saveCluster).transaction

      // check if the cluster exists (we reject this request if it does)
      createCluster <- saveClusterResult match {
        case _: ClusterExists         => F.raiseError[CreateCluster](ClusterExistsException(googleProject))
        case res: ClusterDoesNotExist => F.pure(CreateCluster(res.minimalCluster.id, res.defaultNodepool.id))
      }
      // create list of Precreating nodepools
      eitherNodepoolsOrError = List.tabulate(req.numNodepools.value) { _ =>
        getUserNodepool(createCluster.clusterId, userInfo, req.kubernetesRuntimeConfig, ctx.now)
      }
      nodepools <- eitherNodepoolsOrError.traverse(n => F.fromEither(n))
      _ <- nodepoolQuery.saveAllForCluster(nodepools).transaction
      dbNodepools <- KubernetesServiceDbQueries.getAllNodepoolsForCluster(createCluster.clusterId).transaction
      allNodepoolIds = dbNodepools.map(_.id)
      msg = BatchNodepoolCreateMessage(createCluster.clusterId, allNodepoolIds, googleProject, Some(ctx.traceId))
      _ <- publisherQueue.enqueue1(msg)
    } yield ()

  private[service] def getSavableCluster(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    now: Instant,
    numNodepools: Option[NumNodepools]
  ): Either[Throwable, SaveKubernetesCluster] = {
    val auditInfo = AuditInfo(userInfo.userEmail, now, None, now)

    val defaultNodepool = for {
      nodepoolName <- KubernetesNameUtils.getUniqueName(NodepoolName.apply)
    } yield DefaultNodepool(
      NodepoolLeoId(-1),
      clusterId = KubernetesClusterLeoId(-1),
      nodepoolName,
      status = NodepoolStatus.Precreating,
      auditInfo,
      machineType = leoKubernetesConfig.nodepoolConfig.defaultNodepoolConfig.machineType,
      numNodes = numNodepools
        .map(n =>
          NumNodes(
            math
              .ceil(
                n.value.toDouble /
                  leoKubernetesConfig.nodepoolConfig.defaultNodepoolConfig.maxNodepoolsPerDefaultNode.value.toDouble
              )
              .toInt
          )
        )
        .getOrElse(leoKubernetesConfig.nodepoolConfig.defaultNodepoolConfig.numNodes),
      autoscalingEnabled = leoKubernetesConfig.nodepoolConfig.defaultNodepoolConfig.autoscalingEnabled,
      autoscalingConfig = None
    )

    for {
      nodepool <- defaultNodepool
      clusterName <- KubernetesNameUtils.getUniqueName(KubernetesClusterName.apply)
    } yield SaveKubernetesCluster(
      googleProject = googleProject,
      clusterName = clusterName,
      location = leoKubernetesConfig.clusterConfig.location,
      region = leoKubernetesConfig.clusterConfig.region,
      status = KubernetesClusterStatus.Precreating,
      auditInfo = auditInfo,
      defaultNodepool = nodepool
    )
  }

  private[service] def getUserNodepool(clusterId: KubernetesClusterLeoId,
                                       userInfo: UserInfo,
                                       runtimeConfig: Option[KubernetesRuntimeConfig],
                                       now: Instant): Either[Throwable, Nodepool] = {
    val auditInfo = AuditInfo(userInfo.userEmail, now, None, now)

    val machineConfig = runtimeConfig.getOrElse(
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
      false
    )
  }

  private[service] def getSavableApp(googleProject: GoogleProject,
                                     appName: AppName,
                                     userInfo: UserInfo,
                                     samResourceId: AppSamResourceId,
                                     req: CreateAppRequest,
                                     diskOpt: Option[PersistentDisk],
                                     googleServiceAccount: WorkbenchEmail,
                                     nodepoolId: NodepoolLeoId,
                                     ctx: AppContext): Either[Throwable, SaveApp] = {
    val now = ctx.now
    val auditInfo = AuditInfo(userInfo.userEmail, now, None, now)
    val galaxyConfig = leoKubernetesConfig.galaxyAppConfig

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
        s"${appName.value}-${galaxyConfig.namespaceNameSuffix}",
        NamespaceName.apply
      )
    } yield SaveApp(
      App(
        AppId(-1),
        nodepoolId,
        req.appType,
        appName,
        AppStatus.Precreating,
        Chart(galaxyConfig.chartName, galaxyConfig.chartVersion),
        samResourceId,
        googleServiceAccount,
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
              galaxyConfig.services.map(config => KubernetesService(ServiceId(-1), config))
          },
          Option.empty
        ),
        List.empty,
        req.customEnvironmentVariables
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
      s"App ${googleProject.value}/${appName.value} not found. Trace ID: ${traceId.asString}",
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

case class NoDiskForAppException(googleProject: GoogleProject, appName: AppName, traceId: TraceId)
    extends LeoException(
      s"Specified delete disk for app ${googleProject.value}/${appName.value}, but this app does not have a disk. Trace ID: ${traceId.asString}",
      StatusCodes.BadRequest
    )

case class AppCannotBeCreatedException(googleProject: GoogleProject,
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

case class ClusterExistsException(googleProject: GoogleProject)
    extends LeoException(
      s"Cannot pre-create nodepools for project $googleProject because a cluster already exists",
      StatusCodes.Conflict
    )
