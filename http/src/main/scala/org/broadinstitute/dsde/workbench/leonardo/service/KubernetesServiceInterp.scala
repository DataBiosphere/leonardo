package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.Async
import cats.implicits._
import cats.mtl.ApplicativeAsk
import io.chrisdavenport.log4cats.StructuredLogger
import org.apache.commons.lang3.RandomStringUtils
import org.broadinstitute.dsde.workbench.google2.GKEModels.{KubernetesClusterName, NodepoolName}
import org.broadinstitute.dsde.workbench.google2.KubernetesName
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.NamespaceName
import org.broadinstitute.dsde.workbench.leonardo.AppType.Galaxy
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeoKubernetesServiceInterp.LeoKubernetesConfig
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeonardoService.includeDeletedKey
import org.broadinstitute.dsde.workbench.leonardo.model.SamResourceAction._
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, ServiceAccountProviderConfig, _}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  BatchNodepoolCreateMessage,
  CreateAppMessage,
  DeleteAppMessage,
  StartAppMessage,
  StopAppMessage
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.{ClusterNodepoolAction, LeoPubsubMessage}
import org.broadinstitute.dsde.workbench.leonardo.service.KubernetesService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsp.Release

import scala.concurrent.ExecutionContext

final class LeoKubernetesServiceInterp[F[_]: Parallel](
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
    implicit as: Ask[F, AppContext]
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
      saveClusterResult <- KubernetesServiceDbQueries.saveOrGetClusterForApp(saveCluster).transaction
      // TODO Remove the block below to allow app creation on a new cluster when the existing cluster is in Error status
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

      clusterNodepoolAction = saveClusterResult match {
        case ClusterExists(_) =>
          // If we're claiming a pre-existing nodepool then don't specify CreateNodepool in the pubsub message
          if (claimedNodepoolOpt.isDefined) None else Some(ClusterNodepoolAction.CreateNodepool(nodepool.id))
        case ClusterDoesNotExist(c, n) => Some(ClusterNodepoolAction.CreateClusterAndNodepool(c.id, n.id, nodepool.id))
      }
      createAppMessage = CreateAppMessage(
        googleProject,
        clusterNodepoolAction,
        app.id,
        app.appName,
        diskResultOpt.flatMap(d => if (d.creationNeeded) Some(d.disk.id) else None),
        req.customEnvironmentVariables,
        req.appType,
        Some(ctx.traceId)
      )
      _ <- publisherQueue.enqueue1(createAppMessage)
    } yield ()

  override def getApp(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    appName: AppName
  )(
    implicit as: Ask[F, AppContext]
  ): F[GetAppResponse] =
    for {
      ctx <- as.ask
      appOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(googleProject, appName).transaction
      app <- F.fromOption(appOpt, AppNotFoundException(googleProject, appName, ctx.traceId))

      hasPermission <- authProvider.hasPermission(app.app.samResourceId, AppAction.GetAppStatus, userInfo)
      _ <- if (hasPermission) F.unit
      else
        log.info(s"User ${userInfo} tried to access app ${appName.value} without proper permissions. Returning 404") >> F
          .raiseError[Unit](AppNotFoundException(googleProject, appName, ctx.traceId))
    } yield GetAppResponse.fromDbResult(app, Config.proxyConfig.proxyUrlBase)

  override def listApp(
    userInfo: UserInfo,
    googleProject: Option[GoogleProject],
    params: Map[String, String]
  )(implicit as: Ask[F, AppContext]): F[Vector[ListAppResponse]] =
    for {
      params <- F.fromEither(LeonardoService.processListParameters(params))
      allClusters <- KubernetesServiceDbQueries.listFullApps(googleProject, params._1, params._2).transaction
      samResources = allClusters.flatMap(_.nodepools.flatMap(_.apps.map(_.samResourceId)))
      samVisibleAppsOpt <- NonEmptyList.fromList(samResources).traverse { apps =>
        authProvider.filterUserVisible(apps, userInfo)
      }
      res = samVisibleAppsOpt match {
        case None => Vector.empty
        case Some(samVisibleApps) =>
          val samVisibleAppsSet = samVisibleApps.toSet
          // we construct this list of clusters by first filtering apps the user doesn't have permissions to see
          // then we build back up by filtering nodepools without apps and clusters without nodepools
          allClusters
            .map { c =>
              c.copy(nodepools =
                c.nodepools
                  .map { n =>
                    n.copy(apps = n.apps.filter { a =>
                      // Making the assumption that users will always be able to access apps that they create
                      // Fix for https://github.com/DataBiosphere/leonardo/issues/821
                      samVisibleAppsSet
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
    } yield res

  override def deleteApp(request: DeleteAppRequest)(
    implicit as: Ask[F, AppContext]
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

      diskOpt <- if (request.deleteDisk)
        appResult.app.appResources.disk.fold(
          F.raiseError[Option[DiskId]](
            NoDiskForAppException(appResult.cluster.googleProject, appResult.app.appName, ctx.traceId)
          )
        )(d => F.pure(Some(d.id)))
      else F.pure[Option[DiskId]](None)

      // If the app status is Error, we can assume that the underlying app/nodepool
      // has already been deleted. So we just transition the app to Deleted status
      // without sending a message to Back Leo.
      //
      // Note this has the side effect of not deleting the disk if requested to do so. The
      // caller must manually delete the disk in this situation. We have the same behavior for
      // runtimes.
      _ <- if (appResult.app.status == AppStatus.Error) {
        for {
          _ <- authProvider.notifyResourceDeleted(appResult.app.samResourceId,
                                                  appResult.app.auditInfo.creator,
                                                  appResult.cluster.googleProject)
          _ <- appQuery.markAsDeleted(appResult.app.id, ctx.now).transaction
        } yield ()
      } else {
        for {
          _ <- KubernetesServiceDbQueries.markPreDeleting(appResult.nodepool.id, appResult.app.id).transaction
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
      }
    } yield ()

  override def batchNodepoolCreate(userInfo: UserInfo, googleProject: GoogleProject, req: BatchNodepoolCreateRequest)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      hasPermission <- authProvider.hasPermission(ProjectSamResourceId(googleProject),
                                                  ProjectAction.CreateApp,
                                                  userInfo)
      _ <- if (hasPermission) F.unit else F.raiseError[Unit](AuthorizationError(userInfo.userEmail))

      // create default nodepool with size dependant on number of nodes requested
      saveCluster <- F.fromEither(
        getSavableCluster(userInfo, googleProject, ctx.now, Some(req.numNodepools), req.clusterName)
      )
      saveClusterResult <- KubernetesServiceDbQueries.saveOrGetClusterForApp(saveCluster).transaction

      // check if the cluster exists (we reject this request if it does)
      clusterId <- saveClusterResult match {
        case _: ClusterExists         => F.raiseError[KubernetesClusterLeoId](ClusterExistsException(googleProject))
        case res: ClusterDoesNotExist => F.pure(res.minimalCluster.id)
      }
      // create list of Precreating nodepools
      eitherNodepoolsOrError = List.tabulate(req.numNodepools.value) { _ =>
        getUserNodepool(clusterId, userInfo, req.kubernetesRuntimeConfig, ctx.now)
      }
      nodepools <- eitherNodepoolsOrError.traverse(n => F.fromEither(n))
      _ <- nodepoolQuery.saveAllForCluster(nodepools).transaction
      dbNodepools <- KubernetesServiceDbQueries.getAllNodepoolsForCluster(clusterId).transaction
      allNodepoolIds = dbNodepools.map(_.id)
      msg = BatchNodepoolCreateMessage(clusterId, allNodepoolIds, googleProject, Some(ctx.traceId))
      _ <- publisherQueue.enqueue1(msg)
    } yield ()

  def stopApp(userInfo: UserInfo, googleProject: GoogleProject, appName: AppName)(
    implicit as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      appOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(googleProject, appName).transaction
      appResult <- F.fromOption(appOpt, AppNotFoundException(googleProject, appName, ctx.traceId))

      listOfPermissions <- authProvider.getActions(appResult.app.samResourceId, userInfo)

      // throw 404 if no StopStartApp permission
      hasReadPermission = listOfPermissions.toSet.contains(AppAction.StopStartApp)
      _ <- if (hasReadPermission) F.unit
      else F.raiseError[Unit](AppNotFoundException(googleProject, appName, ctx.traceId))

      // throw 403 if no StopStartApp permission
      hasStopStartPermission = listOfPermissions.toSet.contains(AppAction.StopStartApp)
      _ <- if (hasStopStartPermission) F.unit else F.raiseError[Unit](AuthorizationError(userInfo.userEmail))

      canStop = AppStatus.stoppableStatuses.contains(appResult.app.status)
      _ <- if (canStop) F.unit
      else
        F.raiseError[Unit](
          AppCannotBeStoppedException(googleProject, appName, appResult.app.status, ctx.traceId)
        )

      // TODO note no hasOperationInProgress check, we plan to queue nodepool actions

      _ <- KubernetesServiceDbQueries.markPreStopping(appResult.nodepool.id, appResult.app.id).transaction
      message = StopAppMessage(
        appResult.app.id,
        appResult.nodepool.id,
        appResult.cluster.googleProject,
        Some(ctx.traceId)
      )
      _ <- publisherQueue.enqueue1(message)
    } yield ()

  def startApp(userInfo: UserInfo, googleProject: GoogleProject, appName: AppName)(
    implicit as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      appOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(googleProject, appName).transaction
      appResult <- F.fromOption(appOpt, AppNotFoundException(googleProject, appName, ctx.traceId))

      listOfPermissions <- authProvider.getActions(appResult.app.samResourceId, userInfo)

      // throw 404 if no StopStartApp permission
      hasReadPermission = listOfPermissions.toSet.contains(AppAction.StopStartApp)
      _ <- if (hasReadPermission) F.unit
      else F.raiseError[Unit](AppNotFoundException(googleProject, appName, ctx.traceId))

      // throw 403 if no StopStartApp permission
      hasStopStartPermission = listOfPermissions.toSet.contains(AppAction.StopStartApp)
      _ <- if (hasStopStartPermission) F.unit else F.raiseError[Unit](AuthorizationError(userInfo.userEmail))

      canStop = AppStatus.startableStatuses.contains(appResult.app.status)
      _ <- if (canStop) F.unit
      else
        F.raiseError[Unit](
          AppCannotBeStartedException(googleProject, appName, appResult.app.status, ctx.traceId)
        )

      _ <- KubernetesServiceDbQueries
        .markPreStarting(appResult.nodepool.id, appResult.app.id, appResult.nodepool.numNodes)
        .transaction
      message = StartAppMessage(
        appResult.app.id,
        appResult.nodepool.id,
        appResult.cluster.googleProject,
        Some(ctx.traceId)
      )
      _ <- publisherQueue.enqueue1(message)
    } yield ()

  private[service] def getSavableCluster(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    now: Instant,
    numNodepools: Option[NumNodepools],
    clusterName: Option[KubernetesClusterName] = None
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
      defaultClusterName <- KubernetesNameUtils.getUniqueName(KubernetesClusterName.apply)
    } yield SaveKubernetesCluster(
      googleProject = googleProject,
      clusterName = clusterName.getOrElse(defaultClusterName),
      location = leoKubernetesConfig.clusterConfig.location,
      region = leoKubernetesConfig.clusterConfig.region,
      status = KubernetesClusterStatus.Precreating,
      ingressChart = leoKubernetesConfig.ingressConfig.chart,
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

      // Generate namespace and app release names using a random 6-character string prefix.
      //
      // Theoretically release names should only need to be unique within a namespace, but the Galaxy
      // installation requires that release names be unique within a cluster.
      //
      // Note these names must adhere to Kubernetes naming requirements; additionally the release
      // name is used by the Galaxy chart templates to generate longer strings like preload-cache-cvmfs-gxy-main-<release>
      // which also need to be valid Kubernetes names.
      //
      // Previously we used appName as the prefix instead of a 6-character uid, but thought it is better to
      // decouple this string from UI-generated Leo app names because of hidden k8s/Galaxy constraints.
      //
      // There are DB constraints to handle potential name collisions.
      uid = s"${RandomStringUtils.randomAlphabetic(1)}${RandomStringUtils.randomAlphanumeric(5)}".toLowerCase
      namespaceName <- KubernetesName.withValidation(
        s"${uid}-${galaxyConfig.namespaceNameSuffix}",
        NamespaceName.apply
      )
      release <- KubernetesName.withValidation(
        s"${uid}-${galaxyConfig.releaseNameSuffix}",
        Release.apply
      )
    } yield SaveApp(
      App(
        AppId(-1),
        nodepoolId,
        req.appType,
        appName,
        AppStatus.Precreating,
        Chart(galaxyConfig.chartName, galaxyConfig.chartVersion),
        release,
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
                                 ingressConfig: KubernetesIngressConfig,
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

case class AppCannotBeStoppedException(googleProject: GoogleProject,
                                       appName: AppName,
                                       status: AppStatus,
                                       traceId: TraceId)
    extends LeoException(
      s"App ${googleProject.value}/${appName.value} cannot be stopped in ${status} status. Trace ID: ${traceId.asString}",
      StatusCodes.Conflict
    )

case class AppCannotBeStartedException(googleProject: GoogleProject,
                                       appName: AppName,
                                       status: AppStatus,
                                       traceId: TraceId)
    extends LeoException(
      s"App ${googleProject.value}/${appName.value} cannot be started in ${status} status. Trace ID: ${traceId.asString}",
      StatusCodes.Conflict
    )
