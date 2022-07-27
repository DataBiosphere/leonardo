package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import akka.http.scaladsl.model.StatusCodes
import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import org.apache.commons.lang3.RandomStringUtils
import org.broadinstitute.dsde.workbench.google2.GKEModels.{KubernetesClusterName, NodepoolName}
import org.broadinstitute.dsde.workbench.google2.{GoogleComputeService, KubernetesName, MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.NamespaceName
import org.broadinstitute.dsde.workbench.leonardo.AppRestore.{CromwellRestore, GalaxyRestore}
import org.broadinstitute.dsde.workbench.leonardo.AppType.{appTypeToFormattedByType, Cromwell, Custom, Galaxy}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId._
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeoAppServiceInterp.isPatchVersionDifference
import org.broadinstitute.dsde.workbench.leonardo.model.SamResourceAction._
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, ServiceAccountProviderConfig, _}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.{ClusterNodepoolAction, LeoPubsubMessage}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsp.{ChartVersion, Release}
import org.typelevel.log4cats.StructuredLogger
import java.time.Instant
import java.util.UUID

import scala.concurrent.ExecutionContext

final class LeoAppServiceInterp[F[_]: Parallel](config: AppServiceConfig,
                                                authProvider: LeoAuthProvider[F],
                                                serviceAccountProvider: ServiceAccountProvider[F],
                                                publisherQueue: Queue[F, LeoPubsubMessage],
                                                computeService: GoogleComputeService[F]
)(implicit
  F: Async[F],
  log: StructuredLogger[F],
  dbReference: DbReference[F],
  ec: ExecutionContext
) extends AppService[F] {

  override def createApp(
    userInfo: UserInfo,
    cloudContext: CloudContext,
    appName: AppName,
    req: CreateAppRequest
  )(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      // TODO: add azure support
      googleProject = cloudContext match {
        case CloudContext.Gcp(value) => value
        case CloudContext.Azure(_)   => ???
      }
      hasPermission <- authProvider.hasPermission(ProjectSamResourceId(googleProject),
                                                  ProjectAction.CreateApp,
                                                  userInfo
      )
      _ <- F.raiseWhen(!hasPermission)(ForbiddenError(userInfo.userEmail))

      _ <- checkIfUserAllowed(req.appType, userInfo.userEmail)

      appOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(CloudContext.Gcp(googleProject), appName).transaction
      _ <- appOpt.fold(F.unit)(c =>
        F.raiseError[Unit](AppAlreadyExistsException(cloudContext, appName, c.app.status, ctx.traceId))
      )

      samResourceId <- F.delay(AppSamResourceId(UUID.randomUUID().toString))

      // Look up the original email in case this API was called by a pet SA
      originatingUserEmail <- authProvider.lookupOriginatingUserEmail(userInfo)
      _ <- authProvider
        .notifyResourceCreated(samResourceId, originatingUserEmail, googleProject)
        .handleErrorWith { t =>
          log.error(ctx.loggingCtx, t)(
            s"Failed to notify the AuthProvider for creation of kubernetes app ${googleProject.value} / ${appName.value}"
          ) >> F.raiseError[Unit](t)
        }

      saveCluster <- F.fromEither(getSavableCluster(originatingUserEmail, cloudContext, ctx.now, None))
      saveClusterResult <- KubernetesServiceDbQueries.saveOrGetClusterForApp(saveCluster).transaction
      // TODO Remove the block below to allow app creation on a new cluster when the existing cluster is in Error status
      _ <-
        if (saveClusterResult.minimalCluster.status == KubernetesClusterStatus.Error)
          F.raiseError[Unit](
            KubernetesAppCreationException(
              s"You cannot create an app while a cluster${saveClusterResult.minimalCluster.clusterName.value} is in status ${saveClusterResult.minimalCluster.status}",
              Some(ctx.traceId)
            )
          )
        else F.unit

      clusterId = saveClusterResult.minimalCluster.id

      machineConfig = req.kubernetesRuntimeConfig.getOrElse(
        KubernetesRuntimeConfig(
          config.leoKubernetesConfig.nodepoolConfig.galaxyNodepoolConfig.numNodes,
          config.leoKubernetesConfig.nodepoolConfig.galaxyNodepoolConfig.machineType,
          config.leoKubernetesConfig.nodepoolConfig.galaxyNodepoolConfig.autoscalingEnabled
        )
      )

      // We want to know if the user already has a nodepool with the requested config that can be re-used
      userNodepoolOpt <- nodepoolQuery
        .getMinimalByUserAndConfig(originatingUserEmail, cloudContext, machineConfig)
        .transaction

      nodepool <- userNodepoolOpt match {
        case Some(n) =>
          log.info(ctx.loggingCtx)(
            s"Reusing user's nodepool ${n.id} in ${saveClusterResult.minimalCluster.cloudContext.asStringWithProvider} with ${machineConfig}"
          ) >> F.pure(n)
        case None =>
          for {
            _ <- log.info(ctx.loggingCtx)(
              s"No nodepool with ${machineConfig} found for this user in project ${saveClusterResult.minimalCluster.cloudContext.asStringWithProvider}. Will create a new nodepool."
            )
            saveNodepool <- F.fromEither(
              getUserNodepool(clusterId, originatingUserEmail, req.kubernetesRuntimeConfig, ctx.now)
            )
            savedNodepool <- nodepoolQuery.saveForCluster(saveNodepool).transaction
          } yield savedNodepool
      }

      runtimeServiceAccountOpt <- serviceAccountProvider
        .getClusterServiceAccount(userInfo, googleProject)
      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done Sam call for getClusterServiceAccount")))
      petSA <- F.fromEither(
        runtimeServiceAccountOpt.toRight(new Exception(s"user ${userInfo.userEmail.value} doesn't have a PET SA"))
      )

      // Fail fast if the Galaxy disk, memory, number of CPUs is too small
      appMachineType <-
        if (req.appType == AppType.Galaxy) {
          validateGalaxy(googleProject, req.diskConfig.flatMap(_.size), machineConfig.machineType).map(_.some)
        } else F.pure(None)

      // if request was created by pet SA, processPersistentDiskRequest will look up it's corresponding user email
      // as needed
      diskResultOpt <- req.diskConfig.traverse(diskReq =>
        RuntimeServiceInterp.processPersistentDiskRequest(
          diskReq,
          config.leoKubernetesConfig.diskConfig.defaultZone, // this need to be updated if we support non-default zone for k8s apps
          googleProject,
          userInfo,
          petSA,
          appTypeToFormattedByType(req.appType),
          authProvider,
          config.leoKubernetesConfig.diskConfig
        )
      )

      lastUsedApp <- diskResultOpt.flatTraverse { diskResult =>
        if (diskResult.creationNeeded) F.pure(none[LastUsedApp])
        else {
          (diskResult.disk.formattedBy, diskResult.disk.appRestore) match {
            case (Some(FormattedBy.Galaxy), Some(GalaxyRestore(_, _, _))) |
                (Some(FormattedBy.Cromwell), Some(CromwellRestore(_))) =>
              val lastUsedBy = diskResult.disk.appRestore.get.lastUsedBy
              for {
                lastUsedOpt <- appQuery.getLastUsedApp(lastUsedBy, Some(ctx.traceId)).transaction
                lastUsed <- F.fromOption(
                  lastUsedOpt,
                  new LeoException(s"last used app($lastUsedBy) not found", traceId = Some(ctx.traceId))
                )
                _ <- req.customEnvironmentVariables.get(WORKSPACE_NAME_KEY).traverse { s =>
                  if (lastUsed.workspace.asString == s) F.unit
                  else
                    F.raiseError[Unit](
                      BadRequestException(
                        s"workspace name has to be the same as last used app in order to restore data from existing disk",
                        Some(ctx.traceId)
                      )
                    )
                }
              } yield lastUsed.some
            case (Some(FormattedBy.Galaxy), Some(CromwellRestore(_))) =>
              F.raiseError[Option[LastUsedApp]](
                DiskAlreadyFormattedError(FormattedBy.Galaxy, FormattedBy.Cromwell.asString, ctx.traceId)
              )
            case (Some(FormattedBy.Cromwell), Some(GalaxyRestore(_, _, _))) =>
              F.raiseError[Option[LastUsedApp]](
                DiskAlreadyFormattedError(FormattedBy.Cromwell, FormattedBy.Galaxy.asString, ctx.traceId)
              )
            case (Some(FormattedBy.GCE), _) | (Some(FormattedBy.Custom), _) =>
              F.raiseError[Option[LastUsedApp]](
                DiskAlreadyFormattedError(diskResult.disk.formattedBy.get,
                                          s"${FormattedBy.Cromwell.asString} or ${FormattedBy.Galaxy.asString}",
                                          ctx.traceId
                )
              )
            case (Some(FormattedBy.Galaxy), None) | (Some(FormattedBy.Cromwell), None) =>
              F.raiseError[Option[LastUsedApp]](
                new LeoException("Existing disk found, but no restore info found in DB", traceId = Some(ctx.traceId))
              )
            case (None, _) =>
              F.raiseError[Option[LastUsedApp]](
                new LeoException(
                  "Disk is not formatted yet. Only disks previously used by galaxy app can be re-used to create a new galaxy app",
                  traceId = Some(ctx.traceId)
                )
              )
          }
        }
      }
      saveApp <- F.fromEither(
        getSavableApp(googleProject,
                      appName,
                      originatingUserEmail,
                      samResourceId,
                      req,
                      diskResultOpt.map(_.disk),
                      lastUsedApp,
                      petSA,
                      nodepool.id,
                      ctx
        )
      )
      app <- appQuery.save(saveApp, Some(ctx.traceId)).transaction

      clusterNodepoolAction = saveClusterResult match {
        case ClusterExists(_) =>
          // If we're using a pre-existing nodepool then don't specify CreateNodepool in the pubsub message
          if (userNodepoolOpt.isDefined) None else Some(ClusterNodepoolAction.CreateNodepool(nodepool.id))
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
        app.appResources.namespace.name,
        appMachineType,
        Some(ctx.traceId)
      )
      _ <- publisherQueue.offer(createAppMessage)
    } yield ()

  override def getApp(
    userInfo: UserInfo,
    cloudContext: CloudContext,
    appName: AppName
  )(implicit
    as: Ask[F, AppContext]
  ): F[GetAppResponse] =
    for {
      ctx <- as.ask
      appOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(cloudContext, appName).transaction
      app <- F.fromOption(appOpt, AppNotFoundException(cloudContext, appName, ctx.traceId))

      hasPermission <- authProvider.hasPermission(app.app.samResourceId, AppAction.GetAppStatus, userInfo)
      _ <-
        if (hasPermission) F.unit
        else
          log.info(ctx.loggingCtx)(
            s"User ${userInfo} tried to access app ${appName.value} without proper permissions. Returning 404"
          ) >> F
            .raiseError[Unit](AppNotFoundException(cloudContext, appName, ctx.traceId))
    } yield GetAppResponse.fromDbResult(app, Config.proxyConfig.proxyUrlBase)

  override def listApp(
    userInfo: UserInfo,
    cloudContext: Option[CloudContext],
    params: Map[String, String]
  )(implicit as: Ask[F, AppContext]): F[Vector[ListAppResponse]] =
    for {
      paramMap <- F.fromEither(processListParameters(params))
      allClusters <- KubernetesServiceDbQueries
        .listFullApps(cloudContext, paramMap._1, paramMap._2)
        .transaction
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
              c.copy(
                nodepools = c.nodepools
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
            .flatMap(c => ListAppResponse.fromCluster(c, Config.proxyConfig.proxyUrlBase, paramMap._3))
            .toVector
      }
    } yield res

  override def deleteApp(request: DeleteAppRequest)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      appOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(request.cloudContext, request.appName)
        .transaction
      appResult <- F.fromOption(appOpt, AppNotFoundException(request.cloudContext, request.appName, ctx.traceId))

      listOfPermissions <- authProvider.getActions(appResult.app.samResourceId, request.userInfo)

      // throw 404 if no GetAppStatus permission
      hasReadPermission = listOfPermissions.toSet.contains(AppAction.GetAppStatus)
      _ <-
        if (hasReadPermission) F.unit
        else F.raiseError[Unit](AppNotFoundException(request.cloudContext, request.appName, ctx.traceId))

      // throw 403 if no DeleteApp permission
      hasDeletePermission = listOfPermissions.toSet.contains(AppAction.DeleteApp)
      _ <- if (hasDeletePermission) F.unit else F.raiseError[Unit](ForbiddenError(request.userInfo.userEmail))

      canDelete = AppStatus.deletableStatuses.contains(appResult.app.status)
      _ <-
        if (canDelete) F.unit
        else
          F.raiseError[Unit](
            AppCannotBeDeletedException(request.cloudContext, request.appName, appResult.app.status, ctx.traceId)
          )

      // Get the disk to delete if specified
      diskOpt = if (request.deleteDisk) appResult.app.appResources.disk.map(_.id) else None

      // If the app status is Error, we can assume that the underlying app/nodepool
      // has already been deleted. So we just transition the app to Deleted status
      // without sending a message to Back Leo.
      //
      // Note this has the side effect of not deleting the disk if requested to do so. The
      // caller must manually delete the disk in this situation. We have the same behavior for
      // runtimes.
      googleProjectOpt = LeoLenses.cloudContextToGoogleProject.get(appResult.cluster.cloudContext)
      _ <-
        if (appResult.app.status == AppStatus.Error) {
          for {
            // we only need to delete Sam record for clusters in Google. Sam record for Azure is managed by WSM
            _ <- googleProjectOpt.traverse(g =>
              authProvider.notifyResourceDeleted(appResult.app.samResourceId, appResult.app.auditInfo.creator, g)
            )
            _ <- appQuery.markAsDeleted(appResult.app.id, ctx.now).transaction
          } yield ()
        } else {
          for {
            _ <- KubernetesServiceDbQueries.markPreDeleting(appResult.nodepool.id, appResult.app.id).transaction
            deleteMessage = appResult.cluster.cloudContext match {
              case CloudContext.Gcp(value) =>
                DeleteAppMessage(
                  appResult.app.id,
                  appResult.app.appName,
                  value,
                  diskOpt,
                  Some(ctx.traceId)
                )
              case CloudContext.Azure(_) => ???
            }
            _ <- publisherQueue.offer(deleteMessage)
          } yield ()
        }
    } yield ()

  def stopApp(userInfo: UserInfo, cloudContext: CloudContext, appName: AppName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      appOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(cloudContext, appName).transaction
      appResult <- F.fromOption(appOpt, AppNotFoundException(cloudContext, appName, ctx.traceId))

      listOfPermissions <- authProvider.getActions(appResult.app.samResourceId, userInfo)

      // throw 404 if no StopStartApp permission
      hasReadPermission = listOfPermissions.toSet.contains(AppAction.StopApp)
      _ <-
        if (hasReadPermission) F.unit
        else F.raiseError[Unit](AppNotFoundException(cloudContext, appName, ctx.traceId))

      // throw 403 if no StopStartApp permission
      hasStopStartPermission = listOfPermissions.toSet.contains(AppAction.StopApp)
      _ <- if (hasStopStartPermission) F.unit else F.raiseError[Unit](ForbiddenError(userInfo.userEmail))

      canStop = AppStatus.stoppableStatuses.contains(appResult.app.status)
      _ <-
        if (canStop) F.unit
        else
          F.raiseError[Unit](
            AppCannotBeStoppedException(cloudContext, appName, appResult.app.status, ctx.traceId)
          )

      _ <- appQuery.updateStatus(appResult.app.id, AppStatus.PreStopping).transaction
      // TODO: support Azure
      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(appResult.cluster.cloudContext),
        AzureUnimplementedException("Azure runtime is not supported yet")
      )
      message = StopAppMessage(
        appResult.app.id,
        appResult.app.appName,
        googleProject,
        Some(ctx.traceId)
      )
      _ <- publisherQueue.offer(message)
    } yield ()

  def startApp(userInfo: UserInfo, cloudContext: CloudContext, appName: AppName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      appOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(cloudContext, appName).transaction
      appResult <- F.fromOption(appOpt, AppNotFoundException(cloudContext, appName, ctx.traceId))

      listOfPermissions <- authProvider.getActions(appResult.app.samResourceId, userInfo)

      // throw 404 if no StopStartApp permission
      hasReadPermission = listOfPermissions.toSet.contains(AppAction.StartApp)
      _ <-
        if (hasReadPermission) F.unit
        else F.raiseError[Unit](AppNotFoundException(cloudContext, appName, ctx.traceId))

      // throw 403 if no StopStartApp permission
      hasStopStartPermission = listOfPermissions.toSet.contains(AppAction.StartApp)
      _ <- if (hasStopStartPermission) F.unit else F.raiseError[Unit](ForbiddenError(userInfo.userEmail))

      canStop = AppStatus.startableStatuses.contains(appResult.app.status)
      _ <-
        if (canStop) F.unit
        else
          F.raiseError[Unit](
            AppCannotBeStartedException(cloudContext, appName, appResult.app.status, ctx.traceId)
          )

      _ <- appQuery.updateStatus(appResult.app.id, AppStatus.PreStarting).transaction
      // TODO: support Azure
      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(cloudContext),
        AzureUnimplementedException("Azure runtime is not supported yet")
      )
      message = StartAppMessage(
        appResult.app.id,
        appResult.app.appName,
        googleProject,
        Some(ctx.traceId)
      )
      _ <- publisherQueue.offer(message)
    } yield ()

  private[service] def getSavableCluster(
    userEmail: WorkbenchEmail,
    cloudContext: CloudContext,
    now: Instant,
    numNodepools: Option[NumNodepools],
    clusterName: Option[KubernetesClusterName] = None
  ): Either[Throwable, SaveKubernetesCluster] = {
    val auditInfo = AuditInfo(userEmail, now, None, now)

    val defaultNodepool = for {
      nodepoolName <- KubernetesNameUtils.getUniqueName(NodepoolName.apply)
    } yield DefaultNodepool(
      NodepoolLeoId(-1),
      clusterId = KubernetesClusterLeoId(-1),
      nodepoolName,
      status = NodepoolStatus.Precreating,
      auditInfo,
      machineType = config.leoKubernetesConfig.nodepoolConfig.defaultNodepoolConfig.machineType,
      numNodes = numNodepools
        .map(n =>
          NumNodes(
            math
              .ceil(
                n.value.toDouble /
                  config.leoKubernetesConfig.nodepoolConfig.defaultNodepoolConfig.maxNodepoolsPerDefaultNode.value.toDouble
              )
              .toInt
          )
        )
        .getOrElse(config.leoKubernetesConfig.nodepoolConfig.defaultNodepoolConfig.numNodes),
      autoscalingEnabled = config.leoKubernetesConfig.nodepoolConfig.defaultNodepoolConfig.autoscalingEnabled,
      autoscalingConfig = None
    )

    for {
      nodepool <- defaultNodepool
      defaultClusterName <- KubernetesNameUtils.getUniqueName(KubernetesClusterName.apply)
    } yield SaveKubernetesCluster(
      cloudContext = cloudContext,
      clusterName = clusterName.getOrElse(defaultClusterName),
      location = config.leoKubernetesConfig.clusterConfig.location,
      region = config.leoKubernetesConfig.clusterConfig.region,
      status = KubernetesClusterStatus.Precreating,
      ingressChart = config.leoKubernetesConfig.ingressConfig.chart,
      auditInfo = auditInfo,
      defaultNodepool = nodepool
    )
  }

  private def checkIfUserAllowed(appType: AppType, userEmail: WorkbenchEmail)(implicit ev: Ask[F, TraceId]): F[Unit] =
    if (config.enableCustomAppGroupPermissionCheck)
      for {
        ctx <- ev.ask
        isUserAllowed <- appType match {
          case AppType.Custom => authProvider.isCustomAppAllowed(userEmail)
          case _              => F.pure(true)
        }
        _ <- F.whenA(!isUserAllowed)(log.info(Map("traceId" -> ctx.asString))("user is not in CUSTOM_APP_USERS group"))
        _ <- F.raiseWhen(!isUserAllowed)(ForbiddenError(userEmail))
      } yield ()
    else F.unit

  private[service] def getUserNodepool(clusterId: KubernetesClusterLeoId,
                                       userEmail: WorkbenchEmail,
                                       runtimeConfig: Option[KubernetesRuntimeConfig],
                                       now: Instant
  ): Either[Throwable, Nodepool] = {
    val auditInfo = AuditInfo(userEmail, now, None, now)

    val machineConfig = runtimeConfig.getOrElse(
      KubernetesRuntimeConfig(
        config.leoKubernetesConfig.nodepoolConfig.galaxyNodepoolConfig.numNodes,
        config.leoKubernetesConfig.nodepoolConfig.galaxyNodepoolConfig.machineType,
        config.leoKubernetesConfig.nodepoolConfig.galaxyNodepoolConfig.autoscalingEnabled
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
      autoscalingConfig = Some(config.leoKubernetesConfig.nodepoolConfig.galaxyNodepoolConfig.autoscalingConfig),
      List.empty,
      false
    )
  }

  private[service] def validateGalaxy(googleProject: GoogleProject,
                                      diskSize: Option[DiskSize],
                                      machineTypeName: MachineTypeName
  )(implicit
    as: Ask[F, AppContext]
  ): F[AppMachineType] =
    for {
      ctx <- as.ask
      _ <- diskSize
        .traverse_ { size =>
          if (size.gb < config.leoKubernetesConfig.galaxyDiskConfig.nfsMinimumDiskSizeGB.gb) {
            F.raiseError[Unit](
              BadRequestException(
                s"Galaxy disk must be at least ${config.leoKubernetesConfig.galaxyDiskConfig.nfsMinimumDiskSizeGB}",
                Some(ctx.traceId)
              )
            )
          } else F.unit
        }
      machineType <- computeService
        .getMachineType(googleProject,
                        ZoneName("us-central1-a"),
                        machineTypeName
        ) // TODO: if use non `us-central1-a` zone for galaxy, this needs to be udpated
        .flatMap(opt =>
          F.fromOption(
            opt,
            new LeoException(s"Unknown machine type for ${machineTypeName.value}", traceId = Some(ctx.traceId))
          )
        )
      memoryInGb = machineType.getMemoryMb / 1024
      machineType <-
        if (memoryInGb < config.leoKubernetesConfig.galaxyAppConfig.minMemoryGb)
          F.raiseError(BadRequestException("Galaxy needs more memory configuration", Some(ctx.traceId)))
        else if (machineType.getGuestCpus < config.leoKubernetesConfig.galaxyAppConfig.minNumOfCpus)
          F.raiseError(BadRequestException("Galaxy needs more CPU configuration", Some(ctx.traceId)))
        else F.pure(AppMachineType(memoryInGb, machineType.getGuestCpus))
    } yield machineType

  private[service] def getSavableApp(googleProject: GoogleProject,
                                     appName: AppName,
                                     userEmail: WorkbenchEmail,
                                     samResourceId: AppSamResourceId,
                                     req: CreateAppRequest,
                                     diskOpt: Option[PersistentDisk],
                                     lastUsedApp: Option[LastUsedApp],
                                     googleServiceAccount: WorkbenchEmail,
                                     nodepoolId: NodepoolLeoId,
                                     ctx: AppContext
  ): Either[Throwable, SaveApp] = {
    val now = ctx.now
    val auditInfo = AuditInfo(userEmail, now, None, now)
    val gkeAppConfig: GkeAppConfig = req.appType match {
      case Galaxy   => config.leoKubernetesConfig.galaxyAppConfig
      case Cromwell => config.leoKubernetesConfig.cromwellAppConfig
      case Custom   => config.leoKubernetesConfig.customAppConfig
    }

    val allLabels =
      DefaultKubernetesLabels(
        googleProject,
        appName,
        userEmail,
        config.leoKubernetesConfig.serviceAccountConfig.leoServiceAccountEmail
      ).toMap ++ req.labels
    for {
      // check the labels do not contain forbidden keys
      labels <-
        if (allLabels.contains(includeDeletedKey))
          Left(IllegalLabelKeyException(includeDeletedKey))
        else
          Right(allLabels)

      // TODO make this non optional in the request
      // the original thought when developing was that galaxy needs a disk, but some apps may not
      // all apps require a disk
      disk <- diskOpt.toRight(AppRequiresDiskException(googleProject, appName, req.appType, ctx.traceId))

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
      namespaceId = lastUsedApp.fold(
        NamespaceId(-1)
      )(app => app.namespaceId)
      namespaceName <- lastUsedApp.fold(
        KubernetesName.withValidation(
          s"${uid}-${gkeAppConfig.namespaceNameSuffix.value}",
          NamespaceName.apply
        )
      )(app => app.namespaceName.asRight[Throwable])

      chart = lastUsedApp
        .map { lastUsed =>
          // if there's a patch version bump, then we use the later version defined in leo config; otherwise, we use lastUsed chart definition
          if (
            lastUsed.chart.name == gkeAppConfig.chartName && isPatchVersionDifference(lastUsed.chart.version,
                                                                                      gkeAppConfig.chartVersion
            )
          )
            gkeAppConfig.chart
          else lastUsed.chart
        }
        // TODO fix this for the cromwell case (BW-867)
        // For now for cromwell apps, the chart is wrong in the Leo DB.
        // Back Leo reads the chart from config instead of the DB.
        .getOrElse(gkeAppConfig.chart)
      release <- lastUsedApp.fold(
        KubernetesName.withValidation(
          s"${uid}-${gkeAppConfig.releaseNameSuffix.value}",
          Release.apply
        )
      )(app => app.release.asRight[Throwable])
    } yield SaveApp(
      App(
        AppId(-1),
        nodepoolId,
        req.appType,
        appName,
        AppStatus.Precreating,
        chart,
        release,
        samResourceId,
        googleServiceAccount,
        auditInfo,
        labels,
        AppResources(
          Namespace(
            namespaceId,
            namespaceName
          ),
          Some(disk),
          gkeAppConfig.kubernetesServices,
          Option(gkeAppConfig.serviceAccountName)
        ),
        List.empty,
        req.customEnvironmentVariables,
        req.descriptorPath,
        req.extraArgs
      )
    )
  }

}

object LeoAppServiceInterp {
  case class LeoKubernetesConfig(serviceAccountConfig: ServiceAccountProviderConfig,
                                 clusterConfig: KubernetesClusterConfig,
                                 nodepoolConfig: NodepoolConfig,
                                 ingressConfig: KubernetesIngressConfig,
                                 galaxyAppConfig: GalaxyAppConfig,
                                 galaxyDiskConfig: GalaxyDiskConfig,
                                 diskConfig: PersistentDiskConfig,
                                 cromwellAppConfig: CromwellAppConfig,
                                 customAppConfig: CustomAppConfig
  )

  private[http] def isPatchVersionDifference(a: ChartVersion, b: ChartVersion): Boolean = {
    val aSplited = a.asString.split("\\.")
    val bSplited = b.asString.split("\\.")
    aSplited(0) == bSplited(0) && aSplited(1) == bSplited(1)
  }
}
case class AppNotFoundException(cloudContext: CloudContext, appName: AppName, traceId: TraceId)
    extends LeoException(
      s"App ${cloudContext.asStringWithProvider}/${appName.value} not found.",
      StatusCodes.NotFound,
      traceId = Some(traceId)
    )

case class AppAlreadyExistsException(cloudContext: CloudContext, appName: AppName, status: AppStatus, traceId: TraceId)
    extends LeoException(
      s"App ${cloudContext.asStringWithProvider}/${appName.value} already exists in ${status.toString} status.",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )

case class AppCannotBeDeletedException(cloudContext: CloudContext,
                                       appName: AppName,
                                       status: AppStatus,
                                       traceId: TraceId
) extends LeoException(
      s"App ${cloudContext.asStringWithProvider}/${appName.value} cannot be deleted in ${status} status." +
        (if (status == AppStatus.Stopped) " Please start the app first." else ""),
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )

case class AppCannotBeCreatedException(googleProject: GoogleProject,
                                       appName: AppName,
                                       status: AppStatus,
                                       traceId: TraceId
) extends LeoException(
      s"App ${googleProject.value}/${appName.value} cannot be deleted in ${status} status.",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )

case class AppRequiresDiskException(googleProject: GoogleProject, appName: AppName, appType: AppType, traceId: TraceId)
    extends LeoException(
      s"App ${googleProject.value}/${appName.value} cannot be created because the request does not contain a valid disk. Apps of type ${appType} require a disk. Trace ID: ${traceId.asString}",
      StatusCodes.BadRequest,
      traceId = Some(traceId)
    )

case class ClusterExistsException(googleProject: GoogleProject)
    extends LeoException(
      s"Cannot pre-create nodepools for project $googleProject because a cluster already exists",
      StatusCodes.Conflict,
      traceId = None
    )

case class AppCannotBeStoppedException(cloudContext: CloudContext,
                                       appName: AppName,
                                       status: AppStatus,
                                       traceId: TraceId
) extends LeoException(
      s"App ${cloudContext.asStringWithProvider}/${appName.value} cannot be stopped in ${status} status. Trace ID: ${traceId.asString}",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )

case class AppCannotBeStartedException(cloudContext: CloudContext,
                                       appName: AppName,
                                       status: AppStatus,
                                       traceId: TraceId
) extends LeoException(
      s"App ${cloudContext.asStringWithProvider}/${appName.value} cannot be started in ${status} status. Trace ID: ${traceId.asString}",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )
