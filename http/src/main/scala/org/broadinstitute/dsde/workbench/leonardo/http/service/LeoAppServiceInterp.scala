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
import com.azure.core.management.Region
import org.broadinstitute.dsde.workbench.azure.AKSClusterName
import org.broadinstitute.dsde.workbench.google2.GKEModels.{KubernetesClusterName, NodepoolName}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.NamespaceName
import org.broadinstitute.dsde.workbench.google2.{DiskName, GoogleComputeService, GoogleResourceService, KubernetesName, MachineTypeName, RegionName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.AppRestore.GalaxyRestore
import org.broadinstitute.dsde.workbench.leonardo.AppType._
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId._
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.dao.{SamDAO, WsmDao}
import org.broadinstitute.dsp.ChartName
import org.broadinstitute.dsde.workbench.leonardo.db.KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeoAppServiceInterp.isPatchVersionDifference
import org.broadinstitute.dsde.workbench.leonardo.model.SamResourceAction._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.{ClusterNodepoolAction, LeoPubsubMessage}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsp.{ChartVersion, Release}
import org.http4s.{AuthScheme, Uri}
import org.typelevel.log4cats.StructuredLogger
import monocle.macros.syntax.lens._

import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext

final class LeoAppServiceInterp[F[_]: Parallel](config: AppServiceConfig,
                                                authProvider: LeoAuthProvider[F],
                                                serviceAccountProvider: ServiceAccountProvider[F],
                                                publisherQueue: Queue[F, LeoPubsubMessage],
                                                computeService: GoogleComputeService[F],
                                                googleResourceService: GoogleResourceService[F],
                                                customAppConfig: CustomAppConfig,
                                                wsmDao: WsmDao[F],
                                                samDAO: SamDAO[F]
)(implicit
  F: Async[F],
  log: StructuredLogger[F],
  dbReference: DbReference[F],
  metrics: OpenTelemetryMetrics[F],
  ec: ExecutionContext
) extends AppService[F] {
  override def createApp(
    userInfo: UserInfo,
    cloudContext: CloudContext.Gcp,
    appName: AppName,
    req: CreateAppRequest,
    workspaceId: Option[WorkspaceId] = None
  )(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      googleProject = cloudContext.value
      hasPermission <- authProvider.hasPermission[ProjectSamResourceId, ProjectAction](
        ProjectSamResourceId(googleProject),
        ProjectAction.CreateApp,
        userInfo
      )
      _ <- F.raiseWhen(!hasPermission)(ForbiddenError(userInfo.userEmail))

      enableIntraNodeVisibility = req.labels.get(AOU_UI_LABEL).isDefined
      _ <- req.appType match {
        case AppType.Galaxy | AppType.HailBatch | AppType.Wds | AppType.Cromwell | AppType.WorkflowsApp |
            AppType.CromwellRunnerApp =>
          F.unit
        case AppType.Allowed =>
          req.allowedChartName match {
            case Some(cn) =>
              cn match {
                case AllowedChartName.RStudio => F.unit
                case AllowedChartName.Sas =>
                  if (config.enableSasAppGroupCheck)
                    authProvider.isSasAppAllowed(userInfo.userEmail) flatMap { res =>
                      if (res) F.unit
                      else
                        F.raiseError[Unit](
                          AuthenticationError(Some(userInfo.userEmail),
                                              "You need to obtain a license in order to create a SAS App"
                          )
                        )
                    }
                  else F.unit
              }
            case None =>
              F.raiseError(
                BadRequestException("when AppType is ALLOWED, allowedChartName needs to be specified",
                                    Some(ctx.traceId)
                )
              )
          }
        case AppType.Custom =>
          req.descriptorPath match {
            case Some(descriptorPath) =>
              checkIfAppCreationIsAllowed(userInfo.userEmail, googleProject, descriptorPath)
            case None =>
              F.raiseError(
                BadRequestException(
                  "DescriptorPath is undefined - when AppType is Custom, descriptorPath should be defined.",
                  Some(ctx.traceId)
                )
              )
          }
      }

      appOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(CloudContext.Gcp(googleProject), appName).transaction
      _ <- appOpt.fold(F.unit)(c =>
        F.raiseError[Unit](AppAlreadyExistsException(cloudContext, appName, c.app.status, ctx.traceId))
      )

      // Shared app SAM resource is not enabled for V1 endpoint
      _ <- F.raiseWhen(req.accessScope != None)(
        BadRequestException("accessScope is not a V1 parameter", Some(ctx.traceId))
      )

      samResourceId <- F.delay(AppSamResourceId(UUID.randomUUID().toString, req.accessScope))

      // Look up the original email in case this API was called by a pet SA
      originatingUserEmail <- authProvider.lookupOriginatingUserEmail(userInfo)
      _ <- authProvider
        .notifyResourceCreated(samResourceId, originatingUserEmail, googleProject)
        .handleErrorWith { t =>
          log.error(ctx.loggingCtx, t)(
            s"Failed to notify the AuthProvider for creation of kubernetes app ${googleProject.value} / ${appName.value}"
          ) >> F.raiseError[Unit](t)
        }

      saveCluster <- F.fromEither(
        getSavableCluster(originatingUserEmail, cloudContext, ctx.now, None, None)
      )

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
              getUserNodepool(clusterId, cloudContext, originatingUserEmail, machineConfig, ctx.now)
            )
            savedNodepool <- nodepoolQuery.saveForCluster(saveNodepool).transaction
          } yield savedNodepool
      }

      runtimeServiceAccountOpt <- serviceAccountProvider
        .getClusterServiceAccount(userInfo, cloudContext)
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
      lastUsedApp <- getLastUsedAppForDisk(req, diskResultOpt)
      saveApp <- F.fromEither(
        getSavableApp(cloudContext,
                      appName,
                      originatingUserEmail,
                      samResourceId,
                      req,
                      diskResultOpt.map(_.disk),
                      lastUsedApp,
                      petSA,
                      nodepool.id,
                      None,
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
        Some(ctx.traceId),
        enableIntraNodeVisibility
      )
      _ <- publisherQueue.offer(createAppMessage)
    } yield ()

  override def getApp(
    userInfo: UserInfo,
    cloudContext: CloudContext.Gcp,
    appName: AppName
  )(implicit
    as: Ask[F, AppContext]
  ): F[GetAppResponse] =
    for {
      ctx <- as.ask
      // throw 403 if no project-level permission
      hasProjectPermission <- authProvider.isUserProjectReader(
        cloudContext,
        userInfo
      )
      _ <- F.raiseWhen(!hasProjectPermission)(ForbiddenError(userInfo.userEmail, Some(ctx.traceId)))

      appOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(cloudContext, appName).transaction
      app <- F.fromOption(appOpt, AppNotFoundException(cloudContext, appName, ctx.traceId, "No active app found in DB"))

      // throw 404 if no GetAppStatus permission
      hasPermission <- authProvider.hasPermission[AppSamResourceId, AppAction](app.app.samResourceId,
                                                                               AppAction.GetAppStatus,
                                                                               userInfo
      )
      _ <-
        if (hasPermission) F.unit
        else
          log.info(ctx.loggingCtx)(
            s"User ${userInfo} tried to access app ${appName.value} without proper permissions. Returning 404"
          ) >>
            F.raiseError[Unit](
              AppNotFoundException(cloudContext, appName, ctx.traceId, "Permission Denied")
            )
    } yield GetAppResponse.fromDbResult(app, Config.proxyConfig.proxyUrlBase)

  override def listApp(
    userInfo: UserInfo,
    cloudContext: Option[CloudContext.Gcp],
    params: Map[String, String]
  )(implicit as: Ask[F, AppContext]): F[Vector[ListAppResponse]] =
    for {
      ctx <- as.ask

      // throw 403 if no project-level permission and project provided
      hasProjectPermission <- cloudContext.traverse(cc =>
        authProvider.isUserProjectReader(
          cc,
          userInfo
        )
      )
      _ <- F.raiseWhen(!hasProjectPermission.getOrElse(true))(ForbiddenError(userInfo.userEmail, Some(ctx.traceId)))

      paramMap <- F.fromEither(processListParameters(params))
      creatorOnly <- F.fromEither(processCreatorOnlyParameter(userInfo.userEmail, params, ctx.traceId))
      allClusters <- KubernetesServiceDbQueries
        .listFullApps(cloudContext, paramMap._1, paramMap._2, creatorOnly)
        .transaction

      // V1 endpoints use google project to determine user access
      // listAll apps includes both Azure and GCP apps
      // but Azure apps don't have a google project, so useGoogleProject is false for Azure apps
      partition = allClusters.partition(_.cloudContext.isInstanceOf[CloudContext.Gcp])
      gcpApps <- filterAppsBySamPermission(partition._1, userInfo, paramMap._3, true)
      azureApps <- filterAppsBySamPermission(partition._2, userInfo, paramMap._3, false)
    } yield gcpApps ++ azureApps

  override def deleteApp(userInfo: UserInfo, cloudContext: CloudContext.Gcp, appName: AppName, deleteDisk: Boolean)(
    implicit as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      // throw 403 if no project-level permission
      hasProjectPermission <- authProvider.isUserProjectReader(
        cloudContext,
        userInfo
      )
      _ <- F.raiseWhen(!hasProjectPermission)(ForbiddenError(userInfo.userEmail, Some(ctx.traceId)))

      appOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(cloudContext, appName)
        .transaction
      appResult <- F.fromOption(
        appOpt,
        AppNotFoundException(cloudContext, appName, ctx.traceId, "No active app found in DB")
      )
      tags = Map("appType" -> appResult.app.appType.toString, "deleteDisk" -> deleteDisk.toString)
      _ <- metrics.incrementCounter("deleteApp", 1, tags)
      listOfPermissions <- authProvider.getActions(appResult.app.samResourceId, userInfo)

      // throw 404 if no GetAppStatus permission
      hasPermission = listOfPermissions.toSet.contains(AppAction.GetAppStatus)
      _ <-
        if (hasPermission) F.unit
        else
          F.raiseError[Unit](
            AppNotFoundException(cloudContext, appName, ctx.traceId, "Permission Denied")
          )

      // throw 403 if no DeleteApp permission
      hasDeletePermission = listOfPermissions.toSet.contains(AppAction.DeleteApp)
      _ <- if (hasDeletePermission) F.unit else F.raiseError[Unit](ForbiddenError(userInfo.userEmail))

      canDelete = AppStatus.deletableStatuses.contains(appResult.app.status)
      _ <-
        if (canDelete) F.unit
        else
          F.raiseError[Unit](
            AppCannotBeDeletedException(cloudContext, appName, appResult.app.status, ctx.traceId)
          )

      // Get the disk to delete if specified
      diskOpt = if (deleteDisk) appResult.app.appResources.disk.map(_.id) else None

      // If the app status is Error, we can assume that the underlying app/nodepool
      // has already been deleted. So we just transition the app to Deleted status
      // without sending a message to Back Leo.
      //
      // Note this has the side effect of not deleting the disk if requested to do so. The
      // caller must manually delete the disk in this situation. We have the same behavior for
      // runtimes.
      _ <-
        if (appResult.app.status == AppStatus.Error) {
          for {
            // we only need to delete Sam record for clusters in Google. Sam record for Azure is managed by WSM
            _ <- authProvider.notifyResourceDeleted(appResult.app.samResourceId,
                                                    appResult.app.auditInfo.creator,
                                                    cloudContext.value
            )
            _ <- appQuery.markAsDeleted(appResult.app.id, ctx.now).transaction
          } yield ()
        } else {
          for {
            _ <- KubernetesServiceDbQueries.markPreDeleting(appResult.app.id).transaction
            deleteMessage = DeleteAppMessage(
              appResult.app.id,
              appResult.app.appName,
              cloudContext.value,
              diskOpt,
              Some(ctx.traceId)
            )
            _ <- publisherQueue.offer(deleteMessage)
          } yield ()
        }
    } yield ()

  def stopApp(userInfo: UserInfo, cloudContext: CloudContext.Gcp, appName: AppName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      // throw 403 if no project-level permission
      hasProjectPermission <- authProvider.isUserProjectReader(
        cloudContext,
        userInfo
      )
      _ <- F.raiseWhen(!hasProjectPermission)(ForbiddenError(userInfo.userEmail, Some(ctx.traceId)))

      appOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(cloudContext, appName).transaction
      appResult <- F.fromOption(appOpt,
                                AppNotFoundException(cloudContext, appName, ctx.traceId, "No active app found in DB")
      )
      tags = Map("appType" -> appResult.app.appType.toString)
      _ <- metrics.incrementCounter("stopApp", 1, tags)
      listOfPermissions <- authProvider.getActions(appResult.app.samResourceId, userInfo)

      // throw 404 if no StopStartApp permission
      hasReadPermission = listOfPermissions.toSet.contains(AppAction.StopApp)
      _ <-
        if (hasReadPermission) F.unit
        else F.raiseError[Unit](AppNotFoundException(cloudContext, appName, ctx.traceId, "no read permission"))

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
      message = StopAppMessage(
        appResult.app.id,
        appResult.app.appName,
        cloudContext.value,
        Some(ctx.traceId)
      )
      _ <- publisherQueue.offer(message)
    } yield ()

  def startApp(userInfo: UserInfo, cloudContext: CloudContext.Gcp, appName: AppName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      // throw 403 if no project-level permission
      hasProjectPermission <- authProvider.isUserProjectReader(
        cloudContext,
        userInfo
      )
      _ <- F.raiseWhen(!hasProjectPermission)(ForbiddenError(userInfo.userEmail, Some(ctx.traceId)))

      appOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(cloudContext, appName).transaction
      appResult <- F.fromOption(appOpt,
                                AppNotFoundException(cloudContext, appName, ctx.traceId, "No active app found in DB")
      )
      tags = Map("appType" -> appResult.app.appType.toString)
      _ <- metrics.incrementCounter("startApp", 1, tags)
      listOfPermissions <- authProvider.getActions(appResult.app.samResourceId, userInfo)

      // throw 404 if no StopStartApp permission
      hasReadPermission = listOfPermissions.toSet.contains(AppAction.StartApp)
      _ <-
        if (hasReadPermission) F.unit
        else F.raiseError[Unit](AppNotFoundException(cloudContext, appName, ctx.traceId, "no read permission"))

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
      message = StartAppMessage(
        appResult.app.id,
        appResult.app.appName,
        cloudContext.value,
        Some(ctx.traceId)
      )
      _ <- publisherQueue.offer(message)
    } yield ()

  override def listAppV2(userInfo: UserInfo, workspaceId: WorkspaceId, params: Map[String, String])(implicit
    as: Ask[F, AppContext]
  ): F[Vector[ListAppResponse]] =
    for {
      // Make sure that the user still has access to the resource parent workspace
      hasWorkspacePermission <- authProvider.isUserWorkspaceReader(
        WorkspaceResourceSamResourceId(workspaceId),
        userInfo
      )
      _ <- F.raiseUnless(hasWorkspacePermission)(ForbiddenError(userInfo.userEmail))

      paramMap <- F.fromEither(processListParameters(params))
      allClusters <- KubernetesServiceDbQueries
        .listFullAppsByWorkspaceId(Some(workspaceId), paramMap._1, paramMap._2)
        .transaction
      res <- filterAppsBySamPermission(allClusters, userInfo, paramMap._3, false)

    } yield res

  override def getAppV2(userInfo: UserInfo, workspaceId: WorkspaceId, appName: AppName)(implicit
    as: Ask[F, AppContext]
  ): F[GetAppResponse] =
    for {
      ctx <- as.ask
      appOpt <- getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName).transaction
      app <- F.fromOption(
        appOpt,
        AppNotFoundByWorkspaceIdException(workspaceId, appName, ctx.traceId, "No active app found in DB")
      )
      hasWorkspacePermission <- authProvider.isUserWorkspaceReader(
        WorkspaceResourceSamResourceId(workspaceId),
        userInfo
      )
      _ <- F.raiseUnless(hasWorkspacePermission)(ForbiddenError(userInfo.userEmail))

      hasResourcePermission <- authProvider.hasPermission[AppSamResourceId, AppAction](app.app.samResourceId,
                                                                                       AppAction.GetAppStatus,
                                                                                       userInfo
      )
      _ <-
        if (hasResourcePermission) F.unit
        else
          log.info(ctx.loggingCtx)(
            s"User ${userInfo} tried to access app ${appName.value} without proper permissions. Returning 404"
          ) >> F
            .raiseError[Unit](AppNotFoundByWorkspaceIdException(workspaceId, appName, ctx.traceId, "permission denied"))
    } yield GetAppResponse.fromDbResult(app, Config.proxyConfig.proxyUrlBase)

  override def createAppV2(userInfo: UserInfo, workspaceId: WorkspaceId, appName: AppName, req: CreateAppRequest)(
    implicit as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask

      // Check the calling user has permission on the workspace
      hasPermission <- authProvider.hasPermission[WorkspaceResourceSamResourceId, WorkspaceAction](
        WorkspaceResourceSamResourceId(workspaceId),
        WorkspaceAction.CreateControlledUserResource,
        userInfo
      )
      _ <- F.raiseUnless(hasPermission)(ForbiddenError(userInfo.userEmail))

      // Validate shared access scope apps against an allow-list. No-op for private apps.
      _ <- req.accessScope match {
        case Some(AppAccessScope.WorkspaceShared) =>
          F.raiseUnless(ConfigReader.appConfig.azure.allowedSharedApps.contains(req.appType))(
            SharedAppNotAllowedException(req.appType, ctx.traceId)
          )
        case _ => F.unit
      }

      // Resolve the workspace in WSM to get the cloud context
      userToken = org.http4s.headers.Authorization(
        org.http4s.Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token)
      )
      workspaceDescOpt <- wsmDao.getWorkspace(workspaceId, userToken)
      workspaceDesc <- F.fromOption(workspaceDescOpt, WorkspaceNotFoundException(workspaceId, ctx.traceId))
      cloudContext <- (workspaceDesc.azureContext, workspaceDesc.gcpContext) match {
        case (Some(azureContext), _) => F.pure[CloudContext](CloudContext.Azure(azureContext))
        case (_, Some(gcpContext))   => F.pure[CloudContext](CloudContext.Gcp(gcpContext))
        case (None, None) => F.raiseError[CloudContext](CloudContextNotFoundException(workspaceId, ctx.traceId))
      }

      // Check if the app already exists
      appOpt <- KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName).transaction
      _ <- appOpt.fold(F.unit)(c =>
        F.raiseError[Unit](AppAlreadyExistsInWorkspaceException(workspaceId, appName, c.app.status, ctx.traceId))
      )

      // Get the Landing Zone Resources for the app for Azure
      leoAuth <- samDAO.getLeoAuthToken
      landingZoneResourcesOpt <- cloudContext.cloudProvider match {
        case CloudProvider.Gcp => F.pure(None)
        case CloudProvider.Azure =>
          for {
            landingZoneResources <- wsmDao.getLandingZoneResources(workspaceDesc.spendProfile, leoAuth)
          } yield Some(landingZoneResources)
      }

      // Get the optional storage container for the workspace
      storageContainer <- wsmDao.getWorkspaceStorageContainer(workspaceId, userToken)

      // Validate the machine config from the request
      // For Azure: we don't support setting a machine type in the request; we use the landing zone configuration instead.
      // For GCP: we support setting optionally a machine type in the request; and use a default value otherwise.
      machineConfig <- (cloudContext.cloudProvider, req.kubernetesRuntimeConfig) match {
        case (CloudProvider.Azure, Some(_)) =>
          F.raiseError(AppMachineConfigNotSupportedException(ctx.traceId))
        // TODO: pull this from the landing zone instead of hardcoding once TOAZ-232 is implemented
        case (CloudProvider.Azure, None) =>
          F.pure(KubernetesRuntimeConfig(NumNodes(1), MachineTypeName("Standard_A2_v2"), false))
        case (CloudProvider.Gcp, Some(mt)) => F.pure(mt)
        case (CloudProvider.Gcp, None) =>
          F.pure(
            KubernetesRuntimeConfig(
              config.leoKubernetesConfig.nodepoolConfig.galaxyNodepoolConfig.numNodes,
              config.leoKubernetesConfig.nodepoolConfig.galaxyNodepoolConfig.machineType,
              config.leoKubernetesConfig.nodepoolConfig.galaxyNodepoolConfig.autoscalingEnabled
            )
          )
      }

      // Create a new Sam resource for the app (either shared or not)
      samResourceId <- F.delay(AppSamResourceId(UUID.randomUUID().toString, req.accessScope))
      // Note: originatingUserEmail is only used for GCP to set up app Sam resources with a parent google project.
      originatingUserEmail <- authProvider.lookupOriginatingUserEmail(userInfo)
      _ <- authProvider
        .notifyResourceCreatedV2(samResourceId, originatingUserEmail, cloudContext, workspaceId, userInfo)
        .handleErrorWith { t =>
          log.error(ctx.loggingCtx, t)(
            s"Failed to notify the AuthProvider for creation of kubernetes app ${cloudContext.asStringWithProvider} / ${appName.value}"
          ) >> F.raiseError[Unit](t)
        }

      // Save or retrieve a KubernetesCluster record for the app
      // debugging
      myRegion = landingZoneResourcesOpt.map(_.region).getOrElse("").toString
      _ <- log.info(ctx.loggingCtx)(s"DEBUGGING: azure region: ${myRegion}")

      saveCluster <- F.fromEither(
        getSavableCluster(userInfo.userEmail, cloudContext, ctx.now, landingZoneResourcesOpt.map(_.clusterName),
        landingZoneResourcesOpt.map(_.region))
      )
      saveClusterResult <- KubernetesServiceDbQueries.saveOrGetClusterForApp(saveCluster).transaction
      _ <-
        if (saveClusterResult.minimalCluster.status == KubernetesClusterStatus.Error)
          F.raiseError[Unit](
            KubernetesAppCreationException(
              s"You cannot create an app while a cluster ${saveClusterResult.minimalCluster.clusterName.value} is in status ${saveClusterResult.minimalCluster.status}",
              Some(ctx.traceId)
            )
          )
        else F.unit

      // Save or retrieve a nodepool record for the app
      userNodepoolOpt <- nodepoolQuery
        .getMinimalByUserAndConfig(originatingUserEmail, cloudContext, machineConfig)
        .transaction
      clusterId = saveClusterResult.minimalCluster.id
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
              getUserNodepool(clusterId, cloudContext, originatingUserEmail, machineConfig, ctx.now)
            )
            savedNodepool <- nodepoolQuery.saveForCluster(saveNodepool).transaction
          } yield savedNodepool
      }

      // Retrieve a pet identity from Sam
      runtimeServiceAccountOpt <- serviceAccountProvider.getClusterServiceAccount(userInfo, cloudContext)
      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done Sam call for getClusterServiceAccount")))
      petSA <- F.fromEither(
        runtimeServiceAccountOpt.toRight(new Exception(s"user ${userInfo.userEmail.value} doesn't have a PET SA"))
      )

      // Process persistent disk in the request, check if the disk was previously attached to any other app
      diskResultOpt <- req.diskConfig.traverse(diskReq =>
        RuntimeServiceInterp.processPersistentDiskRequestForWorkspace(
          diskReq,
          config.leoKubernetesConfig.diskConfig.defaultZone, // this need to be updated if we support non-default zone for k8s apps
          cloudContext,
          workspaceId,
          userInfo,
          petSA,
          appTypeToFormattedByType(req.appType),
          authProvider,
          config.leoKubernetesConfig.diskConfig
        )
      )
      lastUsedApp <- getLastUsedAppForDisk(req, diskResultOpt)

      // Save a new App record in the database
      saveApp <- F.fromEither(
        getSavableApp(
          cloudContext,
          appName,
          userInfo.userEmail,
          samResourceId,
          req,
          diskResultOpt.map(_.disk),
          lastUsedApp,
          petSA,
          nodepool.id,
          Some(workspaceId),
          ctx
        )
      )
      app <- appQuery.save(saveApp, Some(ctx.traceId)).transaction

      // Publish a CreateApp message for Back Leo
      createAppV2Message = CreateAppV2Message(
        app.id,
        app.appName,
        workspaceId,
        cloudContext,
        landingZoneResourcesOpt,
        storageContainer,
        Some(ctx.traceId)
      )
      _ <- publisherQueue.offer(createAppV2Message)
    } yield ()

  override def deleteAppV2(userInfo: UserInfo, workspaceId: WorkspaceId, appName: AppName, deleteDisk: Boolean)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] = for {
    hasWorkspacePermission <- authProvider.isUserWorkspaceReader(WorkspaceResourceSamResourceId(workspaceId), userInfo)
    _ <- F.raiseUnless(hasWorkspacePermission)(ForbiddenError(userInfo.userEmail))

    ctx <- as.ask
    appOpt <- KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName).transaction
    appResult <- F.fromOption(
      appOpt,
      AppNotFoundByWorkspaceIdException(workspaceId, appName, ctx.traceId, "No active app found in DB")
    )
    _ <- deleteAppV2Base(appResult.app, userInfo, workspaceId, deleteDisk)
  } yield ()

  override def deleteAllAppsV2(userInfo: UserInfo, workspaceId: WorkspaceId, deleteDisk: Boolean)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] = for {
    ctx <- as.ask
    hasWorkspacePermission <- authProvider.isUserWorkspaceReader(WorkspaceResourceSamResourceId(workspaceId), userInfo)
    _ <- F.raiseUnless(hasWorkspacePermission)(ForbiddenError(userInfo.userEmail))

    allClusters <- KubernetesServiceDbQueries.listFullAppsByWorkspaceId(Some(workspaceId), Map.empty).transaction
    apps = allClusters
      .flatMap(_.nodepools)
      .flatMap(n => n.apps)

    nonDeletableApps = apps.filterNot(app => AppStatus.deletableStatuses.contains(app.status))

    _ <- F
      .raiseError(DeleteAllAppsCannotBePerformed(workspaceId, nonDeletableApps, ctx.traceId))
      .whenA(!nonDeletableApps.isEmpty)

    _ <- apps
      .traverse { app =>
        deleteAppV2Base(app, userInfo, workspaceId, deleteDisk)
      }
  } yield ()

  private def deleteAppV2Base(app: App, userInfo: UserInfo, workspaceId: WorkspaceId, deleteDisk: Boolean)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] = for {
    ctx <- as.ask
    listOfPermissions <- authProvider.getActions(app.samResourceId, userInfo)

    // throw 404 if no GetAppStatus permission
    hasReadPermission = listOfPermissions.toSet.contains(AppAction.GetAppStatus)
    _ <-
      if (hasReadPermission) F.unit
      else
        F.raiseError[Unit](
          AppNotFoundByWorkspaceIdException(workspaceId, app.appName, ctx.traceId, "no read permission")
        )

    // throw 403 if no DeleteApp permission
    hasDeletePermission = listOfPermissions.toSet.contains(AppAction.DeleteApp)
    _ <-
      if (hasDeletePermission) F.unit
      else F.raiseError[Unit](ForbiddenError(userInfo.userEmail))

    canDelete = AppStatus.deletableStatuses.contains(app.status)
    _ <-
      if (canDelete) F.unit
      else
        F.raiseError[Unit](
          AppCannotBeDeletedByWorkspaceIdException(workspaceId, app.appName, app.status, ctx.traceId)
        )

    // Get the disk to delete if specified
    diskOpt = if (deleteDisk) app.appResources.disk.map(_.id) else None

    // Resolve the workspace in WSM to get the cloud context
    userToken = org.http4s.headers.Authorization(
      org.http4s.Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token)
    )
    workspaceDescOpt <- wsmDao.getWorkspace(workspaceId, userToken)
    workspaceDesc <- F.fromOption(workspaceDescOpt, WorkspaceNotFoundException(workspaceId, ctx.traceId))
    cloudContext <- (workspaceDesc.azureContext, workspaceDesc.gcpContext) match {
      case (Some(azureContext), _) => F.pure[CloudContext](CloudContext.Azure(azureContext))
      case (_, Some(gcpContext))   => F.pure[CloudContext](CloudContext.Gcp(gcpContext))
      case (None, None) => F.raiseError[CloudContext](CloudContextNotFoundException(workspaceId, ctx.traceId))
    }

    // Get the Landing Zone Resources for the app for Azure
    leoAuth <- samDAO.getLeoAuthToken
    landingZoneResourcesOpt <- cloudContext.cloudProvider match {
      case CloudProvider.Gcp => F.pure(None)
      case CloudProvider.Azure =>
        for {
          landingZoneResources <- wsmDao.getLandingZoneResources(workspaceDesc.spendProfile, leoAuth)
        } yield Some(landingZoneResources)
    }

    _ <-
      for {
        _ <- KubernetesServiceDbQueries.markPreDeleting(app.id).transaction
        deleteMessage = DeleteAppV2Message(
          app.id,
          app.appName,
          workspaceId,
          cloudContext,
          diskOpt,
          landingZoneResourcesOpt,
          Some(ctx.traceId)
        )
        _ <- publisherQueue.offer(deleteMessage)
      } yield ()
  } yield ()

  private[service] def getSavableCluster(
    userEmail: WorkbenchEmail,
    cloudContext: CloudContext,
    now: Instant,
    aksClusterName: Option[AKSClusterName],
    azureRegionOpt: Option[Region]
  ): Either[Throwable, SaveKubernetesCluster] = {
    val auditInfo = AuditInfo(userEmail, now, None, now)

    val defaultNodepool = for {
      nodepoolName <- KubernetesNameUtils.getUniqueName(NodepoolName.apply)
    } yield DefaultNodepool(
      NodepoolLeoId(-1),
      clusterId = KubernetesClusterLeoId(-1),
      nodepoolName,
      status =
        if (cloudContext.cloudProvider == CloudProvider.Azure) NodepoolStatus.Running else NodepoolStatus.Precreating,
      auditInfo,
      machineType = config.leoKubernetesConfig.nodepoolConfig.defaultNodepoolConfig.machineType,
      numNodes = config.leoKubernetesConfig.nodepoolConfig.defaultNodepoolConfig.numNodes,
      autoscalingEnabled = config.leoKubernetesConfig.nodepoolConfig.defaultNodepoolConfig.autoscalingEnabled,
      autoscalingConfig = None
    )

    for {
      nodepool <- defaultNodepool
      defaultClusterName <- KubernetesNameUtils.getUniqueName(KubernetesClusterName.apply)
      clusterName = aksClusterName.map(c => KubernetesClusterName(c.value)).getOrElse(defaultClusterName)
    } yield SaveKubernetesCluster(
      cloudContext = cloudContext,
      clusterName = clusterName,
      location = config.leoKubernetesConfig.clusterConfig.location,
      region =
        if (cloudContext.cloudProvider == CloudProvider.Azure) RegionName(azureRegionOpt.getOrElse("").toString)
        else config.leoKubernetesConfig.clusterConfig.region,
      status =
        if (cloudContext.cloudProvider == CloudProvider.Azure) KubernetesClusterStatus.Running
        else KubernetesClusterStatus.Precreating,
      ingressChart = config.leoKubernetesConfig.ingressConfig.chart,
      auditInfo = auditInfo,
      defaultNodepool = nodepool
    )
  }

  /**
   * Short-cuit if app creation isn't allowed, and return whether or not the user's project is AoU project
   *
   * @param userEmail
   * @param googleProject
   * @param descriptorPath
   * @param ev
   * @return
   */
  private[service] def checkIfAppCreationIsAllowed(userEmail: WorkbenchEmail,
                                                   googleProject: GoogleProject,
                                                   descriptorPath: Uri
  )(implicit
    ev: Ask[F, TraceId]
  ): F[Unit] =
    if (config.enableCustomAppCheck)
      for {
        ctx <- ev.ask

        allowedOrError <-
          for {
            projectLabels <- googleResourceService.getLabels(googleProject)
            isAppAllowed <- projectLabels match {
              case Some(labels) =>
                labels.get(SECURITY_GROUP) match {
                  case Some(securityGroupValue) =>
                    val appAllowList =
                      if (securityGroupValue == SECURITY_GROUP_HIGH)
                        customAppConfig.customApplicationAllowList.highSecurity
                      else customAppConfig.customApplicationAllowList.default
                    if (appAllowList.contains(descriptorPath.toString()))
                      authProvider.isCustomAppAllowed(userEmail) map { res =>
                        if (res) Right(())
                        else Left("No labels found for this project. User is not in CUSTOM_APP_USERS group")
                      }
                    else
                      F.pure(s"${descriptorPath.toString()} is not in app allow list.".asLeft[Unit])
                  case None =>
                    authProvider.isCustomAppAllowed(userEmail) map { res =>
                      if (res) Right(())
                      else Left("No security-group found for this project. User is not in CUSTOM_APP_USERS group")
                    }
                }
              case None =>
                authProvider.isCustomAppAllowed(userEmail) map { res =>
                  if (res) Right(())
                  else Left("No labels found for this project. User is not in CUSTOM_APP_USERS group")
                }
            }
          } yield isAppAllowed

        _ <- allowedOrError match {
          case Left(error) =>
            log.info(Map("traceId" -> ctx.asString))(error) >> F.raiseError(ForbiddenError(userEmail, Some(ctx)))
          case Right(_) => F.unit
        }
      } yield ()
    else F.unit

  private def getLastUsedAppForDisk(
    req: CreateAppRequest,
    diskResultOpt: Option[PersistentDiskRequestResult]
  )(implicit as: Ask[F, AppContext]): F[Option[LastUsedApp]] = for {
    ctx <- as.ask
    lastUsedApp <- diskResultOpt match {
      case Some(diskResult) =>
        if (diskResult.creationNeeded) {
          F.pure(none[LastUsedApp])
        } else {
          (diskResult.disk.formattedBy, diskResult.disk.appRestore) match {
            case (Some(FormattedBy.Galaxy), Some(GalaxyRestore(_, _))) |
                (Some(FormattedBy.Cromwell), Some(AppRestore.Other(_))) |
                (Some(FormattedBy.Allowed), Some(AppRestore.Other(_))) =>
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
            case (Some(FormattedBy.Galaxy), None) | (Some(FormattedBy.Cromwell), None) |
                (Some(FormattedBy.Allowed), None) =>
              F.raiseError[Option[LastUsedApp]](
                new LeoException("Existing disk found, but no restore info found in DB", traceId = Some(ctx.traceId))
              )
            case (Some(FormattedBy.Custom), _) =>
              F.raiseError[Option[LastUsedApp]](
                new LeoException(
                  "Custom app disk reattachment is not supported",
                  traceId = Some(ctx.traceId)
                )
              )
            case (None, _) =>
              F.raiseError[Option[LastUsedApp]](
                new LeoException(
                  "Disk is not formatted yet. Only disks previously used by galaxy/cromwell/rstudio app can be re-used to create a new galaxy/cromwell/rstudio app",
                  traceId = Some(ctx.traceId)
                )
              )
            case (_, _) =>
              F.raiseError[Option[LastUsedApp]](
                DiskAlreadyFormattedError(
                  diskResult.disk.formattedBy.get,
                  s"${FormattedBy.Cromwell.asString} or ${FormattedBy.Galaxy.asString} or ${FormattedBy.Allowed.asString}",
                  ctx.traceId
                )
              )
          }
        }
      case None => F.pure(none[LastUsedApp])
    }

  } yield lastUsedApp

  private[service] def convertToDisk(userInfo: UserInfo,
                                     cloudContext: CloudContext,
                                     diskName: DiskName,
                                     config: PersistentDiskConfig,
                                     req: CreateAppRequest,
                                     now: Instant
  ): Either[Throwable, PersistentDisk] = {
    // create a LabelMap of default labels
    val defaultLabelMap: LabelMap =
      Map(
        "diskName" -> diskName.value,
        "cloudContext" -> cloudContext.asString,
        "creator" -> userInfo.userEmail.value
      )

    // combine default and given labels
    val allLabels = req.diskConfig.get.labels ++ defaultLabelMap

    for {
      // check the labels do not contain forbidden keys
      labels <-
        if (allLabels.contains(includeDeletedKey))
          Left(IllegalLabelKeyException(includeDeletedKey))
        else
          Right(allLabels)
    } yield PersistentDisk(
      DiskId(0),
      cloudContext,
      ZoneName("us-west1-a"),
      diskName,
      userInfo.userEmail,
      // TODO: WSM will populate this, we can update in backleo if its needed for anything
      PersistentDiskSamResourceId("fakeUUID"),
      DiskStatus.Creating,
      AuditInfo(userInfo.userEmail, now, None, now),
      req.diskConfig.get.size.getOrElse(config.defaultDiskSizeGb),
      req.diskConfig.get.diskType.getOrElse(config.defaultDiskType),
      config.defaultBlockSizeBytes,
      None,
      None,
      labels,
      None,
      None,
      None
    )
  }

  private[service] def getUserNodepool(clusterId: KubernetesClusterLeoId,
                                       cloudContext: CloudContext,
                                       userEmail: WorkbenchEmail,
                                       machineConfig: KubernetesRuntimeConfig,
                                       now: Instant
  ): Either[Throwable, Nodepool] = {
    val auditInfo = AuditInfo(userEmail, now, None, now)
    for {
      nodepoolName <- KubernetesNameUtils.getUniqueName(NodepoolName.apply)
    } yield Nodepool(
      NodepoolLeoId(-1),
      clusterId = clusterId,
      nodepoolName,
      status =
        if (cloudContext.cloudProvider == CloudProvider.Azure) NodepoolStatus.Running else NodepoolStatus.Precreating,
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

  private[service] def getSavableApp(cloudContext: CloudContext,
                                     appName: AppName,
                                     userEmail: WorkbenchEmail,
                                     samResourceId: AppSamResourceId,
                                     req: CreateAppRequest,
                                     diskOpt: Option[PersistentDisk],
                                     lastUsedApp: Option[LastUsedApp],
                                     googleServiceAccount: WorkbenchEmail,
                                     nodepoolId: NodepoolLeoId,
                                     workspaceId: Option[WorkspaceId],
                                     ctx: AppContext
  ): Either[Throwable, SaveApp] = {
    val now = ctx.now
    val auditInfo = AuditInfo(userEmail, now, None, now)
    val allLabels =
      DefaultKubernetesLabels(
        cloudContext,
        appName,
        userEmail,
        googleServiceAccount
      ).toMap ++ req.labels
    for {
      // Validate app type
      gkeAppConfig <- (req.appType, cloudContext.cloudProvider) match {
        case (Galaxy, CloudProvider.Gcp)              => Right(config.leoKubernetesConfig.galaxyAppConfig)
        case (Custom, CloudProvider.Gcp)              => Right(config.leoKubernetesConfig.customAppConfig)
        case (Cromwell, CloudProvider.Gcp)            => Right(config.leoKubernetesConfig.cromwellAppConfig)
        case (AppType.Allowed, CloudProvider.Gcp)     => Right(config.leoKubernetesConfig.allowedAppConfig)
        case (Cromwell, CloudProvider.Azure)          => Right(ConfigReader.appConfig.azure.coaAppConfig)
        case (WorkflowsApp, CloudProvider.Azure)      => Right(ConfigReader.appConfig.azure.workflowsAppConfig)
        case (CromwellRunnerApp, CloudProvider.Azure) => Right(ConfigReader.appConfig.azure.cromwellRunnerAppConfig)
        case (Wds, CloudProvider.Azure)               => Right(ConfigReader.appConfig.azure.wdsAppConfig)
        case (HailBatch, CloudProvider.Azure)         => Right(ConfigReader.appConfig.azure.hailBatchAppConfig)
        case _ =>
          AppTypeNotSupportedOnCloudException(cloudContext.cloudProvider, req.appType, ctx.traceId)
            .asLeft[KubernetesAppConfig]
      }

      // Check if app type is enabled
      _ <- Either.cond(gkeAppConfig.enabled, (), AppTypeNotEnabledException(req.appType, ctx.traceId))

      // check the labels do not contain forbidden keys
      labels <-
        if (allLabels.contains(includeDeletedKey))
          Left(IllegalLabelKeyException(includeDeletedKey))
        else
          Right(allLabels)

      // Validate disk.
      // Apps on GCP require a disk.
      // Apps on Azure require _no_ disk.
      _ <- (cloudContext.cloudProvider, diskOpt) match {
        case (CloudProvider.Gcp, None) =>
          Left(AppRequiresDiskException(cloudContext, appName, req.appType, ctx.traceId))
        case (CloudProvider.Azure, Some(_)) =>
          Left(AppDiskNotSupportedException(cloudContext, appName, req.appType, ctx.traceId))
        case _ => Right(())
      }

      // adding the relayHybridConnection name to custom env vars
      // necessary for backwards compatibility before workspace was added to name
      customEnvironmentVariables = (cloudContext.cloudProvider, workspaceId) match {
        case (CloudProvider.Azure, Some(workspaceId)) =>
          req.customEnvironmentVariables + ("RELAY_HYBRID_CONNECTION_NAME" -> s"${appName.value}-${workspaceId.value}")
        case _ => req.customEnvironmentVariables
      }

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

      potentialNewChart =
        if (req.appType == AppType.Allowed) {
          val chartVersion = req.allowedChartName.get match {
            case AllowedChartName.RStudio => config.leoKubernetesConfig.allowedAppConfig.rstudioChartVersion
            case AllowedChartName.Sas     => config.leoKubernetesConfig.allowedAppConfig.sasChartVersion
          }
          gkeAppConfig.chart
            .lens(_.name)
            .modify(cn => ChartName(cn.asString + req.allowedChartName.map(_.asString).get))
            .lens(_.version)
            .modify(_ => chartVersion)
        } else gkeAppConfig.chart

      chart = lastUsedApp
        .map { lastUsed =>
          // if there's a patch version bump, then we use the later version defined in leo config; otherwise, we use lastUsed chart definition
          if (
            lastUsed.chart.name == potentialNewChart.name && isPatchVersionDifference(lastUsed.chart.version,
                                                                                      gkeAppConfig.chartVersion
            )
          )
            potentialNewChart
          else lastUsed.chart
        }
        .getOrElse(potentialNewChart)

      release <- lastUsedApp.fold(
        KubernetesName
          .withValidation(
            s"${uid}-${gkeAppConfig.releaseNameSuffix.value}",
            Release.apply
          )
      )(app => app.release.asRight[Throwable])
      services =
        if (cloudContext.cloudProvider == CloudProvider.Azure) {
          gkeAppConfig.kubernetesServices.appended(ConfigReader.appConfig.azure.listenerChartConfig.service)
        } else gkeAppConfig.kubernetesServices
    } yield SaveApp(
      App(
        AppId(-1),
        nodepoolId,
        req.appType,
        appName,
        req.accessScope,
        workspaceId,
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
          diskOpt,
          services,
          Option(gkeAppConfig.serviceAccountName)
        ),
        List.empty,
        customEnvironmentVariables,
        req.descriptorPath,
        req.extraArgs,
        req.sourceWorkspaceId
      )
    )
  }

  private def getVisibleApps(allClusters: List[KubernetesCluster], userInfo: UserInfo)(implicit
    as: Ask[F, AppContext]
  ): F[Option[List[AppSamResourceId]]] = for {
    _ <- as.ask
    samResources = allClusters.flatMap(_.nodepools.flatMap(_.apps.map(_.samResourceId)))
    // Partition apps by shared vs. private access scope and make a separate Sam call for each, then re-combine them.
    // The reason we do this shared vs. private app access scope is represented by different Sam resource types/actions,
    // and therefore needs different decoders to process the result list.
    partition = samResources
      .map(sr => sr.copy(accessScope = sr.accessScope.orElse(Some(AppAccessScope.UserPrivate))))
      .partition(_.accessScope == Some(AppAccessScope.WorkspaceShared))
    samVisibleSharedAppsOpt <- NonEmptyList.fromList(partition._1).traverse { apps =>
      authProvider.filterUserVisible(apps, userInfo)(implicitly, sharedAppSamIdDecoder, implicitly)
    }
    samVisiblePrivateAppsOpt <- NonEmptyList.fromList(partition._2).traverse { apps =>
      authProvider.filterUserVisible(apps, userInfo)(implicitly, appSamIdDecoder, implicitly)
    }
    samVisibleAppsOpt = (samVisiblePrivateAppsOpt, samVisibleSharedAppsOpt) match {
      case (Some(a), Some(b)) => Some(a ++ b)
      case (Some(a), None)    => Some(a)
      case (None, Some(b))    => Some(b)
      case (None, None)       => None
    }
  } yield samVisibleAppsOpt

  private def getVisibleAppsByProject(allClusters: List[KubernetesCluster], userInfo: UserInfo)(implicit
    as: Ask[F, AppContext]
  ): F[Option[List[AppSamResourceId]]] = for {
    ctx <- as.ask
    samResources = allClusters.map(a =>
      // Need the project to filter projects not visible to user
      (GoogleProject(a.cloudContext.asString), a.nodepools.flatMap(_.apps.map(_.samResourceId)))
    )
    apps = samResources.flatMap { case (gp, srList) =>
      srList.map(sr => (gp, sr.copy(accessScope = sr.accessScope.orElse(Some(AppAccessScope.UserPrivate)))))
    }

    // Partition apps by shared vs. private access scope and make a separate Sam call for each, then re-combine them.
    // The reason we do this shared vs. private app access scope is represented by different Sam resource types/actions,
    // and therefore needs different decoders to process the result list.
    partition = apps.partition(_._2.accessScope == Some(AppAccessScope.WorkspaceShared))
    samVisibleSharedAppsOpt <- NonEmptyList.fromList(partition._1).traverse { apps =>
      authProvider
        .filterResourceProjectVisible(apps, userInfo)(implicitly, sharedAppSamIdDecoder, implicitly)
        .map(_.map(_._2))
    }
    samVisiblePrivateAppsOpt <- NonEmptyList.fromList(partition._2).traverse { apps =>
      authProvider
        .filterResourceProjectVisible(apps, userInfo)(implicitly, appSamIdDecoder, implicitly)
        .map(_.map(_._2))
    }
    samVisibleAppsOpt = (samVisiblePrivateAppsOpt, samVisibleSharedAppsOpt) match {
      case (Some(a), Some(b)) => Some(a ++ b)
      case (Some(a), None)    => Some(a)
      case (None, Some(b))    => Some(b)
      case (None, None)       => None
    }
  } yield samVisibleAppsOpt

  private def filterAppsBySamPermission(allClusters: List[KubernetesCluster],
                                        userInfo: UserInfo,
                                        labels: List[String],
                                        useGoogleProject: Boolean
  )(implicit
    as: Ask[F, AppContext]
  ): F[Vector[ListAppResponse]] = for {
    _ <- as.ask

    // V1 endpoints use google project to determine user access, but V2 doesn't
    samVisibleAppsOpt <- useGoogleProject match {
      case true  => getVisibleAppsByProject(allClusters, userInfo)
      case false => getVisibleApps(allClusters, userInfo)
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
                    val sr =
                      a.samResourceId.copy(accessScope = a.appAccessScope.orElse(Some(AppAccessScope.UserPrivate)))
                    samVisibleAppsSet.contains(sr)
                  })
                }
                .filterNot(_.apps.isEmpty)
            )
          }
          .filterNot(_.nodepools.isEmpty)
          .flatMap(c => ListAppResponse.fromCluster(c, Config.proxyConfig.proxyUrlBase, labels))
          .toVector
    }
    // We authenticate actions on resources. If there are no visible clusters,
    // we need to check if user should be able to see the empty list.
    _ <- if (res.isEmpty) authProvider.checkUserEnabled(userInfo) else F.unit
  } yield res

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
                                 customAppConfig: CustomAppConfig,
                                 allowedAppConfig: AllowedAppConfig
  )

  private[http] def isPatchVersionDifference(a: ChartVersion, b: ChartVersion): Boolean = {
    val aSplited = a.asString.split("\\.")
    val bSplited = b.asString.split("\\.")
    aSplited(0) == bSplited(0) && aSplited(1) == bSplited(1)
  }
}
case class AppNotFoundException(cloudContext: CloudContext, appName: AppName, traceId: TraceId, extraMsg: String)
    extends LeoException(
      s"App ${cloudContext.asStringWithProvider}/${appName.value} not found",
      StatusCodes.NotFound,
      null,
      extraMsg,
      traceId = Some(traceId)
    )

case class AppNotFoundByWorkspaceIdException(workspaceId: WorkspaceId,
                                             appName: AppName,
                                             traceId: TraceId,
                                             extraMsg: String
) extends LeoException(
      s"App ${workspaceId.value.toString}/${appName.value} not found",
      StatusCodes.NotFound,
      null,
      extraMsg,
      traceId = Some(traceId)
    )

case class AppAlreadyExistsInWorkspaceException(workspaceId: WorkspaceId,
                                                appName: AppName,
                                                status: AppStatus,
                                                traceId: TraceId
) extends LeoException(
      s"App ${workspaceId.value.toString}/${appName.value} already exists in ${status.toString} status.",
      StatusCodes.Conflict,
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

case class AppCannotBeDeletedByWorkspaceIdException(workspaceId: WorkspaceId,
                                                    appName: AppName,
                                                    status: AppStatus,
                                                    traceId: TraceId
) extends LeoException(
      s"App ${workspaceId.value.toString}/${appName.value} cannot be deleted in ${status} status." +
        (if (status == AppStatus.Stopped) " Please start the app first." else ""),
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )

case class DeleteAllAppsCannotBePerformed(workspaceId: WorkspaceId, apps: List[App], traceId: TraceId)
    extends LeoException(
      s"App(s) in workspace ${workspaceId.value.toString} with (name(s), status(es)) ${apps
          .map(app => s"(${app.appName.value},${app.status})")} cannot be deleted due to their status(es).",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )

case class AppRequiresDiskException(cloudContext: CloudContext, appName: AppName, appType: AppType, traceId: TraceId)
    extends LeoException(
      s"App ${cloudContext.asStringWithProvider}/${appName.value} cannot be created because the request does not contain a valid disk. Apps of type ${appType} require a disk. Trace ID: ${traceId.asString}",
      StatusCodes.BadRequest,
      traceId = Some(traceId)
    )

case class AppDiskNotSupportedException(cloudContext: CloudContext,
                                        appName: AppName,
                                        appType: AppType,
                                        traceId: TraceId
) extends LeoException(
      s"App ${cloudContext.asStringWithProvider}/${appName.value} of type ${appType} does not support persistent disk. Trace ID: ${traceId.toString}",
      StatusCodes.BadRequest,
      traceId = Some(traceId)
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

case class AppMachineConfigNotSupportedException(traceId: TraceId)
    extends LeoException(
      s"Machine configuration not supported for Azure apps. Trace ID ${traceId.asString}",
      StatusCodes.BadRequest,
      traceId = Some(traceId)
    )

case class AppTypeNotSupportedOnCloudException(cloudProvider: CloudProvider, appType: AppType, traceId: TraceId)
    extends LeoException(
      s"Apps of type ${appType.toString} not supported on ${cloudProvider.asString}. Trace ID: ${traceId.asString}",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )

case class SharedAppNotAllowedException(appType: AppType, traceId: TraceId)
    extends LeoException(
      s"App with type ${appType.toString} cannot be launched with shared access scope. Trace ID: ${traceId.asString}",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )

case class AppTypeNotEnabledException(appType: AppType, traceId: TraceId)
    extends LeoException(
      s"App with type ${appType.toString} is not enabled. Trace ID: ${traceId.asString}",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )
