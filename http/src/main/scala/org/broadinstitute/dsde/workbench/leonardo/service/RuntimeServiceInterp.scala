package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import cats.Parallel
import cats.effect.concurrent.Semaphore
import cats.effect.{Async, Blocker, ContextShift, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.auth.oauth2.AccessToken
import com.google.cloud.BaseServiceException
import io.chrisdavenport.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageService}
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{Jupyter, Proxy, Welder}
import org.broadinstitute.dsde.workbench.leonardo.config.{AutoFreezeConfig, DataprocConfig, GceConfig, ImageConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.DockerDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, DbReference, SaveCluster}
import org.broadinstitute.dsde.workbench.leonardo.http.api.{CreateRuntime2Request, RuntimeServiceContext}
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeonardoService._
import org.broadinstitute.dsde.workbench.leonardo.http.service.RuntimeServiceInterp._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.CreateRuntimeMessage
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{google, TraceId, UserInfo, WorkbenchEmail}

import scala.concurrent.ExecutionContext

class RuntimeServiceInterp[F[_]: Parallel](blocker: Blocker,
                                           semaphore: Semaphore[F],
                                           config: RuntimeServiceConfig,
                                           authProvider: LeoAuthProvider[F],
                                           serviceAccountProvider: ServiceAccountProvider[F],
                                           dockerDAO: DockerDAO[F],
                                           googleStorageService: GoogleStorageService[F],
                                           publisherQueue: fs2.concurrent.Queue[F, LeoPubsubMessage])(
  implicit F: Async[F],
  log: StructuredLogger[F],
  timer: Timer[F],
  cs: ContextShift[F],
  dbReference: DbReference[F],
  ec: ExecutionContext
) extends RuntimeService[F] {

  def createRuntime(userInfo: UserInfo,
                    googleProject: GoogleProject,
                    runtimeName: RuntimeName,
                    req: CreateRuntime2Request)(implicit as: ApplicativeAsk[F, RuntimeServiceContext]): F[Unit] =
    for {
      context <- as.ask
      implicit0(traceId: ApplicativeAsk[F, TraceId]) <- F.pure(ApplicativeAsk.const[F, TraceId](context.traceId)) //TODO: is there a better way to do this?
      hasPermission <- authProvider.hasProjectPermission(userInfo, ProjectActions.CreateClusters, googleProject)
      _ <- if (hasPermission) F.unit else F.raiseError[Unit](AuthorizationError(Some(userInfo.userEmail)))
      // Grab the service accounts from serviceAccountProvider for use later
      clusterServiceAccountOpt <- serviceAccountProvider
        .getClusterServiceAccount(userInfo, googleProject)
      notebookServiceAccountOpt <- serviceAccountProvider
        .getNotebookServiceAccount(userInfo, googleProject)
      serviceAccountInfo = ServiceAccountInfo(clusterServiceAccountOpt, notebookServiceAccountOpt)

      clusterOpt <- clusterQuery.getActiveClusterByNameMinimal(googleProject, runtimeName).transaction

      _ <- clusterOpt match {
        case Some(c) => F.raiseError[Unit](RuntimeAlreadyExistsException(googleProject, runtimeName, c.status))
        case None =>
          for {
            internalId <- F.delay(RuntimeInternalId(UUID.randomUUID().toString))
            petToken <- serviceAccountProvider.getAccessToken(userInfo.userEmail, googleProject).recoverWith {
              case e =>
                log.warn(e)(
                  s"Could not acquire pet service account access token for user ${userInfo.userEmail.value} in project $googleProject. " +
                    s"Skipping validation of bucket objects in the cluster request."
                ) as None
            }
            clusterImages <- getRuntimeImages(petToken,
                                              userInfo.userEmail,
                                              googleProject,
                                              context.now,
                                              req.toolDockerImage,
                                              req.welderDockerImage)
            cluster <- F.fromEither(
              convertToRuntime(userInfo,
                               serviceAccountInfo,
                               googleProject,
                               runtimeName,
                               internalId,
                               clusterImages,
                               config,
                               req)
            )
            gcsObjectUrisToValidate = cluster.userJupyterExtensionConfig
              .map(
                config =>
                  (config.nbExtensions.values ++ config.serverExtensions.values ++ config.combinedExtensions.values)
                    .filter(_.startsWith("gs://"))
                    .toList
              )
              .getOrElse(List.empty) ++ req.jupyterUserScriptUri.map(_.asString) ++ req.jupyterStartUserScriptUri.map(
              _.asString
            )
            _ <- petToken.traverse(
              t =>
                gcsObjectUrisToValidate
                  .parTraverse(s => validateBucketObjectUri(userInfo.userEmail, t, s, context.traceId))
            )
            _ <- authProvider
              .notifyClusterCreated(internalId, userInfo.userEmail, googleProject, runtimeName)
              .handleErrorWith { t =>
                log.error(t)(
                  s"[$traceId] Failed to notify the AuthProvider for creation of cluster ${cluster.projectNameString}"
                ) >> F.raiseError(t)
              }

            gceRuntimeDefaults = config.gceConfig.runtimeConfigDefaults
            runtimeCofig = req.runtimeConfig
              .fold[RuntimeConfig](gceRuntimeDefaults) { // default to gce if no runtime specific config is provided
                c =>
                  c match {
                    case gce: RuntimeConfigRequest.GceConfig =>
                      RuntimeConfig.GceConfig(
                        gce.machineType.getOrElse(gceRuntimeDefaults.machineType),
                        gce.diskSize.getOrElse(gceRuntimeDefaults.diskSize)
                      ): RuntimeConfig
                    case dataproc: RuntimeConfigRequest.DataprocConfig =>
                      dataproc.toRuntimeConfigDataprocConfig(config.dataprocConfig.runtimeConfigDefaults): RuntimeConfig
                  }
              }

            saveCluster = SaveCluster(cluster = cluster, runtimeConfig = runtimeCofig, now = context.now)
            runtime <- clusterQuery.save(saveCluster).transaction
            _ <- publisherQueue.enqueue1(CreateRuntimeMessage.fromRuntime(runtime, runtimeCofig, Some(context.traceId)))
          } yield ()
      }
    } yield ()

  private[service] def getRuntimeImages(
    petToken: Option[String],
    userEmail: WorkbenchEmail,
    googleProject: GoogleProject,
    now: Instant,
    toolDockerImage: Option[ContainerImage],
    welderDockerImage: Option[ContainerImage]
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[Set[RuntimeImage]] =
    for {
      traceId <- ev.ask
      // Try to autodetect the image
      autodetectedImageOpt <- toolDockerImage.traverse { image =>
        dockerDAO.detectTool(image, petToken).flatMap {
          case None       => F.raiseError[RuntimeImage](ImageNotFoundException(traceId, image))
          case Some(tool) => F.pure(RuntimeImage(tool, image.imageUrl, now))
        }
      }
      // Figure out the tool image. Rules:
      // - if we were able to autodetect an image, use that
      // - else use the default jupyter image
      defaultJupyterImage = RuntimeImage(Jupyter, config.imageConfig.jupyterImage.imageUrl, now)
      toolImage = autodetectedImageOpt getOrElse defaultJupyterImage
      // Figure out the welder image. Rules:
      // - If welder is enabled, we will use the client-supplied image if present, otherwise we will use a default.
      // - If welder is not enabled, we won't use any image.
      welderImage = RuntimeImage(
        Welder,
        welderDockerImage.map(_.imageUrl).getOrElse(config.imageConfig.welderImage.imageUrl),
        now
      )
      // Get the proxy image
      proxyImage = RuntimeImage(Proxy, config.imageConfig.proxyImage.imageUrl, now)
    } yield Set(toolImage, welderImage, proxyImage)

  private[service] def validateBucketObjectUri(userEmail: WorkbenchEmail,
                                               userToken: String,
                                               gcsUri: String,
                                               traceId: TraceId): F[Unit] = {
    val gcsUriOpt = google.parseGcsPath(gcsUri)
    gcsUriOpt match {
      case Left(_)                                                      => F.raiseError(BucketObjectException(gcsUri))
      case Right(gcsPath) if gcsPath.toUri.length > bucketPathMaxLength => F.raiseError(BucketObjectException(gcsUri))
      case Right(gcsPath)                                               =>
        // Retry 401s from Google here because they can be thrown spuriously with valid credentials.
        // See https://github.com/DataBiosphere/leonardo/issues/460
        // Note GoogleStorageDAO already retries 500 and other errors internally, so we just need to catch 401s here.
        // We might think about moving the retry-on-401 logic inside GoogleStorageDAO.
        val accessToken = new AccessToken(userToken, null) // we currently don't get expiration time from sam
        val retryPolicy = RetryPredicates.retryConfigWithPredicates(
          RetryPredicates.standardRetryPredicate,
          RetryPredicates.whenStatusCode(401)
        )

        val res = for {
          blob <- GoogleStorageService.fromAccessToken(accessToken, blocker, Some(semaphore)).use { gcs =>
            gcs
              .getBlob(gcsPath.bucketName, GcsBlobName(gcsPath.objectName.value), Some(traceId), retryPolicy)
              .compile
              .last
          }
          _ <- if (blob.isDefined) F.unit else F.raiseError[Unit](BucketObjectException(gcsUri))
        } yield ()

        res.recoverWith {
          case e: BaseServiceException if e.getCode == StatusCodes.Forbidden.intValue =>
            log.error(e)(
              s"User ${userEmail.value} does not have access to ${gcsPath.bucketName} / ${gcsPath.objectName}"
            ) >> F.raiseError(BucketObjectAccessException(userEmail, gcsPath))
          case e: BaseServiceException if e.getCode == 401 =>
            log.warn(e)(s"Could not validate object [${gcsUri}] as user [${userEmail.value}]")
        }
    }
  }
}

object RuntimeServiceInterp {
  private def convertToRuntime(userInfo: UserInfo,
                               serviceAccountInfo: ServiceAccountInfo,
                               googleProject: GoogleProject,
                               runtimeName: RuntimeName,
                               clusterInternalId: RuntimeInternalId,
                               clusterImages: Set[RuntimeImage],
                               config: RuntimeServiceConfig,
                               req: CreateRuntime2Request): Either[Throwable, Runtime] = {
    // create a LabelMap of default labels
    val defaultLabels = DefaultLabels(
      runtimeName,
      googleProject,
      userInfo.userEmail,
      serviceAccountInfo.clusterServiceAccount,
      serviceAccountInfo.notebookServiceAccount,
      req.jupyterUserScriptUri,
      req.jupyterStartUserScriptUri,
      clusterImages.map(_.imageType).filterNot(_ == Welder).headOption
    ).toMap

    val userJupyterExt = req.jupyterExtensionUri match {
      case Some(ext) => Map("notebookExtension" -> ext.toUri)
      case None      => Map.empty[String, String]
    }

    // add the userJupyterExt to the nbExtensions
    val updatedUserJupyterExtensionConfig = req.userJupyterExtensionConfig match {
      case Some(config) => config.copy(nbExtensions = config.nbExtensions ++ userJupyterExt)
      case None         => UserJupyterExtensionConfig(userJupyterExt, Map.empty, Map.empty, Map.empty)
    }

    // combine default and given labels and add labels for extensions
    val allLabels = req.labels ++ defaultLabels ++ updatedUserJupyterExtensionConfig.asLabels

    val autopauseThreshold = calculateAutopauseThreshold(
      req.autopause,
      req.autopauseThreshold.map(_.toMinutes.toInt),
      config.autoFreezeConfig
    ) //TODO: use FiniteDuration for autopauseThreshold field in Cluster
    val clusterScopes = if (req.scopes.isEmpty) config.dataprocConfig.defaultScopes else req.scopes //TODO: Rob will update this to GCE specific scopes appropriately
    for {
      // check the labels do not contain forbidden keys
      labels <- if (allLabels.contains(includeDeletedKey))
        Left(IllegalLabelKeyException(includeDeletedKey))
      else
        Right(allLabels)
    } yield Runtime(
      0,
      internalId = clusterInternalId,
      runtimeName = runtimeName,
      googleProject = googleProject,
      serviceAccountInfo = serviceAccountInfo,
      asyncRuntimeFields = None,
      auditInfo = AuditInfo(userInfo.userEmail, Instant.now(), None, Instant.now(), None),
      proxyUrl = Runtime.getProxyUrl(config.proxyUrlBase, googleProject, runtimeName, clusterImages, labels),
      status = RuntimeStatus.Creating,
      labels = labels,
      jupyterExtensionUri = req.jupyterExtensionUri,
      jupyterUserScriptUri = req.jupyterUserScriptUri,
      jupyterStartUserScriptUri = req.jupyterStartUserScriptUri,
      errors = List.empty,
      dataprocInstances = Set.empty,
      userJupyterExtensionConfig =
        if (updatedUserJupyterExtensionConfig.asLabels.isEmpty) None else Some(updatedUserJupyterExtensionConfig),
      autopauseThreshold = autopauseThreshold,
      defaultClientId = req.defaultClientId,
      runtimeImages = clusterImages,
      scopes = clusterScopes,
      welderEnabled = true,
      customEnvironmentVariables = req.customEnvironmentVariables,
      allowStop = false, //TODO: double check this should be false when cluster is created
      runtimeConfigId = RuntimeConfigId(-1),
      stopAfterCreation = false
    )
  }
}

final case class RuntimeServiceConfig(proxyUrlBase: String,
                                      imageConfig: ImageConfig,
                                      autoFreezeConfig: AutoFreezeConfig,
                                      dataprocConfig: DataprocConfig,
                                      gceConfig: GceConfig)
