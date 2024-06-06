package org.broadinstitute.dsde.workbench.leonardo
package monitor

import _root_.org.typelevel.log4cats.StructuredLogger
import cats.effect.Async
import cats.effect.implicits._
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import com.google.api.gax.longrunning.OperationFuture
import com.google.cloud.compute.v1.{Disk, Operation}
import fs2.Stream
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.NamespaceName
import org.broadinstitute.dsde.workbench.google2.{
  isSuccess,
  streamUntilDoneOrTimeout,
  DiskName,
  GoogleDiskService,
  GoogleStorageService,
  MachineTypeName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.AppType.appTypeToFormattedByType
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.{Task, TaskMetricsTags}
import org.broadinstitute.dsde.workbench.leonardo.config.{AllowedAppConfig, KubernetesAppConfig}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.http.service.{
  AppNotFoundException,
  AppTypeNotSupportedOnCloudException
}
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, LeoException}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError._
import org.broadinstitute.dsde.workbench.leonardo.util.GKEAlgebra.{
  getGalaxyPostgresDiskName,
  getOldStyleGalaxyPostgresDiskName
}
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.google.{GcsObjectName, GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{ErrorReport, TraceId, WorkbenchException}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.util2.messaging.{CloudSubscriber, ReceivedMessage}
import org.broadinstitute.dsp.{ChartVersion, HelmException}
import org.broadinstitute.dsde.workbench.leonardo.db.LabelResourceType
import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/**
 * @param config
 * @param subscriber
 * @param asyncTasks
 * @param authProvider
 * @param azurePubsubHandler
 * @param operationFutureCache This is used to cancel long running java Futures for Google operations. Currently, we only cancel existing stopping runtime operation if a `deleteRuntime`
 *                             message is received
 * @tparam F
 */
class LeoPubsubMessageSubscriber[F[_]](
  config: LeoPubsubMessageSubscriberConfig,
  subscriber: CloudSubscriber[F, LeoPubsubMessage],
  asyncTasks: Queue[F, Task[F]],
  authProvider: LeoAuthProvider[F],
  azurePubsubHandler: AzurePubsubHandlerAlgebra[F],
  operationFutureCache: scalacache.Cache[F, Long, OperationFuture[Operation, Operation]],
  cloudSpecificDependenciesRegistry: ServicesRegistry
)(implicit
  executionContext: ExecutionContext,
  F: Async[F],
  logger: StructuredLogger[F],
  dbRef: DbReference[F],
  metrics: OpenTelemetryMetrics[F]
) {

  private[monitor] def messageResponder(
    message: LeoPubsubMessage
  )(implicit traceId: Ask[F, AppContext]): F[Unit] =
    for {
      resp <- message match {
        case msg: CreateRuntimeMessage =>
          handleCreateRuntimeMessage(msg)(traceId, getMonitorFromRegistry(), getRuntimeInstancesFromRegistry())
        case msg: DeleteRuntimeMessage =>
          handleDeleteRuntimeMessage(msg)(traceId, getRuntimeInstancesFromRegistry(), getMonitorFromRegistry())
        case msg: StopRuntimeMessage =>
          handleStopRuntimeMessage(msg)(traceId, getRuntimeInstancesFromRegistry(), getMonitorFromRegistry())
        case msg: StartRuntimeMessage =>
          handleStartRuntimeMessage(msg)(traceId, getRuntimeInstancesFromRegistry(), getMonitorFromRegistry())
        case msg: UpdateRuntimeMessage =>
          handleUpdateRuntimeMessage(msg)(traceId, getRuntimeInstancesFromRegistry(), getMonitorFromRegistry())
        case msg: CreateDiskMessage =>
          handleCreateDiskMessage(msg)
        case msg: DeleteDiskMessage =>
          handleDeleteDiskMessage(msg)
        case msg: UpdateDiskMessage =>
          handleUpdateDiskMessage(msg)
        case msg: CreateAppMessage =>
          handleCreateAppMessage(msg)
        case msg: DeleteAppMessage =>
          handleDeleteAppMessage(msg)
        case msg: StopAppMessage =>
          handleStopAppMessage(msg)
        case msg: StartAppMessage =>
          handleStartAppMessage(msg)
        case msg: UpdateAppMessage =>
          handleUpdateAppMessage(msg)
        case msg: CreateAzureRuntimeMessage =>
          azurePubsubHandler.createAndPollRuntime(msg).adaptError { case e =>
            PubsubHandleMessageError.AzureRuntimeCreationError(
              msg.runtimeId,
              msg.workspaceId,
              e.getMessage,
              msg.useExistingDisk
            )
          }
        case msg: DeleteAzureRuntimeMessage =>
          azurePubsubHandler.deleteAndPollRuntime(msg).adaptError { case e =>
            PubsubHandleMessageError.AzureRuntimeDeletionError(
              msg.runtimeId,
              msg.workspaceId,
              e.getMessage
            )
          }
        case msg: CreateAppV2Message  => handleCreateAppV2Message(msg)
        case msg: DeleteAppV2Message  => handleDeleteAppV2Message(msg)
        case msg: DeleteDiskV2Message => handleDeleteDiskV2Message(msg)
      }
    } yield resp

  private[monitor] def messageHandler(event: ReceivedMessage[LeoPubsubMessage]): F[Unit] = {
    val traceId = event.traceId.getOrElse(TraceId("None"))
    val now = event.publishedTime
    implicit val ev = Ask.const[F, AppContext](AppContext(traceId, now, span = None))
    childSpan(event.msg.messageType.asString).use { implicit ev =>
      messageHandlerWithContext(event)
    }

  }
  private[monitor] def messageHandlerWithContext(
    event: ReceivedMessage[LeoPubsubMessage]
  )(implicit ev: Ask[F, AppContext]): F[Unit] = {
    val res = for {
      res <- messageResponder(event.msg)
        .timeout(config.timeout)
        .attempt // set timeout to 55 seconds because subscriber's ack deadline is 1 minute

      ctx <- ev.ask

      _ <- logger.debug(ctx.loggingCtx)(s"using timeout ${config.timeout} in messageHandler")

      _ <- res match {
        case Left(e)  => processMessageFailure(ctx, event, e)
        case Right(_) => ack(event)
      }
    } yield ()

    res.handleErrorWith(e =>
      logger.error(e)("Fail to process pubsub message for some reason") >> F.delay(event.ackHandler.ack())
    )
  }

  val process: Stream[F, Unit] = subscriber.messages
    .parEvalMapUnordered(config.concurrency)(messageHandler)
    .handleErrorWith(error => Stream.eval(logger.error(error)("Failed to initialize message processor")))

  private def ack(event: ReceivedMessage[LeoPubsubMessage]): F[Unit] =
    for {
      _ <- logger.info(s"acking message: ${event}")
      _ <- F.delay(
        event.ackHandler.ack()
      )
      _ <- recordMessageMetric(event)
    } yield ()

  private[monitor] def processMessageFailure(ctx: AppContext, event: ReceivedMessage[LeoPubsubMessage], e: Throwable)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit] = {
    val handleErrorMessages = e match {
      case ee: PubsubHandleMessageError =>
        for {
          _ <- ee match {
            case ee: PubsubKubernetesError =>
              logger.error(ctx.loggingCtx, e)(
                s"Encountered an error for app ${ee.appId}, ${ee.getMessage}"
              ) >> handleKubernetesError(ee)
            case ee: AzureRuntimeCreationError =>
              azurePubsubHandler.handleAzureRuntimeCreationError(ee, ctx.now)
            case ee: AzureRuntimeDeletionError =>
              azurePubsubHandler.handleAzureRuntimeDeletionError(ee)
            case _ => logger.error(ctx.loggingCtx, ee)(s"Failed to process pubsub message.")
          }
          _ <-
            if (ee.isRetryable)
              logger.error(ctx.loggingCtx, e)("Fail to process retryable pubsub message") >> F.delay(
                event.ackHandler.nack()
              )
            else
              logger.error(ctx.loggingCtx, e)("Fail to process non-retryable pubsub message") >> ack(event)
        } yield ()
      case ee: WorkbenchException if ee.getMessage.contains("Call to Google API failed") =>
        logger.error(ctx.loggingCtx, e)(
          "Fail to process retryable pubsub message due to Google API call failure"
        ) >> F.delay(event.ackHandler.nack())
      case _ =>
        logger.error(ctx.loggingCtx, e)("Fail to process pubsub message due to unexpected error") >> ack(event)
    }
    recordMessageMetric(event, Some(e)) >> handleErrorMessages
  }

  private[monitor] def recordMessageMetric(event: ReceivedMessage[LeoPubsubMessage],
                                           e: Option[Throwable] = None
  ): F[Unit] =
    for {
      end <- F.realTimeInstant
      duration = (end.toEpochMilli - event.publishedTime.toEpochMilli).millis
      distributionBucket = List(0.5 minutes,
                                1 minutes,
                                1.5 minutes,
                                2 minutes,
                                2.5 minutes,
                                3 minutes,
                                3.5 minutes,
                                4 minutes,
                                4.5 minutes
      )
      messageType = event.msg.messageType.asString
      (metricsName, tags) = e match {
        case Some(e) =>
          (s"pubsub/fail", Map("messageType" -> messageType, "exception" -> e.getClass.toString.split('.').last))
        case None => (s"pubsub/success/${messageType}", Map("messageType" -> messageType))
      }
      _ <- metrics.recordDuration(metricsName, duration, distributionBucket, tags)
    } yield ()

  private[monitor] def handleCreateRuntimeMessage(msg: CreateRuntimeMessage)(implicit
    ev: Ask[F, AppContext],
    monitor: RuntimeMonitor[F, CloudService],
    runtimeInstances: RuntimeInstances[F]
  ): F[Unit] = {
    val createCluster = for {
      ctx <- ev.ask

      clusterResult <- msg.runtimeConfig.cloudService.interpreter
        .createRuntime(CreateRuntimeParams.fromCreateRuntimeMessage(msg))

      _ <- clusterResult.traverse { createRuntimeResponse =>
        val updateAsyncClusterCreationFields =
          UpdateAsyncClusterCreationFields(
            Some(GcsPath(createRuntimeResponse.initBucket, GcsObjectName(""))),
            msg.runtimeId,
            Some(createRuntimeResponse.asyncRuntimeFields),
            ctx.now
          )

        // Save the VM image and async fields in the database
        val clusterImage =
          RuntimeImage(RuntimeImageType.BootSource, createRuntimeResponse.bootSource.asString, None, ctx.now)
        (clusterQuery.updateAsyncClusterCreationFields(updateAsyncClusterCreationFields) >> clusterImageQuery.save(
          msg.runtimeId,
          clusterImage
        )).transaction
      }
      taskToRun = for {
        _ <- msg.runtimeConfig.cloudService
          .process(msg.runtimeId, RuntimeStatus.Creating, msg.checkToolsInterruptAfter.map(x => x.minutes))
          .compile
          .drain
      } yield ()
      labels <- labelQuery.getAllForResource(msg.runtimeId, LabelResourceType.runtime).transaction
      isAoU = labels.get(AOU_UI_LABEL).contains("true")
      _ <- asyncTasks.offer(
        Task(
          ctx.traceId,
          taskToRun,
          Some(createRuntimeErrorHandler(msg.runtimeId, msg.runtimeConfig.cloudService, ctx.now)),
          ctx.now,
          TaskMetricsTags("createRuntime", None, Some(isAoU), CloudProvider.Gcp, Some(msg.runtimeConfig.cloudService))
        )
      )
    } yield ()

    createCluster.handleErrorWith(e =>
      ev.ask.flatMap(ctx => createRuntimeErrorHandler(msg.runtimeId, msg.runtimeConfig.cloudService, ctx.now)(e))
    )
  }

  private[monitor] def handleDeleteRuntimeMessage(msg: DeleteRuntimeMessage)(implicit
    ev: Ask[F, AppContext],
    runtimeInstances: RuntimeInstances[F],
    monitor: RuntimeMonitor[F, CloudService]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      existingOperationFuture <- operationFutureCache.get(msg.runtimeId)
      _ <- existingOperationFuture.traverse(opFuture => F.delay(opFuture.cancel(true)))
      runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
      runtime <- runtimeOpt.fold(
        F.raiseError[Runtime](PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      )(F.pure)
      _ <-
        if (!Set(RuntimeStatus.Deleting, RuntimeStatus.PreDeleting).contains(runtime.status))
          F.raiseError[Unit](
            PubsubHandleMessageError.ClusterInvalidState(msg.runtimeId, runtime.projectNameString, runtime, msg)
          )
        else F.unit
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      masterInstance <- runtimeConfig.cloudService match {
        case CloudService.Dataproc =>
          instanceQuery
            .getMasterForCluster(runtime.id)
            .transaction
            .map(_.some)
        case _ => F.pure(none[DataprocInstance])
      }
      op <- runtimeConfig.cloudService.interpreter.deleteRuntime(
        DeleteRuntimeParams(RuntimeAndRuntimeConfig(runtime, runtimeConfig), masterInstance)
      )
      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(runtime.cloudContext),
        AzureUnimplementedException("Azure runtime is not supported yet")
      )
      poll = op match {
        case Some(opFuture) =>
          val monitorContext = MonitorContext(ctx.now, runtime.id, ctx.traceId, RuntimeStatus.Deleting)
          for {
            _ <- F.blocking(opFuture.get())
            _ <- runtimeConfig.cloudService
              .handlePollCheckCompletion(monitorContext, RuntimeAndRuntimeConfig(runtime, runtimeConfig))
          } yield ()
        case None =>
          runtimeConfig.cloudService.process(runtime.id, RuntimeStatus.Deleting, None).compile.drain
      }
      fa = msg.persistentDiskToDelete.fold(poll) { id =>
        val deleteDisk = for {
          _ <- poll
          now <- nowInstant
          diskOpt <- persistentDiskQuery.getPersistentDiskRecord(id).transaction
          disk <- F.fromEither(diskOpt.toRight(new RuntimeException(s"disk not found for ${id}")))
          deleteDiskOp <- getGoogleDiskServiceFromRegistry().deleteDisk(googleProject, disk.zone, disk.name)
          _ <- deleteDiskOp.traverse(x => F.blocking(x.get()))
          _ <- persistentDiskQuery.delete(id, now).transaction.void >> authProvider
            .notifyResourceDeleted(
              disk.samResource,
              disk.creator,
              googleProject
            )
        } yield ()

        deleteDisk.handleErrorWith(e =>
          clusterErrorQuery
            .save(runtime.id, RuntimeError(e.getMessage, None, ctx.now))
            .transaction
            .void
        )
      }
      labels <- labelQuery.getAllForResource(msg.runtimeId, LabelResourceType.runtime).transaction
      isAoU = labels.get(AOU_UI_LABEL).contains("true")
      _ <- asyncTasks.offer(
        Task(
          ctx.traceId,
          fa,
          Some(handleRuntimeMessageError(runtime.id, ctx.now, s"deleting runtime ${runtime.projectNameString} failed")),
          ctx.now,
          TaskMetricsTags("deleteRuntime", None, Some(isAoU), CloudProvider.Gcp, Some(runtimeConfig.cloudService))
        )
      )
    } yield ()

  private[monitor] def handleStopRuntimeMessage(msg: StopRuntimeMessage)(implicit
    ev: Ask[F, AppContext],
    runtimeInstances: RuntimeInstances[F],
    monitor: RuntimeMonitor[F, CloudService]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      now <- F.realTimeInstant
      _ <- logger.info(
        s"StopRuntimeMessage timing: About to get the cluster by id, [runtimeId = ${msg.runtimeId}, traceId = ${ctx.traceId.asString},time = ${(now.toEpochMilli - ctx.now.toEpochMilli).toString}]"
      )
      runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
      runtime <- runtimeOpt.fold(
        F.raiseError[Runtime](PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      )(F.pure)
      now <- F.realTimeInstant
      _ <- logger.info(
        s"StopRuntimeMessage timing: Got the cluster, [runtime = ${runtime.runtimeName.asString}, traceId = ${ctx.traceId.asString},time = ${(now.toEpochMilli - ctx.now.toEpochMilli).toString}]"
      )

      _ <-
        if (!Set(RuntimeStatus.Stopping, RuntimeStatus.PreStopping).contains(runtime.status))
          F.raiseError[Unit](
            PubsubHandleMessageError.ClusterInvalidState(msg.runtimeId, runtime.projectNameString, runtime, msg)
          )
        else F.unit
      now <- F.realTimeInstant
      _ <- logger.info(
        s"StopRuntimeMessage timing: About to get the runtimeConfig, [runtime = ${runtime.runtimeName.asString}, traceId = ${ctx.traceId.asString},time = ${(now.toEpochMilli - ctx.now.toEpochMilli).toString}]"
      )
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      now <- F.realTimeInstant
      _ <- logger.info(
        s"StopRuntimeMessage timing: Got the runtimeConfig, [runtime = ${runtime.runtimeName.asString}, traceId = ${ctx.traceId.asString},time = ${(now.toEpochMilli - ctx.now.toEpochMilli).toString}]"
      )
      _ <- runtime.cloudContext match {
        case CloudContext.Gcp(_) =>
          for {
            op <- runtimeConfig.cloudService.interpreter.stopRuntime(
              StopRuntimeParams(RuntimeAndRuntimeConfig(runtime, runtimeConfig), ctx.now, isDataprocFullStop = true)
            )
            poll = op match {
              case Some(o) =>
                val monitorContext = MonitorContext(ctx.now, runtime.id, ctx.traceId, RuntimeStatus.Stopping)
                for {
                  operation <- F.blocking(o.get())
                  _ <- operationFutureCache.put(runtime.id)(o, None)
                  _ <- F.whenA(isSuccess(operation.getHttpErrorStatusCode))(
                    runtimeConfig.cloudService
                      .handlePollCheckCompletion(monitorContext, RuntimeAndRuntimeConfig(runtime, runtimeConfig))
                  )
                } yield ()
              case None =>
                runtimeConfig.cloudService.process(runtime.id, RuntimeStatus.Stopping, None).compile.drain
            }
            now <- F.realTimeInstant
            _ <- logger.info(
              s"StopRuntimeMessage timing: Polling the stopRuntime, [runtime = ${runtime.runtimeName}, traceId = ${ctx.traceId.asString}, time = ${(now.toEpochMilli - ctx.now.toEpochMilli).toString}]"
            )
            isAoU = runtime.labels.get(AOU_UI_LABEL).contains("true")
            _ <- asyncTasks.offer(
              Task(
                ctx.traceId,
                poll,
                Some(
                  handleRuntimeMessageError(
                    msg.runtimeId,
                    ctx.now,
                    s"stopping runtime ${runtime.projectNameString}/${runtime.runtimeName.toString} failed"
                  )
                ),
                ctx.now,
                TaskMetricsTags("stopRuntime", None, Some(isAoU), CloudProvider.Gcp, Some(runtimeConfig.cloudService))
              )
            )
          } yield ()
        case CloudContext.Azure(azureContext) =>
          azurePubsubHandler
            .stopAndMonitorRuntime(runtime, azureContext)
            .handleErrorWith(e =>
              azurePubsubHandler.handleAzureRuntimeStopError(
                AzureRuntimeStoppingError(
                  runtime.id,
                  s"stopping runtime ${runtime.projectNameString} failed. Cause: ${e.getMessage}",
                  ctx.traceId
                ),
                ctx.now
              )
            )
      }
    } yield ()

  private[monitor] def handleStartRuntimeMessage(msg: StartRuntimeMessage)(implicit
    ev: Ask[F, AppContext],
    runtimeInstances: RuntimeInstances[F],
    monitor: RuntimeMonitor[F, CloudService]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
      runtime <- runtimeOpt.fold(
        F.raiseError[Runtime](PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      )(F.pure)
      _ <-
        if (!Set(RuntimeStatus.Starting, RuntimeStatus.PreStarting).contains(runtime.status))
          F.raiseError[Unit](
            PubsubHandleMessageError.ClusterInvalidState(msg.runtimeId, runtime.projectNameString, runtime, msg)
          )
        else F.unit
      _ <- runtime.cloudContext match {
        case CloudContext.Gcp(_) =>
          for {
            runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
            initBucket <- clusterQuery.getInitBucket(msg.runtimeId).transaction
            bucketName <- F.fromOption(
              initBucket.map(_.bucketName),
              new RuntimeException(s"init bucket not found for ${runtime.projectNameString} in DB")
            )
            _ <- runtimeConfig.cloudService.interpreter
              .startRuntime(StartRuntimeParams(RuntimeAndRuntimeConfig(runtime, runtimeConfig), bucketName))
            isAoU = runtime.labels.get(AOU_UI_LABEL).contains("true")
            _ <- asyncTasks.offer(
              Task(
                ctx.traceId,
                runtimeConfig.cloudService.process(msg.runtimeId, RuntimeStatus.Starting, None).compile.drain,
                Some(
                  handleRuntimeMessageError(msg.runtimeId,
                                            ctx.now,
                                            s"starting runtime ${runtime.projectNameString} failed"
                  )
                ),
                ctx.now,
                TaskMetricsTags("startRuntime", None, Some(isAoU), CloudProvider.Gcp, Some(runtimeConfig.cloudService))
              )
            )
          } yield ()
        case CloudContext.Azure(azureContext) =>
          azurePubsubHandler
            .startAndMonitorRuntime(runtime, azureContext)
            .handleErrorWith(e =>
              azurePubsubHandler.handleAzureRuntimeStartError(
                AzureRuntimeStartingError(
                  runtime.id,
                  s"starting runtime ${runtime.projectNameString} failed. Cause: ${e.getMessage}",
                  ctx.traceId
                ),
                ctx.now
              )
            )
      }

    } yield ()

  private[monitor] def handleUpdateRuntimeMessage(msg: UpdateRuntimeMessage)(implicit
    ev: Ask[F, AppContext],
    runtimeInstances: RuntimeInstances[F],
    monitor: RuntimeMonitor[F, CloudService]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
      runtime <- runtimeOpt.fold(
        F.raiseError[Runtime](PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      )(F.pure)
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction

      // We assume all validation has already happened in RuntimeServiceInterp

      // Resize the cluster
      hasResizedCluster <-
        if (msg.newNumWorkers.isDefined || msg.newNumPreemptibles.isDefined) {
          for {
            _ <- runtimeConfig.cloudService.interpreter
              .resizeCluster(
                ResizeClusterParams(RuntimeAndRuntimeConfig(runtime, runtimeConfig),
                                    msg.newNumWorkers,
                                    msg.newNumPreemptibles
                )
              )
            _ <- msg.newNumWorkers.traverse_(a =>
              RuntimeConfigQueries.updateNumberOfWorkers(runtime.runtimeConfigId, a, ctx.now).transaction
            )
            _ <- msg.newNumPreemptibles.traverse_(a =>
              RuntimeConfigQueries
                .updateNumberOfPreemptibleWorkers(runtime.runtimeConfigId, Some(a), ctx.now)
                .transaction
            )
            _ <- clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.Updating, ctx.now).transaction.void
          } yield true
        } else F.pure(false)

      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(runtime.cloudContext),
        new RuntimeException("non google project cloud context is not supported yet")
      )
      // Update the disk size
      _ <- msg.diskUpdate
        .traverse { d =>
          for {
            updateDiskSize <- d match {
              case DiskUpdate.PdSizeUpdate(_, diskName, targetSize) =>
                for {
                  zone <- F.fromOption(LeoLenses.gceZone.getOption(runtimeConfig),
                                       new RuntimeException("GCE runtime must have a zone")
                  )
                } yield UpdateDiskSizeParams.Gce(googleProject, diskName, targetSize, zone)
              case DiskUpdate.NoPdSizeUpdate(targetSize) =>
                for {
                  zone <- F.fromOption(LeoLenses.gceZone.getOption(runtimeConfig),
                                       new RuntimeException("GCE runtime must have a zone")
                  )
                } yield UpdateDiskSizeParams.Gce(
                  googleProject,
                  DiskName(
                    s"${runtime.runtimeName.asString}-1"
                  ), // user disk's diskname is always postfixed with -1 for non-pd runtimes
                  targetSize,
                  zone
                )
              case DiskUpdate.Dataproc(size, masterInstance) =>
                F.pure(UpdateDiskSizeParams.Dataproc(size, masterInstance))
            }
            _ <- runtimeConfig.cloudService.interpreter.updateDiskSize(updateDiskSize)
            _ <- LeoLenses.pdSizeUpdatePrism
              .getOption(d)
              .fold(
                RuntimeConfigQueries.updateDiskSize(runtime.runtimeConfigId, d.newDiskSize, ctx.now).transaction
              )(dd => persistentDiskQuery.updateSize(dd.diskId, dd.newDiskSize, ctx.now).transaction)
          } yield ()
        }
      isAoU = runtime.labels.get(AOU_UI_LABEL).contains("true")
      _ <-
        if (msg.stopToUpdateMachineType) {
          for {
            timeToStop <- nowInstant
            ctxStopping = Ask.const[F, AppContext](
              AppContext(ctx.traceId, timeToStop)
            )
            _ <- dbRef.inTransaction(clusterQuery.updateClusterStatus(msg.runtimeId, RuntimeStatus.Stopping, ctx.now))
            operation <- runtimeConfig.cloudService.interpreter
              .stopRuntime(
                StopRuntimeParams(RuntimeAndRuntimeConfig(runtime, runtimeConfig), ctx.now, false)
              )(
                ctxStopping
              )
            task = for {
              _ <- operation match {
                case Some(op) =>
                  val monitorContext = MonitorContext(ctx.now, runtime.id, ctx.traceId, RuntimeStatus.Stopping)
                  for {
                    operation <- F.blocking(op.get())
                    _ <- F.whenA(isSuccess(operation.getHttpErrorStatusCode))(
                      runtimeConfig.cloudService
                        .handlePollCheckCompletion(monitorContext, RuntimeAndRuntimeConfig(runtime, runtimeConfig))
                    )
                  } yield ()
                case None =>
                  runtimeConfig.cloudService.process(runtime.id, RuntimeStatus.Stopping, None).compile.drain
              }
              now <- nowInstant
              ctxStarting = Ask.const[F, AppContext](
                AppContext(ctx.traceId, now)
              )
              _ <- startAndUpdateRuntime(runtime, runtimeConfig, msg.newMachineType)(ctxStarting,
                                                                                     runtimeInstances,
                                                                                     monitor
              )
            } yield ()
            _ <- asyncTasks.offer(
              Task(
                ctx.traceId,
                task,
                Some(
                  handleRuntimeMessageError(msg.runtimeId,
                                            ctx.now,
                                            s"updating runtime ${runtime.projectNameString} failed"
                  )
                ),
                ctx.now,
                TaskMetricsTags("stopAndUpdateRuntime",
                                None,
                                Some(isAoU),
                                CloudProvider.Gcp,
                                Some(runtimeConfig.cloudService)
                )
              )
            )
          } yield ()
        } else {
          for {
            _ <- msg.newMachineType.traverse_(m =>
              runtimeConfig.cloudService.interpreter
                .updateMachineType(UpdateMachineTypeParams(RuntimeAndRuntimeConfig(runtime, runtimeConfig), m, ctx.now))
            )
            _ <-
              if (hasResizedCluster) {
                asyncTasks.offer(
                  Task(
                    ctx.traceId,
                    runtimeConfig.cloudService.process(runtime.id, RuntimeStatus.Updating, None).compile.drain,
                    Some(handleRuntimeMessageError(runtime.id, ctx.now, "updating runtime")),
                    ctx.now,
                    TaskMetricsTags("updateRuntime",
                                    None,
                                    Some(isAoU),
                                    CloudProvider.Gcp,
                                    Some(runtimeConfig.cloudService)
                    )
                  )
                )
              } else F.unit
          } yield ()
        }
    } yield ()

  private def startAndUpdateRuntime(
    runtime: Runtime,
    runtimeConfig: RuntimeConfig,
    targetMachineType: Option[MachineTypeName]
  )(implicit
    ev: Ask[F, AppContext],
    runtimeInstances: RuntimeInstances[F],
    monitor: RuntimeMonitor[F, CloudService]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- targetMachineType.traverse(m =>
        runtimeConfig.cloudService.interpreter
          .updateMachineType(UpdateMachineTypeParams(RuntimeAndRuntimeConfig(runtime, runtimeConfig), m, ctx.now))
      )
      updatedRuntimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      initBucket <- clusterQuery.getInitBucket(runtime.id).transaction
      bucketName <- F.fromOption(initBucket.map(_.bucketName),
                                 new RuntimeException(s"init bucket not found for ${runtime.projectNameString} in DB")
      )
      _ <- updatedRuntimeConfig.cloudService.interpreter
        .startRuntime(StartRuntimeParams(RuntimeAndRuntimeConfig(runtime, updatedRuntimeConfig), bucketName))
      _ <- dbRef.inTransaction {
        clusterQuery.updateClusterStatus(
          runtime.id,
          RuntimeStatus.Starting,
          ctx.now
        )
      }
      _ <- patchQuery.updatePatchAsComplete(runtime.id).transaction.void
      _ <- updatedRuntimeConfig.cloudService.process(runtime.id, RuntimeStatus.Starting, None).compile.drain
    } yield ()

  private[monitor] def handleCreateDiskMessage(msg: CreateDiskMessage)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    createDisk(msg, None, false)

  // this returns an F[F[Unit]. It kicks off the google operation, and then return an F containing the async polling task
  private[monitor] def createDisk(msg: CreateDiskMessage, formattedBy: Option[FormattedBy], sync: Boolean)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] = {
    val create = {
      val diskBuilder = Disk
        .newBuilder()
        .setName(msg.name.value)
        .setSizeGb(msg.size.gb)
        .setZone(msg.zone.value)
        .setType(msg.diskType.googleString(msg.googleProject, msg.zone))
        .setPhysicalBlockSizeBytes(msg.blockSize.bytes)
        .putAllLabels(Map("leonardo" -> "true").asJava)
      msg.sourceDisk.foreach(sourceDisk => diskBuilder.setSourceDisk(sourceDisk.asString))
      val (disk, timeout) = msg.sourceDisk match {
        case Some(d) =>
          (diskBuilder.setSourceDisk(d.asString).build(),
           config.persistentDiskMonitorConfig.create.sourceDiskCopyInMinutes
          )
        case None => (diskBuilder.build(), config.persistentDiskMonitorConfig.create.defaultInMinutes)
      }

      for {
        ctx <- ev.ask
        operationFutureOpt <- getGoogleDiskServiceFromRegistry()
          .createDisk(
            msg.googleProject,
            msg.zone,
            disk
          )
        _ <- operationFutureOpt match {
          case None => F.unit
          case Some(v) =>
            val task = for {
              _ <- F.blocking(v.get(timeout, TimeUnit.MINUTES))
              _ <- formattedBy match {
                case Some(value) =>
                  persistentDiskQuery
                    .updateStatusAndIsFormatted(msg.diskId, DiskStatus.Ready, value, ctx.now)
                    .transaction[F]
                    .void
                case None =>
                  persistentDiskQuery.updateStatus(msg.diskId, DiskStatus.Ready, ctx.now).transaction[F].void
              }
            } yield ()

            if (sync) task
            else {
              def errorHandler: Throwable => F[Unit] = e =>
                for {
                  _ <- logger.error(ctx.loggingCtx, e)(s"Fail to monitor disk(${msg.diskId.value}) creation")
                  _ <- persistentDiskQuery.updateStatus(msg.diskId, DiskStatus.Failed, ctx.now).transaction
                } yield ()
              asyncTasks.offer(
                Task(ctx.traceId,
                     task,
                     Some(errorHandler),
                     ctx.now,
                     TaskMetricsTags("createDisk", None, None, CloudProvider.Gcp)
                )
              )
            }
        }
      } yield ()
    }

    create.onError { case e =>
      for {
        ctx <- ev.ask
        _ <- logger.error(ctx.loggingCtx, e)(
          s"Failed to create disk ${msg.name.value} in Google project ${msg.googleProject.value}"
        )
        _ <- persistentDiskQuery.updateStatus(msg.diskId, DiskStatus.Failed, ctx.now).transaction[F]
      } yield ()
    }
  }

  private[monitor] def createGalaxyPostgresDiskOnlyInGoogle(project: GoogleProject,
                                                            zone: ZoneName,
                                                            appName: AppName,
                                                            dataDiskName: DiskName
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] = {
    // TODO: remove post-alpha release of Galaxy. For pre-alpha we are only creating the postgress disk in Google since we are not supporting persistence
    // see: https://broadworkbench.atlassian.net/wiki/spaces/IA/pages/859406337/2020-10-02+Galaxy+disk+attachment+pre+post+alpha+release
    val create = for {
      ctx <- ev.ask
      _ <- logger.info(ctx.loggingCtx)(s"Beginning postgres disk creation for app ${appName.value}")
      operationFutureOpt <- getGoogleDiskServiceFromRegistry().createDisk(
        project,
        zone,
        GKEAlgebra.buildGalaxyPostgresDisk(zone, dataDiskName, config.galaxyDiskConfig)
      )
      _ <- operationFutureOpt match {
        case None => F.unit
        case Some(v) =>
          for {
            operation <- F.blocking(v.get())
            _ <- F.raiseUnless(isSuccess(operation.getHttpErrorStatusCode))(
              new Exception(s"fail to create disk ${project}/${dataDiskName} due to ${operation}")
            )
          } yield ()
      }
    } yield ()

    create.onError { case e =>
      for {
        ctx <- ev.ask
        _ <- logger.error(ctx.loggingCtx, e)(
          s"Failed to create Galaxy postgres disk in Google project ${project.value}, AppName: ${appName.value}"
        )
      } yield ()
    }
  }

  private[monitor] def handleDeleteDiskMessage(msg: DeleteDiskMessage)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    deleteDisk(msg.diskId, false)

  private[monitor] def deleteDisk(diskId: DiskId, sync: Boolean)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- logger.info(ctx.loggingCtx)(s"Beginning disk deletion for ${diskId}")
      diskOpt <- persistentDiskQuery.getById(diskId).transaction
      disk <- diskOpt.fold(
        F.raiseError[PersistentDisk](PubsubHandleMessageError.DiskNotFound(diskId))
      )(F.pure)
      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(disk.cloudContext),
        new RuntimeException("non google project cloud context is not supported yet")
      )
      opFutureOpt <- getGoogleDiskServiceFromRegistry().deleteDisk(googleProject, disk.zone, disk.name)
      _ <- opFutureOpt match {
        case None => F.unit
        case Some(v) =>
          val task = for {
            operationAttempt <- F.blocking(v.get()).attempt
            _ <- operationAttempt match {
              case Left(error: java.util.concurrent.ExecutionException) if error.getMessage.contains("Not Found") =>
                logger.info(ctx.loggingCtx)("disk is already deleted") >> persistentDiskQuery
                  .delete(diskId, ctx.now)
                  .transaction[F]
                  .void >> authProvider
                  .notifyResourceDeleted(
                    disk.samResource,
                    disk.auditInfo.creator,
                    googleProject
                  )
              case Left(error) =>
                F.raiseError(
                  new RuntimeException(s"fail to delete disk ${googleProject}/${disk.name}", error)
                )
              case Right(op) =>
                for {
                  _ <- F.raiseUnless(isSuccess(op.getHttpErrorStatusCode))(
                    new RuntimeException(s"fail to delete disk ${googleProject}/${disk.name} due to ${op}")
                  )
                  _ <- persistentDiskQuery.delete(diskId, ctx.now).transaction[F].void >> authProvider
                    .notifyResourceDeleted(
                      disk.samResource,
                      disk.auditInfo.creator,
                      googleProject
                    )
                } yield ()
            }
          } yield ()
          if (sync) task
          else {
            asyncTasks.offer(
              Task(ctx.traceId,
                   task,
                   Some(logError(s"${diskId.value}", "Deleting Disk")),
                   ctx.now,
                   TaskMetricsTags("deleteDisk", None, None, CloudProvider.Gcp)
              )
            )
          }
      }
    } yield ()

  private[monitor] def deleteGalaxyPostgresDiskOnlyInGoogle(project: GoogleProject,
                                                            zone: ZoneName,
                                                            appName: AppName,
                                                            namespaceName: NamespaceName,
                                                            dataDiskName: DiskName
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    // TODO: remove post-alpha release of Galaxy. For pre-alpha we are only deleting the postgress disk in Google since we are not supporting persistence
    // see: https://broadworkbench.atlassian.net/wiki/spaces/IA/pages/859406337/2020-10-02+Galaxy+disk+attachment+pre+post+alpha+release
    for {
      ctx <- ev.ask
      _ <- logger.info(ctx.loggingCtx)(
        s"Beginning postres disk deletion for app ${appName.value} in project ${project.value}"
      )
      operation <- deleteGalaxyPostgresDisk(dataDiskName, namespaceName, project, zone)
      _ <- operation match {
        case None => F.unit
        case Some(op) =>
          F.raiseUnless(isSuccess(op.getHttpErrorStatusCode))(
            new Exception(s"Failed to delete postres disk in app ${appName.value} in project ${project.value} ${op}")
          )
      }
    } yield ()

  private[monitor] def handleUpdateDiskMessage(msg: UpdateDiskMessage)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      diskOpt <- persistentDiskQuery.getById(msg.diskId).transaction
      disk <- diskOpt.fold(
        F.raiseError[PersistentDisk](PubsubHandleMessageError.DiskNotFound(msg.diskId))
      )(F.pure)
      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(disk.cloudContext),
        AzureUnimplementedException("Azure disk is not supported yet")
      )
      opFuture <- getGoogleDiskServiceFromRegistry().resizeDisk(googleProject, disk.zone, disk.name, msg.newSize.gb)

      task = for {
        operation <- F.blocking(opFuture.get())
        _ <- F.raiseUnless(isSuccess(operation.getHttpErrorStatusCode))(
          new RuntimeException(s"fail to resize disk ${googleProject}/${disk.name} due to ${operation}")
        )
        now <- nowInstant
        _ <- persistentDiskQuery.updateSize(msg.diskId, msg.newSize, now).transaction[F]
      } yield ()
      _ <- asyncTasks.offer(
        Task(
          ctx.traceId,
          task,
          Some(logError(s"${ctx.traceId.asString} | ${msg.diskId.value}", "Updating Disk")),
          ctx.now,
          TaskMetricsTags("updateDisk", None, None, CloudProvider.Gcp)
        )
      )
    } yield ()

  private[monitor] def handleCreateAppMessage(msg: CreateAppMessage)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask

      // validate diskId from the message exists in DB
      disk <- msg.createDisk.traverse { diskId =>
        for {
          diskOpt <- persistentDiskQuery.getById(diskId).transaction
          d <- F.fromOption(
            diskOpt,
            PubsubKubernetesError(
              AppError(s"${diskId.value} is not found in database",
                       ctx.now,
                       ErrorAction.CreateApp,
                       ErrorSource.Disk,
                       None,
                       Some(ctx.traceId)
              ),
              Some(msg.appId),
              false,
              None,
              Some(diskId),
              None
            )
          )
        } yield d
      }

      // The "create app" flow potentially does a number of things:
      //  1. creates a Kubernetes cluster if it doesn't exist
      //  2. creates a nodepool within the cluster if it doesn't exist
      //  3. creates a disk if it doesn't exist
      //  4. creates an app
      //
      // Numbers 1-3 are all Google calls; (4) is a helm call. If (1) is needed, we will do the
      // _initial_ GKE call synchronous to the pubsub processing so we can nack the message on
      // errors and retry. All other operations plus monitoring will be asynchronous to the message
      // handler.
      createClusterOrNodepoolOp = msg.clusterNodepoolAction match {
        case Some(ClusterNodepoolAction.CreateClusterAndNodepool(clusterId, defaultNodepoolId, nodepoolId)) =>
          createClusterAndNodepools(msg, clusterId, autopilotEnabled = false, List(defaultNodepoolId, nodepoolId))
        case Some(ClusterNodepoolAction.CreateCluster(clusterId)) =>
          createClusterAndNodepools(msg, clusterId, autopilotEnabled = true, List.empty)
        case Some(ClusterNodepoolAction.CreateNodepool(nodepoolId)) =>
          // create nodepool asynchronously
          getGkeAlgFromRegistry()
            .createAndPollNodepool(CreateNodepoolParams(nodepoolId, msg.project))
            .adaptError { case e =>
              PubsubKubernetesError(
                AppError(e.getMessage, ctx.now, ErrorAction.CreateApp, ErrorSource.Nodepool, None, Some(ctx.traceId)),
                Some(msg.appId),
                false,
                Some(nodepoolId),
                None,
                None
              )
            }
        case None => F.unit
      }

      // create disk asynchronously
      createDiskOp = disk
        .traverse(d =>
          createDisk(CreateDiskMessage.fromDisk(d, Some(ctx.traceId)),
                     Some(appTypeToFormattedByType(msg.appType)),
                     true
          ).adaptError { case e =>
            PubsubKubernetesError(
              AppError(e.getMessage, ctx.now, ErrorAction.CreateApp, ErrorSource.Disk, None, Some(ctx.traceId)),
              Some(msg.appId),
              false,
              None,
              disk.map(_.id),
              None
            )
          }
        )
        .void

      // create second Galaxy disk asynchronously
      createSecondDiskOp =
        if (msg.appType == AppType.Galaxy && disk.isDefined) {
          val d = disk.get // it's safe to do `.get` here because we've verified
          for {
            res <- createGalaxyPostgresDiskOnlyInGoogle(msg.project, ZoneName("us-central1-a"), msg.appName, d.name)
              .adaptError { case e =>
                PubsubKubernetesError(
                  AppError(e.getMessage, ctx.now, ErrorAction.CreateApp, ErrorSource.Disk, None, Some(ctx.traceId)),
                  Some(msg.appId),
                  false,
                  None,
                  None,
                  None
                )
              }
          } yield res
        } else F.unit

      // build asynchronous task
      task = for {
        // parallelize disk creation and cluster/nodepool monitoring
        _ <- List(createDiskOp, createSecondDiskOp, createClusterOrNodepoolOp).parSequence_

        // create and monitor app
        _ <- getGkeAlgFromRegistry()
          .createAndPollApp(
            CreateAppParams(msg.appId, msg.project, msg.appName, msg.machineType)
          )
          .onError { case e =>
            cleanUpAfterCreateAppError(msg.appId, msg.appName, msg.project, msg.createDisk, e)
          }
          .adaptError { case e =>
            PubsubKubernetesError(
              AppError(e.getMessage, ctx.now, ErrorAction.CreateApp, ErrorSource.App, None, Some(ctx.traceId)),
              Some(msg.appId),
              false,
              None,
              None,
              None
            )
          }
      } yield ()

      appChart <- appQuery.getAppChart(msg.appId).transaction
      labels <- labelQuery.getAllForResource(msg.appId.id, LabelResourceType.app).transaction
      isAoU = labels.get(AOU_UI_LABEL).contains("true")
      _ <- asyncTasks.offer(
        Task(
          ctx.traceId,
          task,
          Some(handleKubernetesError),
          ctx.now,
          TaskMetricsTags("createApp", Some(resolveAppType(msg.appType, appChart)), Some(isAoU), CloudProvider.Gcp)
        )
      )
    } yield ()

  private def createClusterAndNodepools(msg: CreateAppMessage,
                                        clusterId: KubernetesClusterLeoId,
                                        autopilotEnabled: Boolean,
                                        nodepoolLeoIds: List[NodepoolLeoId]
  )(implicit
    ev: Ask[F, AppContext]
  ) = for {
    ctx <- ev.ask
    // initial the createCluster call synchronously
    createClusterResultOpt <- getGkeAlgFromRegistry()
      .createCluster(
        CreateClusterParams(clusterId, msg.project, nodepoolLeoIds, msg.enableIntraNodeVisibility, autopilotEnabled)
      )
      .onError { case _ => cleanUpAfterCreateClusterError(clusterId, msg.project) }
      .adaptError { case e =>
        PubsubKubernetesError(
          AppError(e.getMessage, ctx.now, ErrorAction.CreateApp, ErrorSource.Cluster, None, Some(ctx.traceId)),
          Some(msg.appId),
          false,
          // We leave cluster id and default nodepool id as none here because we want the status to stay as DELETED and not transition to ERROR.
          // The app will have the error so the user can see it, delete their app, and try again
          None,
          None,
          None
        )
      }

    _ <- F.sleep(
      config.gkeClusterCreationPollingInitialDelay
    ) // Creating GKE cluster takes a while, so there's no point polling right away
    // monitor cluster creation asynchronously
    monitorOp <- createClusterResultOpt.traverse_(createClusterResult =>
      getGkeAlgFromRegistry()
        .pollCluster(PollClusterParams(clusterId, msg.project, createClusterResult))
        .adaptError { case e =>
          PubsubKubernetesError(
            AppError(e.getMessage, ctx.now, ErrorAction.CreateApp, ErrorSource.Cluster, None, Some(ctx.traceId)),
            Some(msg.appId),
            false,
            // We leave cluster id and default nodepool id as none here because we want the status to stay as DELETED and not transition to ERROR.
            // The app will have the error so the user can see it, delete their app, and try again
            None,
            None,
            None
          )
        }
    )
  } yield monitorOp

  private[monitor] def handleDeleteAppMessage(msg: DeleteAppMessage)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    deleteApp(msg, false, false)

  private[monitor] def deleteApp(msg: DeleteAppMessage, sync: Boolean, errorAfterDelete: Boolean)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      dbAppOpt <- KubernetesServiceDbQueries
        .getFullAppById(CloudContext.Gcp(msg.project), msg.appId)
        .transaction
      dbApp <- F.fromOption(
        dbAppOpt,
        AppNotFoundException(CloudContext.Gcp(msg.project), msg.appName, ctx.traceId, "No active app found in DB")
      )
      zone = ZoneName("us-central1-a")

      getPostgresDiskOp = dbApp.app.appResources.disk.flatTraverse { d =>
        for {
          postgresDiskOpt <- getGoogleDiskServiceFromRegistry()
            .getDisk(
              msg.project,
              zone,
              getGalaxyPostgresDiskName(d.name, config.galaxyDiskConfig.postgresDiskNameSuffix)
            )
          res <- postgresDiskOpt match {
            case Some(disk) => F.pure(disk.some)
            case None =>
              getGoogleDiskServiceFromRegistry().getDisk(
                msg.project,
                zone,
                getOldStyleGalaxyPostgresDiskName(dbApp.app.appResources.namespace,
                                                  config.galaxyDiskConfig.postgresDiskNameSuffix
                )
              )
          }
        } yield res
      }
      // The app must be deleted before the nodepool and disk, to future proof against the app potentially flushing the postgres db somewhere
      task = for {
        postgresDiskOpt <- getPostgresDiskOp

        // we record the last disk detach timestamp here, before it is removed from galaxy
        // this is needed before we can delete disks
        postgresOriginalDetachTimestampOpt = postgresDiskOpt.map(_.getLastDetachTimestamp)

        dataDiskOriginalDetachTimestampOpt <- dbApp.app.appResources.disk.flatTraverse { d =>
          getGoogleDiskServiceFromRegistry()
            .getDisk(
              msg.project,
              zone,
              d.name
            )
            .map(_.map(_.getLastDetachTimestamp))
        }
        _ <- getGkeAlgFromRegistry()
          .deleteAndPollApp(DeleteAppParams(msg.appId, msg.project, msg.appName, errorAfterDelete))
          .adaptError { case e =>
            PubsubKubernetesError(
              AppError(e.getMessage, ctx.now, ErrorAction.DeleteApp, ErrorSource.App, None, Some(ctx.traceId)),
              Some(msg.appId),
              false,
              None,
              None,
              None
            )
          }

        // detach/delete disk when we need to delete disk
        _ <- msg.diskId.traverse_ { diskId =>
          // we now use the detach timestamp recorded prior to helm uninstall so we can observe when galaxy actually 'detaches' the disk from google's perspective
          val getPostgresDisk = getPostgresDiskOp
          val getDataDisk = dbApp.app.appResources.disk.flatTraverse { d =>
            getGoogleDiskServiceFromRegistry()
              .getDisk(
                msg.project,
                zone,
                d.name
              )
          }
          for {
            // For Galaxy apps, wait for the postgres disk to detach before deleting the disks;
            // otherwise, only wait for data disk to detach
            _ <- (for {
              _ <-
                if (dbApp.app.appType == AppType.Galaxy)
                  streamUntilDoneOrTimeout(
                    getDiskDetachStatus(postgresOriginalDetachTimestampOpt, getPostgresDisk),
                    50,
                    5 seconds,
                    "The postgres disk failed to detach within the time limit, cannot proceed with delete disk"
                  )
                else F.unit
              _ <- streamUntilDoneOrTimeout(
                getDiskDetachStatus(dataDiskOriginalDetachTimestampOpt, getDataDisk),
                50,
                5 seconds,
                "The data disk failed to detach within the time limit, cannot proceed with delete disk"
              )
            } yield ()).adaptError { case e =>
              PubsubKubernetesError(
                AppError(e.getMessage, ctx.now, ErrorAction.DeleteApp, ErrorSource.Disk, None, Some(ctx.traceId)),
                Some(msg.appId),
                false,
                None,
                None,
                None
              )
            }
            deleteDataDisk = deleteDisk(diskId, true).adaptError { case e =>
              PubsubKubernetesError(
                AppError(e.getMessage, ctx.now, ErrorAction.DeleteApp, ErrorSource.Disk, None, Some(ctx.traceId)),
                Some(msg.appId),
                false,
                None,
                None,
                None
              )
            }

            deletePostgresDisk =
              if (dbApp.app.appType == AppType.Galaxy)
                deleteGalaxyPostgresDiskOnlyInGoogle(msg.project,
                                                     zone,
                                                     msg.appName,
                                                     dbApp.app.appResources.namespace,
                                                     dbApp.app.appResources.disk.get.name
                )
                  .adaptError { case e =>
                    PubsubKubernetesError(
                      AppError(e.getMessage,
                               ctx.now,
                               ErrorAction.DeleteApp,
                               ErrorSource.PostgresDisk,
                               None,
                               Some(ctx.traceId)
                      ),
                      Some(msg.appId),
                      false,
                      None,
                      None,
                      None
                    )
                  }
              else F.unit

            // Cleans up staging bucket. Right now, only ALLOWED app uses staging bucket
            deleteStagingBucket = dbApp.cluster.cloudContext match {
              case CloudContext.Gcp(project) =>
                for {
                  disk <- persistentDiskQuery.getById(diskId).transaction
                  _ <- getGoogleStorageServiceFromRegistry()
                    .deleteBucket(project,
                                  GKEAlgebra.buildAppStagingBucketName(disk.get.name),
                                  true,
                                  traceId = Some(ctx.traceId)
                    ) // using .get here should be ok because given a diskId, there will definitely be a disk record in DB
                    .compile
                    .lastOrError
                    .void
                    .handleErrorWith {
                      case e: com.google.cloud.storage.StorageException if e.getCode == 404 =>
                        logger.info(ctx.loggingCtx, e)(
                          "Fail to clean up staging bucket because it doesn't exist"
                        )
                      case e =>
                        logger.error(ctx.loggingCtx, e)(
                          "Fail to clean up staging bucket"
                        )
                    }
                } yield ()
              case CloudContext.Azure(_) =>
                logger.error(ctx.loggingCtx)(
                  "This should never happen because Azure app doesn't go through this code path. But not failing app deletion because deleting staging bucket isn't in critical path"
                )
            }
            _ <- List(deleteDataDisk, deletePostgresDisk, deleteStagingBucket).parSequence_
          } yield ()
        }
        _ <-
          if (!errorAfterDelete)
            dbApp.app.status match {
              // If the message is resubmitted, and this step has already been run, we don't want to re-notify the app creator and update the deleted timestamp
              case AppStatus.Deleted => F.unit
              case _ =>
                appQuery.markAsDeleted(msg.appId, ctx.now).transaction.void >> authProvider
                  .notifyResourceDeleted(dbApp.app.samResourceId, dbApp.app.auditInfo.creator, msg.project)
                  .void
            }
          else F.unit
      } yield ()

      labels <- labelQuery.getAllForResource(msg.appId.id, LabelResourceType.app).transaction
      isAoU = labels.get(AOU_UI_LABEL).contains("true")
      _ <-
        if (sync) task
        else {
          val toolType = resolveAppType(dbApp.app.appType, Some(dbApp.app.chart))
          asyncTasks.offer(
            Task(
              ctx.traceId,
              task,
              Some(handleKubernetesError),
              ctx.now,
              TaskMetricsTags("deleteApp", Some(toolType), Some(isAoU), CloudProvider.Gcp)
            )
          )
        }
    } yield ()

  private def getDiskDetachStatus(originalDetachTimestampOpt: Option[String],
                                  getDisk: F[Option[Disk]]
  ): F[DiskDetachStatus] =
    for {
      disk <- getDisk
    } yield DiskDetachStatus(disk, originalDetachTimestampOpt)

  implicit val diskDetachDone: DoneCheckable[DiskDetachStatus] = x =>
    x.disk.map(_.getLastDetachTimestamp) != x.originalDetachTimestampOpt

  private def cleanUpAfterCreateClusterError(clusterId: KubernetesClusterLeoId, project: GoogleProject)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- logger.info(ctx.loggingCtx)(
        s"Beginning clean up for cluster $clusterId in project $project due to an error during cluster creation"
      )
      _ <- kubernetesClusterQuery.markPendingDeletion(clusterId).transaction
      _ <- getGkeAlgFromRegistry().deleteAndPollCluster(DeleteClusterParams(clusterId, project)).handleErrorWith { e =>
        // we do not want to bubble up errors with cluster clean-up
        logger.error(ctx.loggingCtx, e)(
          s"An error occurred during resource clean up for cluster ${clusterId} in project ${project}"
        )
      }
    } yield ()

  private def resolveAppType(appType: AppType, chart: Option[Chart]): String = appType match {
    case AppType.Allowed =>
      chart match {
        case Some(value) =>
          AllowedChartName.fromChartName(value.name) match {
            case Some(AllowedChartName.Sas)     => "sas"
            case Some(AllowedChartName.RStudio) => "rstudio"
            case _                              => "unknown"
          }
        case None => "unknown"
      }

    case x => x.toString.toLowerCase()
  }

  // clean-up resources in the event of an app creation error
  private def cleanUpAfterCreateAppError(appId: AppId,
                                         appName: AppName,
                                         project: GoogleProject,
                                         diskId: Option[DiskId],
                                         error: Throwable
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- logger.info(ctx.loggingCtx)(
        s"Attempting to clean up resources due to app creation error for app ${appName} in project ${project} due to ${error.getMessage}"
      )
      // we need to look up the app because we always want to clean up the nodepool associated with an errored app, even if it was pre-created
      appOpt <- KubernetesServiceDbQueries.getFullAppById(CloudContext.Gcp(project), appId).transaction
      // note that this will only clean up the disk if it was created as part of this app creation.
      // it should not clean up the disk if it already existed
      _ <- appOpt.traverse { _ =>
        val deleteMsg =
          DeleteAppMessage(appId, appName, project, diskId, Some(ctx.traceId))
        // This is a good-faith attempt at clean-up. We do not want to take any action if clean-up fails for some reason.
        deleteApp(deleteMsg, true, true).handleErrorWith { e =>
          logger.error(ctx.loggingCtx, e)(
            s"An error occurred during resource clean up for app ${appName} in project ${project}"
          )
        }
      }
    } yield ()

  private[monitor] def handleStopAppMessage(msg: StopAppMessage)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      stopApp = getGkeAlgFromRegistry()
        .stopAndPollApp(StopAppParams(msg.appId, msg.appName, msg.project))
        .adaptError { case e =>
          PubsubKubernetesError(
            AppError(e.getMessage, ctx.now, ErrorAction.StopApp, ErrorSource.App, None, Some(ctx.traceId)),
            Some(msg.appId),
            false,
            None,
            None,
            None
          )
        }
      _ <- asyncTasks.offer(
        Task(ctx.traceId,
             stopApp,
             Some(handleKubernetesError),
             ctx.now,
             TaskMetricsTags("stopApp", None, None, CloudProvider.Gcp)
        )
      )
    } yield ()

  private[monitor] def handleStartAppMessage(
    msg: StartAppMessage
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      startApp = getGkeAlgFromRegistry()
        .startAndPollApp(StartAppParams(msg.appId, msg.appName, msg.project))
        .adaptError { case e =>
          PubsubKubernetesError(
            AppError(e.getMessage, ctx.now, ErrorAction.StartApp, ErrorSource.App, None, Some(ctx.traceId)),
            Some(msg.appId),
            false,
            None,
            None,
            None
          )
        }

      _ <- asyncTasks.offer(
        Task(ctx.traceId,
             startApp,
             Some(handleKubernetesError),
             ctx.now,
             TaskMetricsTags("startApp", None, Some(false), CloudProvider.Gcp)
        )
      )
    } yield ()

  private[monitor] def handleUpdateAppMessage(
    msg: UpdateAppMessage
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      appOpt <- KubernetesServiceDbQueries
        .getFullAppById(msg.cloudContext, msg.appId)
        .transaction
      appResult <- appOpt.fold(
        F.raiseError[GetAppResult](PubsubHandleMessageError.AppNotFound(msg.appId.id, msg))
      )(F.pure)

      latestAppChartVersion <- KubernetesAppConfig.configForTypeAndCloud(appResult.app.appType,
                                                                         msg.cloudContext.cloudProvider
      ) match {
        case Some(AllowedAppConfig(_, rstudioChartVersion, sasChartVersion, _, _, _, _, _, _, _)) =>
          AllowedChartName.fromChartName(appResult.app.chart.name) match {
            case Some(AllowedChartName.RStudio) =>
              F.pure(rstudioChartVersion)
            case Some(AllowedChartName.Sas) =>
              F.pure(sasChartVersion)
            case None =>
              F.raiseError[ChartVersion](
                AppTypeNotSupportedOnCloudException(msg.cloudContext.cloudProvider, appResult.app.appType, ctx.traceId)
              )
          }
        case Some(conf) => F.pure(conf.chartVersion)
        case None =>
          F.raiseError[ChartVersion](
            AppTypeNotSupportedOnCloudException(msg.cloudContext.cloudProvider, appResult.app.appType, ctx.traceId)
          )
      }

      updateApp = (msg.cloudContext match {
        case CloudContext.Gcp(_) =>
          getGkeAlgFromRegistry()
            .updateAndPollApp(UpdateAppParams(msg.appId, msg.appName, latestAppChartVersion, msg.googleProject))
        case CloudContext.Azure(azureContext) =>
          azurePubsubHandler
            .updateAndPollApp(msg.appId, msg.appName, latestAppChartVersion, msg.workspaceId, azureContext)
      }).flatMap { _ =>
        updateAppLogQuery
          .update(msg.appId, msg.jobId, UpdateAppJobStatus.Success, Some(ctx.now))
          .transaction
          .void
      }

      updateAppWithErrorHandling = updateApp
        .handleErrorWith { throwable =>
          updateAppLogQuery
            .update(msg.appId, msg.jobId, UpdateAppJobStatus.Error, Some(ctx.now))
            .transaction >> (throwable match {
            // Fatal case (as in, the app is no longer usable), polling app liveness failed
            // The two fatal cases are included separately, because later we may wish to fail fatally on `HelmException`, but roll back on `AppUpdatePollingException`
            // This would provide more cases in which an app is left in a usable state
            // Note that an app can also emit this error if the liveness probe fails before an update is triggered, so rolling back may not have an effect
            case e: AppUpdatePollingException => F.raiseError(e)
            // Fatal case, helm call failed for either listener or app charts
            case e: HelmException => F.raiseError(e)
            // Non fatal catch-all case, set app status back to running but append whatever error occurred in db for traceability
            case e =>
              for {
                _ <- appQuery.updateStatus(msg.appId, AppStatus.Running).transaction
                // You cannot update this log wording without updating the corresponding prod and staging alerts here
                //     https://console.cloud.google.com/monitoring/alerting/policies/8184448493858086363?project=broad-dsde-prod
                //     https://console.cloud.google.com/monitoring/alerting/policies/16136890211426349187?project=broad-dsde-staging
                errorContext =
                  s"Error updating Azure app with id ${msg.appId.id} and cloudContext ${msg.cloudContext.asString}"
                _ <- logger.warn(ctx.loggingCtx, e)(errorContext)
                error = AppError(
                  s"${errorContext}: ${e.getMessage}",
                  ctx.now,
                  ErrorAction.UpdateApp,
                  ErrorSource.App,
                  None,
                  Some(ctx.traceId)
                )
                _ <- appErrorQuery.save(msg.appId, error).transaction
              } yield ()
          })
        }
        .adaptError { case e =>
          PubsubKubernetesError(
            AppError(e.getMessage, ctx.now, ErrorAction.UpdateApp, ErrorSource.App, None, Some(ctx.traceId)),
            Some(msg.appId),
            false,
            None,
            None,
            None
          )
        }

      _ <- asyncTasks.offer(
        Task(ctx.traceId,
             updateAppWithErrorHandling,
             Some(handleKubernetesError),
             ctx.now,
             TaskMetricsTags("updateApp", None, None, msg.cloudContext.cloudProvider)
        )
      )
    } yield ()

  private def handleKubernetesError(e: Throwable)(implicit ev: Ask[F, AppContext]): F[Unit] = ev.ask.flatMap { ctx =>
    e match {
      case e: PubsubKubernetesError =>
        for {
          _ <- logger.error(ctx.loggingCtx, e)("top level handler for handleKubernetesError")
          _ <- e.appId.traverse(id => appErrorQuery.save(id, e.dbError).transaction)
          _ <- e.appId.traverse(id => appQuery.markAsErrored(id).transaction)
          _ <- e.clusterId.traverse(clusterId =>
            kubernetesClusterQuery.updateStatus(clusterId, KubernetesClusterStatus.Error).transaction
          )
          _ <- e.diskId.traverse_(diskId =>
            persistentDiskQuery.updateStatus(diskId, DiskStatus.Failed, ctx.now).transaction
          )
          _ <- e.nodepoolId.traverse(nodepoolId =>
            nodepoolQuery.updateStatus(nodepoolId, NodepoolStatus.Error).transaction
          )
        } yield ()
      case _ =>
        logger.error(ctx.loggingCtx, e)(
          s"handleKubernetesError should not be used with a non kubernetes error. WHATEVER ERROR THIS IS SHOULD BE HANDLED AT ITS SOURCE TO APPROPRIATELY UPDATE DB STATE. Error: ${e}"
        )
    }
  }

  private[monitor] def createRuntimeErrorHandler(runtimeId: Long, cloudService: CloudService, now: Instant)(
    e: Throwable
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- metrics.incrementCounter(s"createRuntimeError", 1)
      _ <- logger.error(ctx.loggingCtx, e)(s"Failed to create runtime ${runtimeId}")
      // want to detach persistent disk for runtime
      _ <- cloudService match {
        case CloudService.GCE =>
          for {
            runtimeOpt <- clusterQuery.getClusterById(runtimeId).transaction
            _ <- runtimeOpt.traverse_ { runtime =>
              // If the disk is in Creating status, then it means it hasn't been used previously. Hence delete the disk
              // if the runtime fails to create.
              // Otherwise, the disk is most likely used previously by an old runtime, and we don't want to delete it
              if (runtime.status == RuntimeStatus.Creating) {
                for {
                  googleProject <- runtime.cloudContext match {
                    case CloudContext.Gcp(value) => F.pure(value)
                    case CloudContext.Azure(_)   => F.raiseError(new RuntimeException("This should never happen"))
                  }
                  runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
                  gceRuntimeConfig <- runtimeConfig match {
                    case x: RuntimeConfig.GceWithPdConfig => F.pure(x.some)
                    case _                                => F.pure(none[RuntimeConfig.GceWithPdConfig])
                  }

                  _ <- gceRuntimeConfig.traverse_ { rc =>
                    for {
                      persistentDiskOpt <- rc.persistentDiskId.flatTraverse(did =>
                        persistentDiskQuery.getPersistentDiskRecord(did).transaction
                      )
                      _ <- persistentDiskOpt match {
                        case Some(value) =>
                          if (value.status == DiskStatus.Creating || value.status == DiskStatus.Failed) {
                            persistentDiskOpt.traverse_(d =>
                              getGoogleDiskServiceFromRegistry().deleteDisk(googleProject,
                                                                            rc.zone,
                                                                            d.name
                              ) >> persistentDiskQuery
                                .delete(d.id, now)
                                .transaction
                            )
                          } else F.unit
                        case None => F.unit
                      }
                    } yield ()
                  }
                } yield ()
              } else F.unit
            }
            errorMessage = e match {
              case leoEx: LeoException =>
                Some(ErrorReport.loggableString(leoEx.toErrorReport))
              case ee: com.google.api.gax.rpc.AbortedException
                  if ee.getStatusCode.getCode.getHttpStatusCode == 409 && ee.getMessage.contains("already exists") =>
                None // this could happen when pubsub redelivers an event unexpectedly
              case _ =>
                Some(s"Failed to create cluster ${runtimeId} due to ${e.getMessage}")
            }
            _ <- errorMessage.traverse(m =>
              (clusterErrorQuery.save(runtimeId, RuntimeError(m.take(1024), None, now, Some(ctx.traceId))) >>
                clusterQuery.updateClusterStatus(runtimeId, RuntimeStatus.Error, now)).transaction[F]
            )
            _ <- clusterQuery.detachPersistentDisk(runtimeId, now).transaction
          } yield ()
        case CloudService.Dataproc =>
          val errorMessage = e match {
            case leoEx: LeoException =>
              Some(ErrorReport.loggableString(leoEx.toErrorReport))
            case ee: com.google.api.gax.rpc.AbortedException
                if ee.getStatusCode.getCode.getHttpStatusCode == 409 && ee.getMessage.contains("already exists") =>
              None // this could happen when pubsub redelivers an event unexpectedly
            case _ =>
              Some(s"Failed to create cluster ${runtimeId} due to ${e.getMessage}")
          }
          errorMessage.traverse(m =>
            (clusterErrorQuery.save(runtimeId, RuntimeError(m.take(1024), None, now, Some(ctx.traceId))) >>
              clusterQuery.updateClusterStatus(runtimeId, RuntimeStatus.Error, now)).transaction[F]
          )
        case CloudService.AzureVm => F.unit
      }
    } yield ()

  private def handleRuntimeMessageError(runtimeId: Long, now: Instant, msg: String)(
    e: Throwable
  )(implicit ev: Ask[F, AppContext]): F[Unit] = {
    val m = s"${msg} due to ${e.getMessage}"
    for {
      ctx <- ev.ask
      _ <- clusterErrorQuery.save(runtimeId, RuntimeError(m, None, now)).transaction
      _ <- logger.error(ctx.loggingCtx, e)(m)
    } yield ()
  }

  private def logError(projectAndName: String, action: String)(implicit ev: Ask[F, AppContext]): Throwable => F[Unit] =
    t => ev.ask.flatMap(ctx => logger.error(ctx.loggingCtx, t)(s"Fail to monitor ${projectAndName} for ${action}"))

  private[leonardo] def deleteGalaxyPostgresDisk(
    diskName: DiskName,
    namespaceName: NamespaceName,
    project: GoogleProject,
    zone: ZoneName
  )(implicit traceId: Ask[F, AppContext]): F[Option[Operation]] =
    for {
      postgresDiskOpt <- getGoogleDiskServiceFromRegistry()
        .deleteDisk(
          project,
          zone,
          getGalaxyPostgresDiskName(diskName, config.galaxyDiskConfig.postgresDiskNameSuffix)
        )
      res <- postgresDiskOpt match {
        case Some(operation) =>
          F.blocking(operation.get()).map(_.some)
          for {
            operation <- F.blocking(operation.get())
            _ <- F.raiseUnless(isSuccess(operation.getHttpErrorStatusCode))(
              new Exception(s"fail to delete disk ${project.value}/${diskName.value} due to ${operation}")
            )
          } yield operation.some
        case None =>
          val diskName =
            GKEAlgebra.getOldStyleGalaxyPostgresDiskName(namespaceName, config.galaxyDiskConfig.postgresDiskNameSuffix)
          for {
            operationFutureOpt <- getGoogleDiskServiceFromRegistry()
              .deleteDisk(
                project,
                zone,
                diskName
              )
            operation <- operationFutureOpt.traverse(optFuture =>
              for {
                operation <- F.blocking(optFuture.get())
                _ <- F.raiseUnless(isSuccess(operation.getHttpErrorStatusCode))(
                  new Exception(s"fail to delete disk ${project.value}/${diskName.value} due to ${operation}")
                )
              } yield operation
            )
          } yield operation
      }
    } yield res

  private[monitor] def handleCreateAppV2Message(
    msg: CreateAppV2Message
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- msg.cloudContext match {
        case CloudContext.Azure(c) =>
          val task =
            azurePubsubHandler.createAndPollApp(msg.appId, msg.appName, msg.workspaceId, c, msg.billingProfileId)
          asyncTasks.offer(
            Task(ctx.traceId,
                 task,
                 Some(handleKubernetesError),
                 ctx.now,
                 TaskMetricsTags("createAppV2", None, Some(false), CloudProvider.Azure)
            )
          )
        case CloudContext.Gcp(c) =>
          F.raiseError(
            PubsubKubernetesError(
              AppError(
                s"Error creating GCP app with id ${msg.appId} and cloudContext ${c.value}: CreateAppV2 not supported for GCP",
                ctx.now,
                ErrorAction.CreateApp,
                ErrorSource.App,
                None,
                Some(ctx.traceId)
              ),
              Some(msg.appId),
              false,
              None,
              None,
              None
            )
          )
      }
    } yield ()

  private[monitor] def handleDeleteAppV2Message(
    msg: DeleteAppV2Message
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- msg.cloudContext match {
        case CloudContext.Azure(c) =>
          val task =
            azurePubsubHandler.deleteApp(msg.appId, msg.appName, msg.workspaceId, c, msg.billingProfileId)
          asyncTasks.offer(
            Task(ctx.traceId,
                 task,
                 Some(handleKubernetesError),
                 ctx.now,
                 TaskMetricsTags("deleteAppV2", None, Some(false), CloudProvider.Azure)
            )
          )

        case CloudContext.Gcp(c) =>
          F.raiseError(
            PubsubKubernetesError(
              AppError(
                s"Error creating GCP app with id ${msg.appId} and cloudContext ${c.value}: DeleteAppV2 not supported for GCP",
                ctx.now,
                ErrorAction.DeleteApp,
                ErrorSource.App,
                None,
                Some(ctx.traceId)
              ),
              Some(msg.appId),
              false,
              None,
              None,
              None
            )
          )
      }
    } yield ()

  private[monitor] def handleDeleteDiskV2Message(
    msg: DeleteDiskV2Message
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      _ <- msg.cloudContext match {
        case CloudContext.Azure(_) =>
          azurePubsubHandler.deleteDisk(msg).adaptError { case e =>
            PubsubHandleMessageError.DiskDeletionError(
              msg.diskId,
              msg.workspaceId,
              e.getMessage
            )
          }
        case CloudContext.Gcp(_) =>
          deleteDisk(msg.diskId, false)
      }
    } yield ()
  private def getGoogleDiskServiceFromRegistry(): GoogleDiskService[F] = {
    logger.info(s"Getting googleDiskService from registry")
    cloudSpecificDependenciesRegistry.lookup[GcpDependencies[F]].get.googleDiskService
  }
  private def getGoogleStorageServiceFromRegistry(): GoogleStorageService[F] =
    cloudSpecificDependenciesRegistry.lookup[GcpDependencies[F]].get.googleStorageService
  private def getGkeAlgFromRegistry(): GKEAlgebra[F] = {
    logger.info(s"Getting gkeAlg from registry")
    cloudSpecificDependenciesRegistry.lookup[GKEAlgebra[F]].get
  }
  private def getMonitorFromRegistry(): RuntimeMonitor[F, CloudService] = {
    logger.info(s"Getting monitor from registry")
    cloudSpecificDependenciesRegistry.lookup[RuntimeMonitor[F, CloudService]].get
  }
  private def getRuntimeInstancesFromRegistry(): RuntimeInstances[F] = {
    logger.info(s"Getting runtimeInstances from registry")
    cloudSpecificDependenciesRegistry.lookup[RuntimeInstances[F]].get
  }
}
