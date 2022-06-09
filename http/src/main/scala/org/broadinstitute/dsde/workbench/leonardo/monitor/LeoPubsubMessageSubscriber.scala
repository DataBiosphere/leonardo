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
  Event,
  GoogleDiskService,
  GoogleSubscriber,
  MachineTypeName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.AppType.{appTypeToFormattedByType, Galaxy}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppNotFoundException
import org.broadinstitute.dsde.workbench.leonardo.http.{cloudServiceSyntax, _}
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

import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/**
 * @param config
 * @param subscriber
 * @param asyncTasks
 * @param googleDiskService
 * @param authProvider
 * @param gkeAlg
 * @param azurePubsubHandler
 * @param operationFutureCache This is used to cancel long running java Futures for Google operations. Currently, we only cancel existing stopping runtime operation if a `deleteRuntime`
 *                             message is received
 * @tparam F
 */
class LeoPubsubMessageSubscriber[F[_]](
  config: LeoPubsubMessageSubscriberConfig,
  subscriber: GoogleSubscriber[F, LeoPubsubMessage],
  asyncTasks: Queue[F, Task[F]],
  googleDiskService: GoogleDiskService[F],
  authProvider: LeoAuthProvider[F],
  gkeAlg: GKEAlgebra[F],
  azurePubsubHandler: AzurePubsubHandlerAlgebra[F],
  operationFutureCache: scalacache.Cache[F, Long, OperationFuture[Operation, Operation]]
)(implicit
  executionContext: ExecutionContext,
  F: Async[F],
  logger: StructuredLogger[F],
  dbRef: DbReference[F],
  runtimeInstances: RuntimeInstances[F],
  monitor: RuntimeMonitor[F, CloudService],
  metrics: OpenTelemetryMetrics[F]
) {

  private[monitor] def messageResponder(
    message: LeoPubsubMessage
  )(implicit traceId: Ask[F, AppContext]): F[Unit] =
    for {
      resp <- message match {
        case msg: CreateRuntimeMessage =>
          handleCreateRuntimeMessage(msg)
        case msg: DeleteRuntimeMessage =>
          handleDeleteRuntimeMessage(msg)
        case msg: StopRuntimeMessage =>
          handleStopRuntimeMessage(msg)
        case msg: StartRuntimeMessage =>
          handleStartRuntimeMessage(msg)
        case msg: UpdateRuntimeMessage =>
          handleUpdateRuntimeMessage(msg)
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
        case msg: CreateAzureRuntimeMessage =>
          azurePubsubHandler.createAndPollRuntime(msg).adaptError { case e =>
            PubsubHandleMessageError.AzureRuntimeCreationError(
              msg.runtimeId,
              msg.workspaceId,
              e.getMessage
            )
          }
        case msg: DeleteAzureRuntimeMessage =>
          azurePubsubHandler.deleteAndPollRuntime(msg)
      }
    } yield resp

  private[monitor] def messageHandler(event: Event[LeoPubsubMessage]): F[Unit] = {
    val traceId = event.traceId.getOrElse(TraceId("None"))
    val now = Instant.ofEpochMilli(com.google.protobuf.util.Timestamps.toMillis(event.publishedTime))
    implicit val appContext = Ask.const[F, AppContext](AppContext(traceId, now))
    val res = for {
      res <- messageResponder(event.msg)
        .timeout(config.timeout)
        .attempt // set timeout to 55 seconds because subscriber's ack deadline is 1 minute
      ctx <- appContext.ask
      _ <- res match {
        case Left(e) =>
          e match {
            case ee: PubsubHandleMessageError =>
              for {
                _ <- ee match {
                  case ee: PubsubKubernetesError =>
                    logger.error(ctx.loggingCtx, e)(
                      s"Encountered an error for app ${ee.appId}, ${ee.getMessage}"
                    ) >> handleKubernetesError(
                      ee
                    )
                  case ee: AzureRuntimeCreationError =>
                    azurePubsubHandler.handleAzureRuntimeCreationError(ee, now)
                  case _ => logger.error(ctx.loggingCtx, ee)(s"Failed to process pubsub message.")
                }
                _ <-
                  if (ee.isRetryable)
                    logger.error(ctx.loggingCtx, e)("Fail to process retryable pubsub message") >> F
                      .delay(event.consumer.nack())
                  else
                    logger.error(ctx.loggingCtx, e)("Fail to process non-retryable pubsub message") >> ack(event)
              } yield ()
            case ee: WorkbenchException if ee.getMessage.contains("Call to Google API failed") =>
              logger
                .error(ctx.loggingCtx, e)(
                  "Fail to process retryable pubsub message due to Google API call failure"
                ) >> F
                .delay(event.consumer.nack())
            case _ =>
              logger.error(ctx.loggingCtx, e)("Fail to process pubsub message due to unexpected error") >> ack(event)
          }
        case Right(_) => ack(event)
      }
    } yield ()

    res.handleErrorWith(e =>
      logger.error(e)("Fail to process pubsub message for some reason") >> F.delay(event.consumer.ack())
    )
  }

  val process: Stream[F, Unit] = subscriber.messages
    .parEvalMapUnordered(config.concurrency)(messageHandler)
    .handleErrorWith(error => Stream.eval(logger.error(error)("Failed to initialize message processor")))

  private def ack(event: Event[LeoPubsubMessage]): F[Unit] =
    for {
      _ <- logger.info(s"acking message: ${event}")
      _ <- F.delay(
        event.consumer.ack()
      )
      end <- F.realTimeInstant
      duration = (end.toEpochMilli - com.google.protobuf.util.Timestamps.toMillis(event.publishedTime)).millis
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
      metricsName = s"pubsub/ack/${event.msg.messageType.asString}"
      _ <- metrics.recordDuration(metricsName, duration, distributionBucket)
    } yield ()

  private[monitor] def handleCreateRuntimeMessage(msg: CreateRuntimeMessage)(implicit
    ev: Ask[F, AppContext]
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
        _ <- msg.runtimeConfig.cloudService.process(msg.runtimeId, RuntimeStatus.Creating).compile.drain
      } yield ()
      _ <- asyncTasks.offer(
        Task(
          ctx.traceId,
          taskToRun,
          Some(createRuntimeErrorHandler(msg.runtimeId, ctx.now)),
          ctx.now,
          "createRuntime"
        )
      )
    } yield ()

    createCluster.handleErrorWith(e => ev.ask.flatMap(ctx => createRuntimeErrorHandler(msg.runtimeId, ctx.now)(e)))
  }

  private[monitor] def handleDeleteRuntimeMessage(msg: DeleteRuntimeMessage)(implicit
    ev: Ask[F, AppContext]
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
          runtimeConfig.cloudService.process(runtime.id, RuntimeStatus.Deleting).compile.drain
      }
      fa = msg.persistentDiskToDelete.fold(poll) { id =>
        val deleteDisk = for {
          _ <- poll
          now <- nowInstant
          diskOpt <- persistentDiskQuery.getPersistentDiskRecord(id).transaction
          disk <- F.fromEither(diskOpt.toRight(new RuntimeException(s"disk not found for ${id}")))
          deleteDiskOp <- googleDiskService.deleteDisk(googleProject, disk.zone, disk.name)
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
      _ <- asyncTasks.offer(
        Task(
          ctx.traceId,
          fa,
          Some(handleRuntimeMessageError(runtime.id, ctx.now, s"deleting runtime ${runtime.projectNameString} failed")),
          ctx.now,
          "deleteRuntime"
        )
      )
    } yield ()

  private[monitor] def handleStopRuntimeMessage(msg: StopRuntimeMessage)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
      runtime <- runtimeOpt.fold(
        F.raiseError[Runtime](PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      )(F.pure)
      _ <-
        if (!Set(RuntimeStatus.Stopping, RuntimeStatus.PreStopping).contains(runtime.status))
          F.raiseError[Unit](
            PubsubHandleMessageError.ClusterInvalidState(msg.runtimeId, runtime.projectNameString, runtime, msg)
          )
        else F.unit
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      op <- runtimeConfig.cloudService.interpreter.stopRuntime(
        StopRuntimeParams(RuntimeAndRuntimeConfig(runtime, runtimeConfig), ctx.now, true)
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
          runtimeConfig.cloudService.process(runtime.id, RuntimeStatus.Stopping).compile.drain
      }
      _ <- asyncTasks.offer(
        Task(
          ctx.traceId,
          poll,
          Some(
            handleRuntimeMessageError(msg.runtimeId, ctx.now, s"stopping runtime ${runtime.projectNameString} failed")
          ),
          ctx.now,
          "stopRuntime"
        )
      )
    } yield ()

  private[monitor] def handleStartRuntimeMessage(msg: StartRuntimeMessage)(implicit
    ev: Ask[F, AppContext]
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
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      initBucket <- clusterQuery.getInitBucket(msg.runtimeId).transaction
      bucketName <- F.fromOption(initBucket.map(_.bucketName),
                                 new RuntimeException(s"init bucket not found for ${runtime.projectNameString} in DB")
      )
      _ <- runtimeConfig.cloudService.interpreter
        .startRuntime(StartRuntimeParams(RuntimeAndRuntimeConfig(runtime, runtimeConfig), bucketName))
      _ <- asyncTasks.offer(
        Task(
          ctx.traceId,
          runtimeConfig.cloudService.process(msg.runtimeId, RuntimeStatus.Starting).compile.drain,
          Some(
            handleRuntimeMessageError(msg.runtimeId, ctx.now, s"starting runtime ${runtime.projectNameString} failed")
          ),
          ctx.now,
          "startRuntime"
        )
      )
    } yield ()

  private[monitor] def handleUpdateRuntimeMessage(msg: UpdateRuntimeMessage)(implicit
    ev: Ask[F, AppContext]
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
                  runtimeConfig.cloudService.process(runtime.id, RuntimeStatus.Stopping).compile.drain
              }
              now <- nowInstant
              ctxStarting = Ask.const[F, AppContext](
                AppContext(ctx.traceId, now)
              )
              _ <- startAndUpdateRuntime(runtime, runtimeConfig, msg.newMachineType)(ctxStarting)
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
                "stopAndUpdateRuntime"
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
                    runtimeConfig.cloudService.process(runtime.id, RuntimeStatus.Updating).compile.drain,
                    Some(handleRuntimeMessageError(runtime.id, ctx.now, "updating runtime")),
                    ctx.now,
                    "updateRuntime"
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
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
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
      _ <- updatedRuntimeConfig.cloudService.process(runtime.id, RuntimeStatus.Starting).compile.drain
    } yield ()

  private[monitor] def handleCreateDiskMessage(msg: CreateDiskMessage)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    createDisk(msg, None, false)

  // this returns an F[F[Unit]. It kicks off the google operation, and then return an F containing the async polling task
  private[monitor] def createDisk(msg: CreateDiskMessage, formattedBy: Option[FormattedBy], sync: Boolean)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] = {
    val create = for {
      ctx <- ev.ask
      operationFutureOpt <- googleDiskService
        .createDisk(
          msg.googleProject,
          msg.zone,
          Disk
            .newBuilder()
            .setName(msg.name.value)
            .setSizeGb(msg.size.gb)
            .setZone(msg.zone.value)
            .setType(msg.diskType.googleString(msg.googleProject, msg.zone))
            .setPhysicalBlockSizeBytes(msg.blockSize.bytes)
            .putAllLabels(Map("leonardo" -> "true").asJava)
            .build()
        )
      _ <- operationFutureOpt match {
        case None => F.unit
        case Some(v) =>
          val task = for {
            _ <- F.blocking(v.get())
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
            asyncTasks.offer(
              Task(ctx.traceId,
                   task,
                   Some(logError(s"${ctx.traceId.asString} | ${msg.diskId.value}", "Creating Disk")),
                   ctx.now,
                   "createDisk"
              )
            )
          }
      }
    } yield ()

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
      operationFutureOpt <- googleDiskService.createDisk(
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
      opFutureOpt <- googleDiskService.deleteDisk(googleProject, disk.zone, disk.name)
      _ <- opFutureOpt match {
        case None => F.unit
        case Some(v) =>
          val task = for {
            operation <- F.blocking(v.get())
            _ <- F.raiseUnless(isSuccess(operation.getHttpErrorStatusCode))(
              new RuntimeException(s"fail to delete disk ${googleProject}/${disk.name} due to ${operation}")
            )
            _ <- persistentDiskQuery.delete(diskId, ctx.now).transaction[F].void >> authProvider.notifyResourceDeleted(
              disk.samResource,
              disk.auditInfo.creator,
              googleProject
            )
          } yield ()
          if (sync) task
          else {
            asyncTasks.offer(
              Task(ctx.traceId,
                   task,
                   Some(logError(s"${ctx.traceId.asString} | ${diskId.value}", "Deleting Disk")),
                   ctx.now,
                   "deleteDisk"
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
      opFuture <- googleDiskService.resizeDisk(googleProject, disk.zone, disk.name, msg.newSize.gb)

      task = for {
        operation <- F.blocking(opFuture.get())
        _ <- F.raiseUnless(isSuccess(operation.getHttpErrorStatusCode))(
          new RuntimeException(s"fail to resize disk ${googleProject}/${disk.name} due to ${operation}")
        )
        now <- nowInstant
        _ <- persistentDiskQuery.updateSize(msg.diskId, msg.newSize, now).transaction[F]
      } yield ()
      _ <- asyncTasks.offer(
        Task(ctx.traceId,
             task,
             Some(logError(s"${ctx.traceId.asString} | ${msg.diskId.value}", "Updating Disk")),
             ctx.now,
             "updateDisk"
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

      createClusterOrNodepoolOp <- msg.clusterNodepoolAction match {
        case Some(ClusterNodepoolAction.CreateClusterAndNodepool(clusterId, defaultNodepoolId, nodepoolId)) =>
          for {
            // initial the createCluster call synchronously
            createClusterResultOpt <- gkeAlg
              .createCluster(
                CreateClusterParams(clusterId, msg.project, List(defaultNodepoolId, nodepoolId))
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
                  None
                )
              }
            // monitor cluster creation asynchronously
            monitorOp = createClusterResultOpt.traverse_(createClusterResult =>
              gkeAlg
                .pollCluster(PollClusterParams(clusterId, msg.project, createClusterResult))
                .adaptError { case e =>
                  PubsubKubernetesError(
                    AppError(e.getMessage,
                             ctx.now,
                             ErrorAction.CreateApp,
                             ErrorSource.Cluster,
                             None,
                             Some(ctx.traceId)
                    ),
                    Some(msg.appId),
                    false,
                    // We leave cluster id and default nodepool id as none here because we want the status to stay as DELETED and not transition to ERROR.
                    // The app will have the error so the user can see it, delete their app, and try again
                    None,
                    None
                  )
                }
            )
          } yield monitorOp

        case Some(ClusterNodepoolAction.CreateNodepool(nodepoolId)) =>
          // create nodepool asynchronously
          F.pure(
            gkeAlg
              .createAndPollNodepool(CreateNodepoolParams(nodepoolId, msg.project))
              .adaptError { case e =>
                PubsubKubernetesError(
                  AppError(e.getMessage, ctx.now, ErrorAction.CreateApp, ErrorSource.Nodepool, None, Some(ctx.traceId)),
                  Some(msg.appId),
                  false,
                  Some(nodepoolId),
                  None
                )
              }
          )
        case None => F.pure(F.unit)
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
              None
            )
          }
        )
        .void

      // create second Galaxy disk asynchronously
      createSecondDiskOp =
        if (msg.appType == Galaxy && disk.isDefined) {
          val d = disk.get // it's safe to do `.get` here because we've verified
          for {
            res <- createGalaxyPostgresDiskOnlyInGoogle(msg.project, ZoneName("us-central1-a"), msg.appName, d.name)
              .adaptError { case e =>
                PubsubKubernetesError(
                  AppError(e.getMessage, ctx.now, ErrorAction.CreateApp, ErrorSource.Disk, None, Some(ctx.traceId)),
                  Some(msg.appId),
                  false,
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
        _ <- gkeAlg
          .createAndPollApp(CreateAppParams(msg.appId, msg.project, msg.appName, msg.machineType))
          .onError { case e =>
            cleanUpAfterCreateAppError(msg.appId, msg.appName, msg.project, msg.createDisk, e)
          }
          .adaptError { case e =>
            PubsubKubernetesError(
              AppError(e.getMessage, ctx.now, ErrorAction.CreateApp, ErrorSource.App, None, Some(ctx.traceId)),
              Some(msg.appId),
              false,
              None,
              None
            )
          }
      } yield ()

      _ <- asyncTasks.offer(
        Task(ctx.traceId, task, Some(handleKubernetesError), ctx.now, "createApp")
      )
    } yield ()

  private[monitor] def handleDeleteAppMessage(msg: DeleteAppMessage)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    deleteApp(msg, false, false)

  private[monitor] def deleteApp(msg: DeleteAppMessage, sync: Boolean, errorAfterDelete: Boolean)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask

      dbAppOpt <- KubernetesServiceDbQueries.getFullAppByName(msg.project, msg.appId).transaction
      dbApp <- F.fromOption(dbAppOpt, AppNotFoundException(msg.project, msg.appName, ctx.traceId))
      zone = ZoneName("us-central1-a")

      getPostgresDiskOp = dbApp.app.appResources.disk.flatTraverse { d =>
        for {
          postgresDiskOpt <- googleDiskService
            .getDisk(
              msg.project,
              zone,
              getGalaxyPostgresDiskName(d.name, config.galaxyDiskConfig.postgresDiskNameSuffix)
            )
          res <- postgresDiskOpt match {
            case Some(disk) => F.pure(disk.some)
            case None =>
              googleDiskService.getDisk(
                msg.project,
                zone,
                getOldStyleGalaxyPostgresDiskName(dbApp.app.appResources.namespace.name,
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
          googleDiskService
            .getDisk(
              msg.project,
              zone,
              d.name
            )
            .map(_.map(_.getLastDetachTimestamp))
        }
        _ <- gkeAlg
          .deleteAndPollApp(DeleteAppParams(msg.appId, msg.project, msg.appName, errorAfterDelete))
          .adaptError { case e =>
            PubsubKubernetesError(
              AppError(e.getMessage, ctx.now, ErrorAction.DeleteApp, ErrorSource.App, None, Some(ctx.traceId)),
              Some(msg.appId),
              false,
              None,
              None
            )
          }

        // detach/delete disk when we need to delete disk
        _ <- msg.diskId.traverse_ { diskId =>
          // we now use the detach timestamp recorded prior to helm uninstall so we can observe when galaxy actually 'detaches' the disk from google's perspective
          val getPostgresDisk = getPostgresDiskOp
          val getDataDisk = dbApp.app.appResources.disk.flatTraverse { d =>
            googleDiskService
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
                None
              )
            }
            deleteDataDisk = deleteDisk(diskId, true).adaptError { case e =>
              PubsubKubernetesError(
                AppError(e.getMessage, ctx.now, ErrorAction.DeleteApp, ErrorSource.Disk, None, Some(ctx.traceId)),
                Some(msg.appId),
                false,
                None,
                None
              )
            }

            deletePostgresDisk =
              if (dbApp.app.appType == AppType.Galaxy)
                deleteGalaxyPostgresDiskOnlyInGoogle(msg.project,
                                                     zone,
                                                     msg.appName,
                                                     dbApp.app.appResources.namespace.name,
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
                      None
                    )
                  }
              else F.unit

            _ <- List(deleteDataDisk, deletePostgresDisk).parSequence_
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

      _ <-
        if (sync) task
        else
          asyncTasks.offer(
            Task(ctx.traceId, task, Some(handleKubernetesError), ctx.now, "deleteApp")
          )
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
      _ <- gkeAlg.deleteAndPollCluster(DeleteClusterParams(clusterId, project)).handleErrorWith { e =>
        // we do not want to bubble up errors with cluster clean-up
        logger.error(ctx.loggingCtx, e)(
          s"An error occurred during resource clean up for cluster ${clusterId} in project ${project}"
        )
      }
    } yield ()

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
      appOpt <- KubernetesServiceDbQueries.getFullAppByName(project, appId).transaction
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
      stopApp = gkeAlg
        .stopAndPollApp(StopAppParams(msg.appId, msg.appName, msg.project))
        .adaptError { case e =>
          PubsubKubernetesError(
            AppError(e.getMessage, ctx.now, ErrorAction.StopApp, ErrorSource.App, None, Some(ctx.traceId)),
            Some(msg.appId),
            false,
            None,
            None
          )
        }
      _ <- asyncTasks.offer(Task(ctx.traceId, stopApp, Some(handleKubernetesError), ctx.now, "stopApp"))
    } yield ()

  private[monitor] def handleStartAppMessage(
    msg: StartAppMessage
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      startApp = gkeAlg
        .startAndPollApp(StartAppParams(msg.appId, msg.appName, msg.project))
        .adaptError { case e =>
          PubsubKubernetesError(
            AppError(e.getMessage, ctx.now, ErrorAction.StartApp, ErrorSource.App, None, Some(ctx.traceId)),
            Some(msg.appId),
            false,
            None,
            None
          )
        }

      _ <- asyncTasks.offer(Task(ctx.traceId, startApp, Some(handleKubernetesError), ctx.now, "startApp"))
    } yield ()

  private def handleKubernetesError(e: Throwable)(implicit ev: Ask[F, AppContext]): F[Unit] = ev.ask.flatMap { ctx =>
    e match {
      case e: PubsubKubernetesError =>
        for {
          _ <- e.appId.traverse(id => appErrorQuery.save(id, e.dbError).transaction)
          _ <- e.appId.traverse(id => appQuery.markAsErrored(id).transaction)
          _ <- e.clusterId.traverse(clusterId =>
            kubernetesClusterQuery.updateStatus(clusterId, KubernetesClusterStatus.Error).transaction
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

  private[monitor] def createRuntimeErrorHandler(runtimeId: Long, now: Instant)(
    e: Throwable
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- logger.error(ctx.loggingCtx, e)(s"Failed to create runtime ${runtimeId}")
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
        (clusterErrorQuery.save(runtimeId, RuntimeError(m.take(1024), None, now)) >>
          clusterQuery.updateClusterStatus(runtimeId, RuntimeStatus.Error, now)).transaction[F]
      )
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
      postgresDiskOpt <- googleDiskService
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
            operationFutureOpt <- googleDiskService
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
}
