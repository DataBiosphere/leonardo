package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant
import java.util.concurrent.TimeoutException

import _root_.io.chrisdavenport.log4cats.StructuredLogger
import cats.Parallel
import cats.effect.implicits._
import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.Disk
import fs2.Stream
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.errorReporting.ErrorReporting
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.google2.{
  ComputePollOperation,
  DiskName,
  Event,
  GoogleDiskService,
  GoogleSubscriber,
  MachineTypeName,
  OperationName
}
import org.broadinstitute.dsde.workbench.errorReporting.ReportWorthySyntax._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.{cloudServiceSyntax, _}
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, LeoException}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.google.{GcsObjectName, GcsPath}
import org.broadinstitute.dsde.workbench.model.{ErrorReport, TraceId, WorkbenchException}
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppNotFoundException

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class LeoPubsubMessageSubscriber[F[_]: Timer: ContextShift](
  config: LeoPubsubMessageSubscriberConfig,
  subscriber: GoogleSubscriber[F, LeoPubsubMessage],
  asyncTasks: InspectableQueue[F, Task[F]],
  googleDiskService: GoogleDiskService[F],
  computePollOperation: ComputePollOperation[F],
  authProvider: LeoAuthProvider[F],
  gkeInterp: GKEInterpreter[F],
  errorReporting: ErrorReporting[F]
)(implicit executionContext: ExecutionContext,
  F: ConcurrentEffect[F],
  parallel: Parallel[F],
  logger: StructuredLogger[F],
  dbRef: DbReference[F],
  runtimeInstances: RuntimeInstances[F],
  monitor: RuntimeMonitor[F, CloudService]) {

  private[monitor] def messageResponder(
    message: LeoPubsubMessage
  )(implicit traceId: ApplicativeAsk[F, AppContext]): F[Unit] =
    message match {
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
      case msg: BatchNodepoolCreateMessage =>
        handleBatchNodepoolCreateMessage(msg)
    }

  private[monitor] def messageHandler(event: Event[LeoPubsubMessage]): F[Unit] = {
    val traceId = event.traceId.getOrElse(TraceId("None"))
    val now = Instant.ofEpochMilli(com.google.protobuf.util.Timestamps.toMillis(event.publishedTime))
    implicit val appContext = ApplicativeAsk.const[F, AppContext](AppContext(traceId, now))
    val res = for {
      res <- messageResponder(event.msg)
        .timeout(config.timeout)
        .attempt // set timeout to 55 seconds because subscriber's ack deadline is 1 minute
      _ <- res match {
        case Left(e) =>
          e match {
            case ee: PubsubHandleMessageError =>
              for {
                _ <- ee match {
                  case ee: PubsubKubernetesError =>
                    handleKubernetesError(ee)
                  case _ => F.unit
                }
                _ <- if (ee.isRetryable)
                  logger.error(e)("Fail to process retryable pubsub message") >> F
                    .delay(event.consumer.nack())
                else
                  logger.error(e)("Fail to process non-retryable pubsub message") >> ack(event)
              } yield ()
            case ee: WorkbenchException if ee.getMessage.contains("Call to Google API failed") =>
              logger
                .error(e)("Fail to process retryable pubsub message due to Google API call failure") >> F
                .delay(event.consumer.nack())
            //persist and log
            case _ =>
              logger.error(e)("Fail to process non-retryable pubsub message") >> ack(event)
          }
        case Right(_) => ack(event)
      }
    } yield ()

    res.handleErrorWith(e => logger.error(e)("Fail to process pubsub message") >> F.delay(event.consumer.ack()))
  }

  val process: Stream[F, Unit] = subscriber.messages
    .parEvalMapUnordered(config.concurrency)(messageHandler)
    .handleErrorWith(error => Stream.eval(logger.error(error)("Failed to initialize message processor")))

  private def ack(event: Event[LeoPubsubMessage]): F[Unit] =
    logger.info(s"acking message: ${event}") >> F.delay(
      event.consumer.ack()
    )

  private[monitor] def handleCreateRuntimeMessage(msg: CreateRuntimeMessage)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] = {
    val createCluster = for {
      ctx <- ev.ask
      clusterResult <- msg.runtimeConfig.cloudService.interpreter
        .createRuntime(CreateRuntimeParams.fromCreateRuntimeMessage(msg))
      updateAsyncClusterCreationFields = UpdateAsyncClusterCreationFields(
        Some(GcsPath(clusterResult.initBucket, GcsObjectName(""))),
        clusterResult.serviceAccountKey,
        msg.runtimeId,
        Some(clusterResult.asyncRuntimeFields),
        ctx.now
      )
      // Save the VM image and async fields in the database
      clusterImage = RuntimeImage(RuntimeImageType.VM, clusterResult.customImage.asString, ctx.now)
      _ <- (clusterQuery.updateAsyncClusterCreationFields(updateAsyncClusterCreationFields) >> clusterImageQuery.save(
        msg.runtimeId,
        clusterImage
      )).transaction
      taskToRun = for {
        _ <- msg.runtimeConfig.cloudService.process(msg.runtimeId, RuntimeStatus.Creating).compile.drain
        _ <- if (msg.stopAfterCreation) { //TODO: once we remove legacy /api/clusters route or we remove `stopAfterCreation` support, we can remove this block
          for {
            _ <- clusterQuery
              .updateClusterStatus(msg.runtimeId, RuntimeStatus.Stopping, ctx.now)
              .transaction
            runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
            runtime <- F.fromEither(runtimeOpt.toRight(new Exception(s"can't find ${msg.runtimeId} in DB")))
            dataprocConfig = msg.runtimeConfig match {
              case x: RuntimeConfigInCreateRuntimeMessage.DataprocConfig =>
                Some(dataprocInCreateRuntimeMsgToDataprocRuntime(x))
              case _ => none[RuntimeConfig.DataprocConfig]
            }
            _ <- msg.runtimeConfig.cloudService.interpreter
              .stopRuntime(StopRuntimeParams(runtime, dataprocConfig, ctx.now))
            _ <- msg.runtimeConfig.cloudService.process(msg.runtimeId, RuntimeStatus.Stopping).compile.drain
          } yield ()
        } else F.unit
      } yield ()
      _ <- asyncTasks.enqueue1(
        Task(
          ctx.traceId,
          taskToRun,
          Some(createRuntimeErrorHandler(msg, ctx.now)),
          ctx.now
        )
      )
    } yield ()

    createCluster.handleErrorWith(e => ev.ask.flatMap(ctx => createRuntimeErrorHandler(msg, ctx.now)(e)))
  }

  private[monitor] def handleDeleteRuntimeMessage(msg: DeleteRuntimeMessage)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
      runtime <- runtimeOpt.fold(
        F.raiseError[Runtime](PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      )(F.pure)
      _ <- if (!Set(RuntimeStatus.Deleting, RuntimeStatus.PreDeleting).contains(runtime.status))
        F.raiseError[Unit](
          PubsubHandleMessageError.ClusterInvalidState(msg.runtimeId, runtime.projectNameString, runtime, msg)
        )
      else F.unit
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      op <- runtimeConfig.cloudService.interpreter.deleteRuntime(
        DeleteRuntimeParams(runtime.googleProject, runtime.runtimeName, runtime.asyncRuntimeFields.isDefined)
      )
      poll = op match {
        case Some(o) =>
          runtimeConfig.cloudService.pollCheck(runtime.googleProject,
                                               RuntimeAndRuntimeConfig(runtime, runtimeConfig),
                                               o,
                                               RuntimeStatus.Deleting)
        case None =>
          runtimeConfig.cloudService.process(runtime.id, RuntimeStatus.Deleting).compile.drain
      }
      fa = msg.persistentDiskToDelete.fold(poll) { id =>
        val deleteDisk = for {
          _ <- poll
          now <- nowInstant
          diskOpt <- persistentDiskQuery.getPersistentDiskRecord(id).transaction
          disk <- F.fromEither(diskOpt.toRight(new RuntimeException(s"disk not found for ${id}")))

          deleteDiskOp <- googleDiskService.deleteDisk(runtime.googleProject, disk.zone, disk.name)
          whenDone = persistentDiskQuery.delete(id, now).transaction.void >> authProvider.notifyResourceDeleted(
            disk.samResource,
            disk.creator,
            disk.googleProject
          )
          whenTimeout = F.raiseError[Unit](
            new RuntimeException(s"Fail to delete ${disk.name} in a timely manner")
          )
          whenInterrupted = F.unit
          _ <- computePollOperation.pollZoneOperation(runtime.googleProject,
                                                      disk.zone,
                                                      OperationName(deleteDiskOp.getName),
                                                      2 seconds,
                                                      10,
                                                      None)(whenDone, whenTimeout, whenInterrupted)
        } yield ()

        deleteDisk.handleErrorWith(e =>
          clusterErrorQuery
            .save(runtime.id, RuntimeError(e.getMessage, -1, ctx.now))
            .transaction
            .void
        )
      }
      _ <- asyncTasks.enqueue1(
        Task(
          ctx.traceId,
          fa,
          Some(handleRuntimeMessageError(runtime.id, ctx.now, s"deleting runtime ${runtime.projectNameString} failed")),
          ctx.now
        )
      )
    } yield ()

  private[monitor] def handleStopRuntimeMessage(msg: StopRuntimeMessage)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
      runtime <- runtimeOpt.fold(
        F.raiseError[Runtime](PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      )(F.pure)
      _ <- if (!Set(RuntimeStatus.Stopping, RuntimeStatus.PreStopping).contains(runtime.status))
        F.raiseError[Unit](
          PubsubHandleMessageError.ClusterInvalidState(msg.runtimeId, runtime.projectNameString, runtime, msg)
        )
      else F.unit
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      op <- runtimeConfig.cloudService.interpreter.stopRuntime(
        StopRuntimeParams(runtime, LeoLenses.dataprocPrism.getOption(runtimeConfig), ctx.now)
      )
      poll = op match {
        case Some(o) =>
          runtimeConfig.cloudService.pollCheck(runtime.googleProject,
                                               RuntimeAndRuntimeConfig(runtime, runtimeConfig),
                                               o,
                                               RuntimeStatus.Stopping)
        case None =>
          runtimeConfig.cloudService.process(runtime.id, RuntimeStatus.Stopping).compile.drain
      }
      _ <- asyncTasks.enqueue1(
        Task(
          ctx.traceId,
          poll,
          Some(
            handleRuntimeMessageError(msg.runtimeId, ctx.now, s"stopping runtime ${runtime.projectNameString} failed")
          ),
          ctx.now
        )
      )
    } yield ()

  private[monitor] def handleStartRuntimeMessage(msg: StartRuntimeMessage)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
      runtime <- runtimeOpt.fold(
        F.raiseError[Runtime](PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      )(F.pure)
      _ <- if (!Set(RuntimeStatus.Starting, RuntimeStatus.PreStarting).contains(runtime.status))
        F.raiseError[Unit](
          PubsubHandleMessageError.ClusterInvalidState(msg.runtimeId, runtime.projectNameString, runtime, msg)
        )
      else F.unit
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      initBucket <- clusterQuery.getInitBucket(msg.runtimeId).transaction
      bucketName <- F.fromOption(initBucket.map(_.bucketName),
                                 new RuntimeException(s"init bucket not found for ${runtime.projectNameString} in DB"))
      _ <- runtimeConfig.cloudService.interpreter.startRuntime(StartRuntimeParams(runtime, bucketName))
      _ <- asyncTasks.enqueue1(
        Task(
          ctx.traceId,
          runtimeConfig.cloudService.process(msg.runtimeId, RuntimeStatus.Starting).compile.drain,
          Some(
            handleRuntimeMessageError(msg.runtimeId, ctx.now, s"starting runtime ${runtime.projectNameString} failed")
          ),
          ctx.now
        )
      )
    } yield ()

  private[monitor] def handleUpdateRuntimeMessage(msg: UpdateRuntimeMessage)(
    implicit ev: ApplicativeAsk[F, AppContext]
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
      hasResizedCluster <- if (msg.newNumWorkers.isDefined || msg.newNumPreemptibles.isDefined) {
        for {
          _ <- runtimeConfig.cloudService.interpreter
            .resizeCluster(ResizeClusterParams(runtime, msg.newNumWorkers, msg.newNumPreemptibles))
          _ <- msg.newNumWorkers.traverse_(a =>
            RuntimeConfigQueries.updateNumberOfWorkers(runtime.runtimeConfigId, a, ctx.now).transaction
          )
          _ <- msg.newNumPreemptibles.traverse_(a =>
            RuntimeConfigQueries.updateNumberOfPreemptibleWorkers(runtime.runtimeConfigId, Some(a), ctx.now).transaction
          )
          _ <- clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.Updating, ctx.now).transaction.void
        } yield true
      } else F.pure(false)

      // Update the disk size
      _ <- msg.diskUpdate
        .traverse { d =>
          val updateDiskSize = d match {
            case DiskUpdate.PdSizeUpdate(_, diskName, targetSize) =>
              UpdateDiskSizeParams.Gce(runtime.googleProject, diskName, targetSize)
            case DiskUpdate.NoPdSizeUpdate(targetSize) =>
              UpdateDiskSizeParams.Gce(
                runtime.googleProject,
                DiskName(
                  s"${runtime.runtimeName.asString}-1"
                ), // user disk's diskname is always postfixed with -1 for non-pd runtimes
                targetSize
              )
            case DiskUpdate.Dataproc(size, masterInstance) =>
              UpdateDiskSizeParams.Dataproc(size, masterInstance)
          }
          for {
            _ <- runtimeConfig.cloudService.interpreter.updateDiskSize(updateDiskSize)
            _ <- LeoLenses.pdSizeUpdatePrism
              .getOption(d)
              .fold(
                RuntimeConfigQueries.updateDiskSize(runtime.runtimeConfigId, d.newDiskSize, ctx.now).transaction
              )(dd => persistentDiskQuery.updateSize(dd.diskId, dd.newDiskSize, ctx.now).transaction)
          } yield ()
        }

      _ <- if (msg.stopToUpdateMachineType) {
        for {
          timeToStop <- nowInstant
          ctxStopping = ApplicativeAsk.const[F, AppContext](
            AppContext(ctx.traceId, timeToStop)
          )
          _ <- dbRef.inTransaction(clusterQuery.updateClusterStatus(msg.runtimeId, RuntimeStatus.Stopping, ctx.now))
          operation <- runtimeConfig.cloudService.interpreter
            .stopRuntime(StopRuntimeParams(runtime, LeoLenses.dataprocPrism.getOption(runtimeConfig), ctx.now))(
              ctxStopping
            )
          task = for {
            _ <- operation match {
              case Some(op) =>
                runtimeConfig.cloudService.pollCheck(runtime.googleProject,
                                                     RuntimeAndRuntimeConfig(runtime, runtimeConfig),
                                                     op,
                                                     RuntimeStatus.Stopping)
              case None =>
                runtimeConfig.cloudService.process(runtime.id, RuntimeStatus.Stopping).compile.drain
            }
            now <- nowInstant
            ctxStarting = ApplicativeAsk.const[F, AppContext](
              AppContext(ctx.traceId, now)
            )
            _ <- startAndUpdateRuntime(runtime, msg.newMachineType)(ctxStarting)
          } yield ()
          _ <- asyncTasks.enqueue1(
            Task(ctx.traceId,
                 task,
                 Some(
                   handleRuntimeMessageError(msg.runtimeId,
                                             ctx.now,
                                             s"updating runtime ${runtime.projectNameString} failed")
                 ),
                 ctx.now)
          )
        } yield ()
      } else {
        for {
          _ <- msg.newMachineType.traverse_(m =>
            runtimeConfig.cloudService.interpreter.updateMachineType(UpdateMachineTypeParams(runtime, m, ctx.now))
          )
          _ <- if (hasResizedCluster) {
            asyncTasks.enqueue1(
              Task(
                ctx.traceId,
                runtimeConfig.cloudService.process(runtime.id, RuntimeStatus.Updating).compile.drain,
                Some(handleRuntimeMessageError(runtime.id, ctx.now, "updating runtime")),
                ctx.now
              )
            )
          } else F.unit
        } yield ()
      }
    } yield ()

  private def startAndUpdateRuntime(
    runtime: Runtime,
    targetMachineType: Option[MachineTypeName]
  )(implicit ev: ApplicativeAsk[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      _ <- targetMachineType.traverse(m =>
        runtimeConfig.cloudService.interpreter
          .updateMachineType(UpdateMachineTypeParams(runtime, m, ctx.now))
      )
      initBucket <- clusterQuery.getInitBucket(runtime.id).transaction
      bucketName <- F.fromOption(initBucket.map(_.bucketName),
                                 new RuntimeException(s"init bucket not found for ${runtime.projectNameString} in DB"))
      _ <- runtimeConfig.cloudService.interpreter.startRuntime(StartRuntimeParams(runtime, bucketName))
      _ <- dbRef.inTransaction {
        clusterQuery.updateClusterStatus(
          runtime.id,
          RuntimeStatus.Starting,
          ctx.now
        )
      }
      _ <- patchQuery.updatePatchAsComplete(runtime.id).transaction.void
      _ <- runtimeConfig.cloudService.process(runtime.id, RuntimeStatus.Starting).compile.drain
    } yield ()

  private[monitor] def handleCreateDiskMessage(msg: CreateDiskMessage)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    createDisk(msg, false)

  //this returns an F[F[Unit]. It kicks off the google operation, and then return an F containing the async polling task
  private[monitor] def createDisk(msg: CreateDiskMessage, sync: Boolean)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] = {
    val create = for {
      ctx <- ev.ask
      operation <- googleDiskService
        .createDisk(
          msg.googleProject,
          msg.zone,
          Disk
            .newBuilder()
            .setName(msg.name.value)
            .setSizeGb(msg.size.gb.toString)
            .setZone(msg.zone.value)
            .setType(msg.diskType.googleString(msg.googleProject, msg.zone))
            .setPhysicalBlockSizeBytes(msg.blockSize.bytes.toString)
            .build()
        )
      _ <- persistentDiskQuery.updateGoogleId(msg.diskId, GoogleId(operation.getTargetId), ctx.now).transaction[F]
      task = computePollOperation
        .pollZoneOperation(
          msg.googleProject,
          msg.zone,
          OperationName(operation.getName),
          config.persistentDiskMonitorConfig.create.interval,
          config.persistentDiskMonitorConfig.create.maxAttempts,
          None
        )(
          persistentDiskQuery.updateStatus(msg.diskId, DiskStatus.Ready, ctx.now).transaction[F].void,
          F.raiseError(
            new TimeoutException(s"Fail to create disk ${msg.name.value} in a timely manner")
          ), //Should save disk creation error if we have error column in DB
          F.unit
        )
      _ <- if (sync) task
      else {
        asyncTasks.enqueue1(
          Task(ctx.traceId,
               task,
               Some(logError(s"${ctx.traceId.asString} | ${msg.diskId.value}", "Creatiing Disk")),
               ctx.now)
        )
      }
    } yield ()

    create.onError {
      case e =>
        for {
          ctx <- ev.ask
          _ <- logger.error(e)(
            s"Failed to create disk ${msg.name.value} in Google project ${msg.googleProject.value}"
          )
          _ <- persistentDiskQuery.updateStatus(msg.diskId, DiskStatus.Failed, ctx.now).transaction[F]
        } yield ()
    }
  }

  private[monitor] def handleDeleteDiskMessage(msg: DeleteDiskMessage)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    deleteDisk(msg.diskId, false)

  private[monitor] def deleteDisk(diskId: DiskId, sync: Boolean)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      diskOpt <- persistentDiskQuery.getById(diskId).transaction
      disk <- diskOpt.fold(
        F.raiseError[PersistentDisk](PubsubHandleMessageError.DiskNotFound(diskId))
      )(F.pure)
      _ <- if (disk.status != DiskStatus.Deleting)
        F.raiseError[Unit](
          PubsubHandleMessageError.DiskInvalidState(diskId, disk.projectNameString, disk)
        )
      else F.unit
      operation <- googleDiskService.deleteDisk(disk.googleProject, disk.zone, disk.name)
      whenDone = persistentDiskQuery.delete(diskId, ctx.now).transaction[F].void >> authProvider.notifyResourceDeleted(
        disk.samResource,
        disk.auditInfo.creator,
        disk.googleProject
      )
      whenTimeout = F.raiseError[Unit](
        new TimeoutException(s"Fail to delete disk ${disk.name.value} in a timely manner")
      )
      whenInterrupted = F.unit
      task = computePollOperation
        .pollZoneOperation(
          disk.googleProject,
          disk.zone,
          OperationName(operation.getName),
          config.persistentDiskMonitorConfig.create.interval,
          config.persistentDiskMonitorConfig.create.maxAttempts,
          None
        )(whenDone, whenTimeout, whenInterrupted)
      _ <- if (sync) task
      else {
        asyncTasks.enqueue1(
          Task(ctx.traceId,
               task,
               Some(logError(s"${ctx.traceId.asString} | ${diskId.value}", "Deleting Disk")),
               ctx.now)
        )
      }
    } yield ()

  private[monitor] def handleUpdateDiskMessage(msg: UpdateDiskMessage)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      diskOpt <- persistentDiskQuery.getById(msg.diskId).transaction
      disk <- diskOpt.fold(
        F.raiseError[PersistentDisk](PubsubHandleMessageError.DiskNotFound(msg.diskId))
      )(F.pure)
      operation <- googleDiskService.resizeDisk(disk.googleProject, disk.zone, disk.name, msg.newSize.gb)
      task = computePollOperation
        .pollZoneOperation(
          disk.googleProject,
          disk.zone,
          OperationName(operation.getName),
          config.persistentDiskMonitorConfig.create.interval,
          config.persistentDiskMonitorConfig.create.maxAttempts,
          None
        )(
          for {
            now <- nowInstant
            _ <- persistentDiskQuery.updateSize(msg.diskId, msg.newSize, now).transaction[F]
          } yield (),
          F.raiseError(
            new TimeoutException(s"Fail to update disk ${disk.name.value} in a timely manner")
          ), //Should save disk creation error if we have error column in DB
          F.unit
        )
      _ <- asyncTasks.enqueue1(
        Task(ctx.traceId,
             task,
             Some(logError(s"${ctx.traceId.asString} | ${msg.diskId.value}", "Updating Disk")),
             ctx.now)
      )
    } yield ()

  def handleCreateAppMessage(msg: CreateAppMessage)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask

      initialGoogleCall = msg.cluster
        .traverse(createClusterMessage =>
          gkeInterp.createCluster(createClusterMessage.clusterId, List(createClusterMessage.defaultNodepoolId), false)
        )
      clusterResult <- initialGoogleCall.adaptError {
        case e =>
          PubsubKubernetesError(
            AppError(e.getMessage(), ctx.now, ErrorAction.CreateGalaxyApp, ErrorSource.Cluster, None),
            Some(msg.appId),
            false,
            msg.cluster.map(_.defaultNodepoolId),
            msg.cluster.map(_.clusterId)
          )
      }

      createClusterAndNodepool = for {
        _ <- clusterResult.traverse(result => gkeInterp.pollCluster(result)).adaptError {
          case e =>
            PubsubKubernetesError(
              AppError(e.getMessage(), ctx.now, ErrorAction.CreateGalaxyApp, ErrorSource.Cluster, None),
              Some(msg.appId),
              false,
              msg.cluster.map(_.defaultNodepoolId),
              msg.cluster.map(_.clusterId)
            )
        }

        _ <- msg.nodepoolId.traverse { nodepoolId =>
          gkeInterp.createAndPollNodepool(nodepoolId, msg.appId).adaptError {
            case e =>
              PubsubKubernetesError(
                AppError(e.getMessage(), ctx.now, ErrorAction.CreateGalaxyApp, ErrorSource.Nodepool, None),
                Some(msg.appId),
                false,
                Some(nodepoolId),
                None
              )
          }
        }
      } yield ()

      diskCreation = createDiskForApp(msg).adaptError {
        case e =>
          PubsubKubernetesError(
            AppError(e.getMessage(), ctx.now, ErrorAction.CreateGalaxyApp, ErrorSource.Disk, None),
            Some(msg.appId),
            false,
            None,
            None
          )
      }
      parPreAppCreationSetup = List(createClusterAndNodepool, diskCreation).parSequence.flatMap(_ => F.unit)

      task = for {
        _ <- parPreAppCreationSetup
        _ <- gkeInterp.createAndPollApp(msg).adaptError {
          case e =>
            PubsubKubernetesError(
              AppError(e.getMessage(), ctx.now, ErrorAction.CreateGalaxyApp, ErrorSource.App, None),
              Some(msg.appId),
              false,
              None,
              None
            )
        }
      } yield ()

      _ <- asyncTasks.enqueue1(
        Task(ctx.traceId, task, Some(handleKubernetesError), ctx.now)
      )
    } yield ()

  def handleBatchNodepoolCreateMessage(msg: BatchNodepoolCreateMessage)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      initialGoogleCall = gkeInterp.createCluster(msg.clusterId, msg.nodepools, true)
      clusterResult <- initialGoogleCall.adaptError {
        case e =>
          PubsubKubernetesError(
            AppError(e.getMessage(), ctx.now, ErrorAction.CreateGalaxyApp, ErrorSource.Cluster, None),
            None,
            false,
            None,
            Some(msg.clusterId)
          )
      }

      task = gkeInterp.pollCluster(clusterResult).adaptError {
        case e =>
          PubsubKubernetesError(
            AppError(e.getMessage(), ctx.now, ErrorAction.CreateGalaxyApp, ErrorSource.Cluster, None),
            None,
            false,
            None,
            Some(msg.clusterId)
          )
      }

      _ <- asyncTasks.enqueue1(
        Task(ctx.traceId, task, Some(handleKubernetesError), ctx.now)
      )
    } yield ()

  private def handleKubernetesError(e: Throwable): F[Unit] =
    e match {
      case e: PubsubKubernetesError =>
        for {
          _ <- e.appId.traverse(id => appErrorQuery.save(id, e.dbError).transaction)
          _ <- e.appId.traverse(id => appQuery.updateStatus(id, AppStatus.Error).transaction)
          _ <- e.clusterId.traverse(clusterId =>
            kubernetesClusterQuery.updateStatus(clusterId, KubernetesClusterStatus.Error).transaction
          )
          _ <- e.nodepoolId.traverse(nodepoolId =>
            nodepoolQuery.updateStatus(nodepoolId, NodepoolStatus.Error).transaction
          )
          _ <- logger.error(e)(s"updating db state for an async error for app ${e.appId}")
        } yield ()
      case _ =>
        F.raiseError(
          new RuntimeException(s"handleKubernetesError should not be used with a non kubernetes error. Error: ${e}")
        )
    }

  private def deleteDiskForApp(diskId: DiskId)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    deleteDisk(diskId, true)

  private def createDiskForApp(msg: CreateAppMessage)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    msg.createDisk match {
      case true =>
        for {
          _ <- logger.info(s"Beginning disk creation for app ${msg.appId}")
          ctx <- ev.ask
          getAppOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(msg.project, msg.appName).transaction
          getApp <- F.fromOption(getAppOpt, AppNotFoundException(msg.project, msg.appName, ctx.traceId))
          disk <- F.fromOption(
            getApp.app.appResources.disk,
            AppCreationException(
              s"create disk was true for create app message, but app ${getApp.app.id} does not have a disk id saved"
            )
          )
          _ <- createDisk(CreateDiskMessage.fromDisk(disk, Some(ctx.traceId)), true)
        } yield ()
      case false => F.unit
    }

  def handleDeleteAppMessage(msg: DeleteAppMessage)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- logger.info(s"beginning delete app for app ${msg.project}/${msg.appName}")

      initialGoogleCall = for {
        getAppOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(msg.project, msg.appName).transaction
        getApp <- F.fromOption(getAppOpt, AppNotFoundException(msg.project, msg.appName, ctx.traceId))
        deleteResult <- gkeInterp.deleteNodepool(getApp)
      } yield (getApp, deleteResult)
      (getApp, deleteResult) <- initialGoogleCall.adaptError {
        case e =>
          PubsubKubernetesError(
            AppError(e.getMessage(), ctx.now, ErrorAction.DeleteGalaxyApp, ErrorSource.Nodepool, None),
            Some(msg.appId),
            false,
            Some(msg.nodepoolId),
            None
          )
      }

      task = for {
        _ <- gkeInterp.pollNodepool(deleteResult).adaptError {
          case e =>
            PubsubKubernetesError(
              AppError(e.getMessage(), ctx.now, ErrorAction.DeleteGalaxyApp, ErrorSource.Nodepool, None),
              Some(msg.appId),
              false,
              Some(msg.nodepoolId),
              None
            )
        }
        _ <- gkeInterp.deleteAndPollApp(getApp).adaptError {
          case e =>
            PubsubKubernetesError(
              AppError(e.getMessage(), ctx.now, ErrorAction.DeleteGalaxyApp, ErrorSource.App, None),
              Some(msg.appId),
              false,
              None,
              None
            )
        }
        _ <- msg.diskId.traverse(diskId => deleteDiskForApp(diskId)).adaptError {
          case e =>
            PubsubKubernetesError(
              AppError(e.getMessage(), ctx.now, ErrorAction.DeleteGalaxyApp, ErrorSource.Disk, None),
              Some(msg.appId),
              false,
              None,
              None
            )
        }
        _ <- logger.info(s"completed delete app for app ${msg.project}/${msg.appName}")
      } yield ()
      _ <- asyncTasks.enqueue1(
        Task(ctx.traceId, task, Some(handleKubernetesError), ctx.now)
      )
    } yield ()

  private def createRuntimeErrorHandler(msg: CreateRuntimeMessage, now: Instant)(e: Throwable): F[Unit] =
    for {
      _ <- logger.error(e)(s"Failed to create runtime ${msg.runtimeProjectAndName} in Google")
      errorMessage = e match {
        case leoEx: LeoException =>
          Some(ErrorReport.loggableString(leoEx.toErrorReport))
        case ee: com.google.api.gax.rpc.AbortedException
            if ee.getStatusCode().getCode == 409 && ee.getMessage().contains("already exists") =>
          None //this could happen when pubsub redelivers an event unexpectedly
        case _ =>
          Some(s"Failed to create cluster ${msg.runtimeProjectAndName} due to ${e.getMessage}")
      }
      _ <- errorMessage.traverse(m =>
        (clusterErrorQuery.save(msg.runtimeId, RuntimeError(m, -1, now)) >>
          clusterQuery.updateClusterStatus(msg.runtimeId, RuntimeStatus.Error, now)).transaction[F]
      )
      _ <- if (e.isReportWorthy)
        errorReporting.reportError(e)
      else F.unit
    } yield ()

  private def handleRuntimeMessageError(runtimeId: Long, now: Instant, msg: String)(e: Throwable): F[Unit] = {
    val m = s"${msg} due to ${e.getMessage}"
    for {
      _ <- clusterErrorQuery.save(runtimeId, RuntimeError(m, -1, now)).transaction
      _ <- logger.error(e)(m)
      _ <- if (e.isReportWorthy)
        errorReporting.reportError(e)
      else F.unit
    } yield ()
  }

  private def logError(projectAndName: String, action: String): Throwable => F[Unit] =
    t => logger.error(t)(s"Fail to monitor ${projectAndName} for ${action}")
}
