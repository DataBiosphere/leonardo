package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant
import java.util.concurrent.TimeoutException

import _root_.io.chrisdavenport.log4cats.StructuredLogger
import cats.effect.implicits._
import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.Disk
import fs2.Stream
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.google2.{
  ComputePollOperation,
  Event,
  GoogleDiskService,
  GoogleSubscriber,
  MachineTypeName,
  OperationName
}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.{cloudServiceSyntax, _}
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, LeoException}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.google.{GcsObjectName, GcsPath}
import org.broadinstitute.dsde.workbench.model.{ErrorReport, TraceId, WorkbenchException}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class LeoPubsubMessageSubscriber[F[_]: Timer: ContextShift](
  config: LeoPubsubMessageSubscriberConfig,
  subscriber: GoogleSubscriber[F, LeoPubsubMessage],
  asyncTasks: InspectableQueue[F, Task[F]],
  googleDiskService: GoogleDiskService[F],
  computePollOperation: ComputePollOperation[F],
  authProvider: LeoAuthProvider[F]
)(implicit executionContext: ExecutionContext,
  F: ConcurrentEffect[F],
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
              if (ee.isRetryable)
                logger.error(e)("Fail to process retryable pubsub message") >> F
                  .delay(event.consumer.nack())
              else
                logger.error(e)("Fail to process non-retryable pubsub message") >> ack(event)
            case ee: WorkbenchException if ee.getMessage.contains("Call to Google API failed") =>
              logger
                .error(e)("Fail to process retryable pubsub message due to Google API call failure") >> F
                .delay(event.consumer.nack())
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
        Task(ctx.traceId, taskToRun, Some(logError(msg.runtimeProjectAndName.toString, "creating runtime")), ctx.now)
      )
    } yield ()

    createCluster.handleErrorWith {
      case e =>
        for {
          ctx <- ev.ask
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
            (clusterErrorQuery.save(msg.runtimeId, RuntimeError(m, -1, ctx.now)) >>
              clusterQuery.updateClusterStatus(msg.runtimeId, RuntimeStatus.Error, ctx.now)).transaction[F]
          )
        } yield ()
    }
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
      fa = if (msg.deleteDisk)
        runtimeConfig match {
          case rc: RuntimeConfig.GceWithPdConfig =>
            for {
              _ <- poll
              now <- nowInstant
              _ <- rc.persistentDiskId.traverse { id =>
                val deleteDisk = for {
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
                    .save(runtime.id, RuntimeError(e.getMessage, -1, now))
                    .transaction
                    .void
                )
              }
            } yield ()
          case _ => poll
        }
      else poll
      _ <- asyncTasks.enqueue1(
        Task(ctx.traceId, fa, Some(logError(runtime.projectNameString, "deleting runtime")), ctx.now)
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
        Task(ctx.traceId, poll, Some(logError(runtime.projectNameString, "stopping runtime")), ctx.now)
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
          Some(logError(runtime.projectNameString, "starting runtime")),
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
      _ <- if (msg.newNumWorkers.isDefined || msg.newNumPreemptibles.isDefined) {
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
        } yield ()
      } else F.unit

      // Update the disk size
      _ <- msg.newDiskSize.traverse_ { d =>
        for {
          _ <- runtimeConfig.cloudService.interpreter.updateDiskSize(UpdateDiskSizeParams(runtime, d))
          _ <- RuntimeConfigQueries.updateDiskSize(runtime.runtimeConfigId, d, ctx.now).transaction
        } yield ()
      }
      // Update the machine type
      // If it's a stop-update transition, transition the runtime to Stopping
      _ <- msg.newMachineType.traverse_ { m =>
        if (msg.stopToUpdateMachineType) {
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
              _ <- updateRuntimeAfterStopAndStarting(runtime, m)(ctxStarting)
              _ <- patchQuery.updatePatchAsComplete(runtime.id).transaction
            } yield ()
            _ <- asyncTasks.enqueue1(
              Task(ctx.traceId, task, Some(logError(runtime.projectNameString, "updating runtime")), ctx.now)
            )
          } yield ()
        } else {
          runtimeConfig.cloudService.interpreter.updateMachineType(UpdateMachineTypeParams(runtime, m, ctx.now))
        }
      }
    } yield ()

  private def updateRuntimeAfterStopAndStarting(
    runtime: Runtime,
    targetMachineType: MachineTypeName
  )(implicit ev: ApplicativeAsk[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask

      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      _ <- runtimeConfig.cloudService.interpreter
        .updateMachineType(UpdateMachineTypeParams(runtime, targetMachineType, ctx.now))

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
      _ <- runtimeConfig.cloudService.process(runtime.id, RuntimeStatus.Starting).compile.drain
    } yield ()

  private[monitor] def handleCreateDiskMessage(msg: CreateDiskMessage)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] = {
    val createDisk = for {
      ctx <- ev.ask
      operation <- googleDiskService.createDisk(
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
      _ <- asyncTasks.enqueue1(
        Task(
          ctx.traceId,
          task,
          Some(logError(s"${ctx.traceId.asString} | ${msg.googleProject.value}/${msg.diskId.value}", "creating disk")),
          ctx.now
        )
      )
    } yield ()
    createDisk.handleErrorWith {
      case e =>
        for {
          ctx <- ev.ask
          _ <- logger.error(e)(s"Failed to create disk ${msg.name.value} in Google project ${msg.googleProject.value}")
          _ <- persistentDiskQuery.updateStatus(msg.diskId, DiskStatus.Failed, ctx.now).transaction[F]
        } yield ()
    }
  }

  private[monitor] def handleDeleteDiskMessage(msg: DeleteDiskMessage)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      now <- nowInstant
      diskOpt <- persistentDiskQuery.getById(msg.diskId).transaction
      disk <- diskOpt.fold(
        F.raiseError[PersistentDisk](PubsubHandleMessageError.DiskNotFound(msg.diskId, msg))
      )(F.pure)
      _ <- if (disk.status != DiskStatus.Deleting)
        F.raiseError[Unit](
          PubsubHandleMessageError.DiskInvalidState(msg.diskId, disk.projectNameString, disk, msg)
        )
      else F.unit
      operation <- googleDiskService.deleteDisk(disk.googleProject, disk.zone, disk.name)
      whenDone = persistentDiskQuery.delete(msg.diskId, now).transaction[F].void >> authProvider.notifyResourceDeleted(
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

      _ <- asyncTasks.enqueue1(
        Task(ctx.traceId,
             task,
             Some(logError(s"${ctx.traceId.asString} | ${msg.diskId.value}", "Deleting Disk")),
             ctx.now)
      )
    } yield ()

  private[monitor] def handleUpdateDiskMessage(msg: UpdateDiskMessage)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      diskOpt <- persistentDiskQuery.getById(msg.diskId).transaction
      disk <- diskOpt.fold(
        F.raiseError[PersistentDisk](PubsubHandleMessageError.DiskNotFound(msg.diskId, msg))
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
          nowInstant.flatMap(now => persistentDiskQuery.updateSize(msg.diskId, msg.newSize, now).transaction[F]).void,
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

  def handleCreateAppMessage(msg: CreateAppMessage): F[Unit] = F.unit

  def handleDeleteAppMessage(msg: DeleteAppMessage): F[Unit] = F.unit

  private def logError(projectAndName: String, action: String): Throwable => F[Unit] =
    t => logger.error(t)(s"Fail to monitor ${projectAndName} for ${action}")
}

sealed trait PubsubHandleMessageError extends NoStackTrace {
  def isRetryable: Boolean
}
object PubsubHandleMessageError {
  final case class ClusterNotFound(clusterId: Long, message: LeoPubsubMessage) extends PubsubHandleMessageError {
    override def getMessage: String =
      s"Unable to process transition finished message ${message} for cluster ${clusterId} because it was not found in the database"
    val isRetryable: Boolean = false
  }

  final case class ClusterNotStopped(clusterId: Long,
                                     projectName: String,
                                     clusterStatus: RuntimeStatus,
                                     message: LeoPubsubMessage)
      extends PubsubHandleMessageError {
    override def getMessage: String =
      s"Unable to process message ${message} for cluster ${clusterId}/${projectName} in status ${clusterStatus.toString}, when the monitor signalled it stopped as it is not stopped."
    val isRetryable: Boolean = false
  }

  final case class ClusterInvalidState(clusterId: Long,
                                       projectName: String,
                                       cluster: Runtime,
                                       message: LeoPubsubMessage)
      extends PubsubHandleMessageError {
    override def getMessage: String =
      s"${clusterId}, ${projectName}, ${message} | This is likely due to a mismatch in state between the db and the message, or an improperly formatted machineConfig in the message. Cluster details: ${cluster}"
    val isRetryable: Boolean = false
  }

  final case class DiskNotFound(diskId: DiskId, message: LeoPubsubMessage) extends PubsubHandleMessageError {
    override def getMessage: String =
      s"Unable to process message ${message} for disk ${diskId.value} because it was not found in the database"
    val isRetryable: Boolean = false
  }

  final case class DiskInvalidState(diskId: DiskId,
                                    projectName: String,
                                    disk: PersistentDisk,
                                    message: LeoPubsubMessage)
      extends PubsubHandleMessageError {
    override def getMessage: String =
      s"${diskId}, ${projectName}, ${message} | Unable to process disk because not in correct state. Disk details: ${disk}"
    val isRetryable: Boolean = false
  }
}

final case class PersistentDiskMonitor(maxAttempts: Int, interval: FiniteDuration)
final case class PersistentDiskMonitorConfig(create: PersistentDiskMonitor,
                                             delete: PersistentDiskMonitor,
                                             update: PersistentDiskMonitor)
final case class LeoPubsubMessageSubscriberConfig(concurrency: Int,
                                                  timeout: FiniteDuration,
                                                  persistentDiskMonitorConfig: PersistentDiskMonitorConfig)
