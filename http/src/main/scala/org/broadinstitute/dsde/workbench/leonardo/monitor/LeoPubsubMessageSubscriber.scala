package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant

import _root_.io.chrisdavenport.log4cats.StructuredLogger
import cats.effect.implicits._
import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import fs2.Stream
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.google2.{Event, GoogleSubscriber, MachineTypeName}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.{cloudServiceSyntax, _}
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
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
  asyncTasks: InspectableQueue[F, Task[F]]
)(implicit executionContext: ExecutionContext,
  F: ConcurrentEffect[F],
  logger: StructuredLogger[F],
  dbRef: DbReference[F],
  runtimeInstances: RuntimeInstances[F],
  monitor: RuntimeMonitor[F, CloudService]) {
  private[monitor] def messageResponder(message: LeoPubsubMessage,
                                        now: Instant)(implicit traceId: ApplicativeAsk[F, AppContext]): F[Unit] =
    message match {
      case msg: CreateRuntimeMessage =>
        handleCreateRuntimeMessage(msg, now)
      case msg: DeleteRuntimeMessage =>
        handleDeleteRuntimeMessage(msg, now)
      case msg: StopRuntimeMessage =>
        handleStopRuntimeMessage(msg, now)
      case msg: StartRuntimeMessage =>
        handleStartRuntimeMessage(msg, now)
      case msg: UpdateRuntimeMessage =>
        handleUpdateRuntimeMessage(msg, now)
      case _ => throw new NotImplementedError("disk events are not implemented yet") //TODO: put disk messages here
    }

  private[monitor] def messageHandler(event: Event[LeoPubsubMessage]): F[Unit] = {
    val traceId = event.traceId.getOrElse(TraceId("None"))
    val now = Instant.ofEpochMilli(com.google.protobuf.util.Timestamps.toMillis(event.publishedTime))
    implicit val appContext = ApplicativeAsk.const[F, AppContext](AppContext(traceId, now))
    val res = for {
      res <- messageResponder(event.msg, now)
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

  private[monitor] def handleCreateRuntimeMessage(msg: CreateRuntimeMessage, now: Instant)(
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
        now
      )
      // Save the VM image and async fields in the database
      clusterImage = RuntimeImage(RuntimeImageType.VM, clusterResult.customImage.asString, now)
      _ <- (clusterQuery.updateAsyncClusterCreationFields(updateAsyncClusterCreationFields) >> clusterImageQuery.save(
        msg.runtimeId,
        clusterImage
      )).transaction
      taskToRun = for {
        _ <- msg.runtimeConfig.cloudService.process(msg.runtimeId, RuntimeStatus.Creating).compile.drain
        _ <- if (msg.stopAfterCreation) { //TODO: once we remove legacy /api/clusters aroute or we remove `stopAfterCreation` support, we can remove this block
          for {
            _ <- clusterQuery
              .updateClusterStatus(msg.runtimeId, RuntimeStatus.Stopping, now)
              .transaction
            runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
            runtime <- F.fromEither(runtimeOpt.toRight(new Exception(s"can't find ${msg.runtimeId} in DB")))
            runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(runtime, msg.runtimeConfig)
            _ <- msg.runtimeConfig.cloudService.interpreter
              .stopRuntime(StopRuntimeParams(runtimeAndRuntimeConfig, now))
            _ <- msg.runtimeConfig.cloudService.process(msg.runtimeId, RuntimeStatus.Stopping).compile.drain
          } yield ()
        } else F.unit
      } yield ()
      _ <- asyncTasks.enqueue1(Task(ctx.traceId, taskToRun, Some(logError(msg.runtimeProjectAndName.toString)), now))
    } yield ()

    createCluster.handleErrorWith {
      case e =>
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
        } yield ()
    }
  }

  private[monitor] def handleDeleteRuntimeMessage(msg: DeleteRuntimeMessage, now: Instant)(
    implicit ev: ApplicativeAsk[F, TraceId]
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
        DeleteRuntimeParams(runtime.googleProject, runtime.runtimeName, runtime.asyncRuntimeFields)
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
      _ <- asyncTasks.enqueue1(Task(ctx, poll, Some(logError(runtime.projectNameString)), now))
    } yield ()

  private[monitor] def handleStopRuntimeMessage(msg: StopRuntimeMessage, now: Instant)(
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
        StopRuntimeParams(RuntimeAndRuntimeConfig(runtime, runtimeConfig), now)
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
      _ <- asyncTasks.enqueue1(Task(ctx.traceId, poll, Some(logError(runtime.projectNameString)), ctx.now))
    } yield ()

  private[monitor] def handleStartRuntimeMessage(msg: StartRuntimeMessage, now: Instant)(
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
      _ <- runtimeConfig.cloudService.interpreter.startRuntime(StartRuntimeParams(runtime, now))
      _ <- asyncTasks.enqueue1(
        Task(ctx.traceId,
             runtimeConfig.cloudService.process(msg.runtimeId, RuntimeStatus.Starting).compile.drain,
             Some(logError(runtime.projectNameString)),
             ctx.now)
      )
    } yield ()

  private[monitor] def handleUpdateRuntimeMessage(msg: UpdateRuntimeMessage, now: Instant)(
    implicit traceId: ApplicativeAsk[F, TraceId]
  ): F[Unit] =
    for {
      tid <- traceId.ask
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
            RuntimeConfigQueries.updateNumberOfWorkers(runtime.runtimeConfigId, a, now).transaction
          )
          _ <- msg.newNumPreemptibles.traverse_(a =>
            RuntimeConfigQueries.updateNumberOfPreemptibleWorkers(runtime.runtimeConfigId, Some(a), now).transaction
          )
          _ <- clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.Updating, now).transaction.void
        } yield ()
      } else F.unit

      // Update the disk size
      _ <- msg.newDiskSize.traverse_ { d =>
        for {
          _ <- runtimeConfig.cloudService.interpreter.updateDiskSize(UpdateDiskSizeParams(runtime, d))
          _ <- RuntimeConfigQueries.updateDiskSize(runtime.runtimeConfigId, d, now).transaction
        } yield ()
      }
      // Update the machine type
      // If it's a stop-update transition, transition the runtime to Stopping
      _ <- msg.newMachineType.traverse_ { m =>
        if (msg.stopToUpdateMachineType) {
          for {
            timeToStop <- nowInstant
            ctxStopping = ApplicativeAsk.const[F, AppContext](
              AppContext(tid, timeToStop)
            )
            _ <- dbRef.inTransaction(clusterQuery.updateClusterStatus(msg.runtimeId, RuntimeStatus.Stopping, now))
            operation <- runtimeConfig.cloudService.interpreter
              .stopRuntime(StopRuntimeParams(RuntimeAndRuntimeConfig(runtime, runtimeConfig), now))(ctxStopping)
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
              implicit0(ctxStarting: ApplicativeAsk[F, AppContext]) = ApplicativeAsk.const[F, AppContext](
                AppContext(tid, now)
              )
              _ <- updateRuntimeAfterStopAndStarting(runtime, m)
              _ <- patchQuery.updatePatchAsComplete(runtime.id).transaction
            } yield ()
            _ <- asyncTasks.enqueue1(Task(tid, task, Some(logError(runtime.projectNameString)), now))
          } yield ()
        } else {
          runtimeConfig.cloudService.interpreter.updateMachineType(UpdateMachineTypeParams(runtime, m, now))
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

      _ <- runtimeConfig.cloudService.interpreter.startRuntime(StartRuntimeParams(runtime, ctx.now))
      _ <- dbRef.inTransaction {
        clusterQuery.updateClusterStatus(
          runtime.id,
          RuntimeStatus.Starting,
          ctx.now
        )
      }
      _ <- runtimeConfig.cloudService.process(runtime.id, RuntimeStatus.Starting).compile.drain
    } yield ()

  private def logError(projectAndName: String): Throwable => F[Unit] =
    t => logger.error(t)(s"Fail to monitor ${projectAndName}")
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
}

final case class LeoPubsubMessageSubscriberConfig(concurrency: Int, timeout: FiniteDuration)
