package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant

import _root_.io.chrisdavenport.log4cats.StructuredLogger
import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import fs2.{Pipe, Stream}
import org.broadinstitute.dsde.workbench.google2.{Event, GoogleComputeService, GoogleSubscriber, InstanceName, MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.RuntimeStatus.{Starting, Stopped}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.config.Config.subscriberConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.WelderDAO
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.google.{GcsObjectName, GcsPath}
import org.broadinstitute.dsde.workbench.model.{ErrorReport, TraceId, WorkbenchException}

import scala.concurrent.ExecutionContext
import scala.util.control.NoStackTrace

class LeoPubsubMessageSubscriber[F[_]: Timer: ContextShift](
  config: LeoPubsubMessageSubscriberConfig,
  subscriber: GoogleSubscriber[F, LeoPubsubMessage],
  googleComputeService: GoogleComputeService[F],
  gceRuntimeMonitor: GceRuntimeMonitor[F],
  welderDAO: WelderDAO[F]
)(implicit executionContext: ExecutionContext,
  F: ConcurrentEffect[F],
  logger: StructuredLogger[F],
  dbRef: DbReference[F],
  runtimeInstances: RuntimeInstances[F]) {
  private[monitor] def messageResponder(message: LeoPubsubMessage,
                                        now: Instant)(implicit traceId: ApplicativeAsk[F, AppContext]): F[Unit] =
    message match {
      case msg: StopUpdateMessage =>
        handleStopUpdateMessage(msg, now) //TODO: does this need monitor?
      case msg: RuntimeTransitionMessage =>
        handleRuntimeTransitionFinished(msg, now)
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
    }

  private[monitor] def messageHandler: Pipe[F, Event[LeoPubsubMessage], Unit] = in => {
    in.evalMap { event =>
      val traceId = event.traceId.getOrElse(TraceId("None"))

      val now = Instant.ofEpochMilli(com.google.protobuf.util.Timestamps.toMillis(event.publishedTime))
      implicit val appContext = ApplicativeAsk.const[F, AppContext](AppContext(traceId, now))
      val res = for {
        res <- messageResponder(event.msg, now).attempt
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

      res.handleErrorWith { e =>
        logger.error(e)("Fail to process pubsub message") >> F.delay(event.consumer.ack())
      }
    }
  }

  val process: Stream[F, Unit] =
    (Stream.eval(logger.info(s"starting subscriber ${subscriberConfig.projectTopicName}")) ++ (subscriber.messages through messageHandler))
      .handleErrorWith(error => Stream.eval(logger.error(error)("Failed to initialize message processor")))

  private def ack(event: Event[LeoPubsubMessage]): F[Unit] =
    logger.info(s"acking message: ${event}") >> F.delay(
      event.consumer.ack()
    )

  private def handleStopUpdateMessage(message: StopUpdateMessage,
                                      now: Instant)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    dbRef
      .inTransaction(clusterQuery.getClusterById(message.runtimeId))
      .flatMap {
        case Some(resolvedCluster) if RuntimeStatus.stoppableStatuses.contains(resolvedCluster.status) =>
          for {
            _ <- logger.info(
              s"stopping cluster ${resolvedCluster.projectNameString} in messageResponder"
            )
            _ <- dbRef.inTransaction(
              clusterQuery.setToStopping(message.runtimeId, now)
            )
            runtimeConfig <- dbRef.inTransaction(
              RuntimeConfigQueries.getRuntimeConfig(resolvedCluster.runtimeConfigId)
            )
            _ <- runtimeConfig.cloudService.interpreter
              .stopRuntime(StopRuntimeParams(RuntimeAndRuntimeConfig(resolvedCluster, runtimeConfig), now))
          } yield ()
        case Some(resolvedCluster) =>
          F.raiseError(
            PubsubHandleMessageError
              .ClusterInvalidState(message.runtimeId, resolvedCluster.projectNameString, resolvedCluster, message)
          )
        case None =>
          F.raiseError(PubsubHandleMessageError.ClusterNotFound(message.runtimeId, message))
      }

  // This can be deprecated once we have Dataproc also moved to fs2.Stream for monitoring
  private def handleRuntimeTransitionFinished(
    message: RuntimeTransitionMessage,
    now: Instant
  )(implicit ev: ApplicativeAsk[F, AppContext]): F[Unit] =
    message.runtimePatchDetails.runtimeStatus match {
      case Stopped =>
        for {
          clusterOpt <- dbRef.inTransaction {
            clusterQuery.getClusterById(message.runtimePatchDetails.runtimeId)
          }
          savedMasterMachineType <- dbRef.inTransaction {
            patchQuery.getPatchAction(message.runtimePatchDetails.runtimeId)
          }

          result <- clusterOpt match {
            case Some(resolvedCluster)
                if resolvedCluster.status != RuntimeStatus.Stopped && savedMasterMachineType.isDefined =>
              F.raiseError[Unit](
                PubsubHandleMessageError.ClusterNotStopped(resolvedCluster.id,
                                                           resolvedCluster.projectNameString,
                                                           resolvedCluster.status,
                                                           message)
              )
            case Some(resolvedCluster) =>
              savedMasterMachineType match {
                case Some(machineType) =>
                  for {
                    runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(resolvedCluster.runtimeConfigId).transaction
                    _ <- runtimeConfig.cloudService.interpreter
                      .updateMachineType(UpdateMachineTypeParams(resolvedCluster, machineType, now))
                    _ <- runtimeConfig.cloudService.interpreter.startRuntime(StartRuntimeParams(resolvedCluster, now))
                    _ <- dbRef.inTransaction {
                      clusterQuery.updateClusterStatus(
                        resolvedCluster.id,
                        RuntimeStatus.Starting,
                        now
                      )
                    }
                  } yield ()
                case None => F.unit //the database has no record of a follow-up being needed. This is a no-op
              }

            case None =>
              F.raiseError[Unit](
                PubsubHandleMessageError.ClusterNotFound(message.runtimePatchDetails.runtimeId, message)
              )
          }
        } yield result

      case Starting =>
        for {
          _ <- dbRef.inTransaction {
            patchQuery.updatePatchAsComplete(message.runtimePatchDetails.runtimeId)
          }
          // TODO: should eventually check if the resulting machine config is what the user requested to see if the patch worked correctly
        } yield ()

      //No actions for other statuses yet. There is some logic that will be needed for all other cases (i.e. the 'None' case where no cluster is found in the db and possibly the case that checks for the data in the DB)
      // TODO: Refactor once there is more than one case
      case _ => F.unit
    }

  private[monitor] def handleCreateRuntimeMessage(msg: CreateRuntimeMessage, now: Instant)(
    implicit traceId: ApplicativeAsk[F, AppContext]
  ): F[Unit] = {
    val createCluster = for {
      _ <- logger.info(s"Attempting to create cluster ${msg.runtimeProjectAndName} in Google...")
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
      _ <- if (msg.runtimeConfig.cloudService == CloudService.GCE)
        F.runAsync(gceRuntimeMonitor.process(msg.runtimeId).compile.drain)(
            logError(msg.runtimeProjectAndName.toString)
          )
          .to[F]
      else F.unit
    } yield ()

    createCluster.handleErrorWith {
      case e =>
        for {
          _ <- logger.error(e)(s"Failed to create cluster ${msg.runtimeProjectAndName} in Google")
          errorMessage = e match {
            case leoEx: LeoException =>
              ErrorReport.loggableString(leoEx.toErrorReport)
            case _ =>
              s"Failed to create cluster ${msg.runtimeProjectAndName} due to ${e.toString}"
          }
          _ <- (clusterErrorQuery.save(msg.runtimeId, RuntimeError(errorMessage, -1, now)) >>
            clusterQuery.updateClusterStatus(msg.runtimeId, RuntimeStatus.Error, now)).transaction[F]
        } yield ()
    }
  }

  private[monitor] def handleDeleteRuntimeMessage(msg: DeleteRuntimeMessage, now: Instant)(
    implicit traceId: ApplicativeAsk[F, TraceId]
  ): F[Unit] =
    for {
      runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
      runtime <- runtimeOpt.fold(
        F.raiseError[Runtime](PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      )(F.pure)
      _ <- if (runtime.status != RuntimeStatus.Deleting)
        F.raiseError[Unit](
          PubsubHandleMessageError.ClusterInvalidState(msg.runtimeId, runtime.projectNameString, runtime, msg)
        )
      else F.unit
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      _ <- if (runtimeConfig.cloudService == CloudService.GCE) {
        googleComputeService
          .deleteInstance(runtime.googleProject, config.gceZoneName, InstanceName(runtime.runtimeName.asString))
          .flatMap(
            op =>
              F.runAsync(
                  gceRuntimeMonitor.pollCheck(runtime.googleProject,
                                              RuntimeAndRuntimeConfig(runtime, runtimeConfig),
                                              op,
                                              RuntimeStatus.Deleting)
                )(logError(runtime.projectNameString))
                .to[F]
          )
      } else CloudService.Dataproc.interpreter.deleteRuntime(DeleteRuntimeParams(runtime))
    } yield ()

  private[monitor] def handleStopRuntimeMessage(msg: StopRuntimeMessage, now: Instant)(
    implicit traceId: ApplicativeAsk[F, TraceId]
  ): F[Unit] =
    for {
      runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
      runtime <- runtimeOpt.fold(
        F.raiseError[Runtime](PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      )(F.pure)
      _ <- if (runtime.status != RuntimeStatus.Stopping)
        F.raiseError[Unit](
          PubsubHandleMessageError.ClusterInvalidState(msg.runtimeId, runtime.projectNameString, runtime, msg)
        )
      else F.unit
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      _ <- if (runtime.welderEnabled) {
        welderDAO
          .flushCache(runtime.googleProject, runtime.runtimeName)
          .handleErrorWith(e => logger.error(e)(s"Failed to flush welder cache for ${runtime.projectNameString}"))
      } else F.unit
      _ <- if (runtimeConfig.cloudService == CloudService.GCE)
        for {
          operation <- googleComputeService.stopInstance(runtime.googleProject,
            config.gceZoneName,
            InstanceName(runtime.runtimeName.asString))
          _ <- F.runAsync(
            gceRuntimeMonitor.pollCheck(runtime.googleProject,
              RuntimeAndRuntimeConfig(runtime, runtimeConfig),
              operation,
              RuntimeStatus.Stopping)
          )(logError(runtime.projectNameString))
            .to[F]
        } yield ()
      else CloudService.Dataproc.interpreter.stopRuntime(StopRuntimeParams(RuntimeAndRuntimeConfig(runtime, runtimeConfig), now))
    } yield ()

  private[monitor] def handleStartRuntimeMessage(msg: StartRuntimeMessage, now: Instant)(
    implicit traceId: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
      runtime <- runtimeOpt.fold(
        F.raiseError[Runtime](PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      )(F.pure)
      _ <- if (runtime.status != RuntimeStatus.Starting)
        F.raiseError[Unit](
          PubsubHandleMessageError.ClusterInvalidState(msg.runtimeId, runtime.projectNameString, runtime, msg)
        )
      else F.unit
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      _ <- runtimeConfig.cloudService.interpreter.startRuntime(StartRuntimeParams(runtime, now))
      _ <- if (runtimeConfig.cloudService == CloudService.GCE)
        F.runAsync(gceRuntimeMonitor.process(msg.runtimeId).compile.drain)(logError(runtime.projectNameString)).to[F]
      else F.unit
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
            _ <- dbRef.inTransaction(clusterQuery.setToStopping(msg.runtimeId, now))
            _ <- runtimeConfig.cloudService.interpreter
              .stopRuntime(StopRuntimeParams(RuntimeAndRuntimeConfig(runtime, runtimeConfig), now))
             _ <- if (runtimeConfig.cloudService == CloudService.GCE) //TODO: we should cover dataproc as well in the future
              // We're running this asynchronously because we don't want to hold up acking event since Stopping an instance can take a while
               F.runAsync(gceRuntimeMonitor.process(msg.runtimeId).compile.drain) {
                 cb =>
                  cb match {
                    case Left(e) => F.toIO(logger.error(e)(s"fail to stop runtime ${runtime.projectNameString} for updating machine type"))
                    case Right(_) =>
                      val res = for {
                        now <- nowInstant
                        implicit0(ctx: ApplicativeAsk[F, AppContext]) = ApplicativeAsk.const[F, AppContext](AppContext(tid, now))
                        _ <- updateRuntimeAfterStopAndStarting(runtime, m)
                        _ <- patchQuery.updatePatchAsComplete(runtime.id).transaction
                      } yield ()
                      F.toIO(res)
                  }
               }.to[F]
            else F.unit
          } yield ()
        } else {
          for {
            _ <- runtimeConfig.cloudService.interpreter.updateMachineType(UpdateMachineTypeParams(runtime, m, now))
            _ <- RuntimeConfigQueries.updateMachineType(runtime.runtimeConfigId, m, now).transaction
          } yield ()
        }
      }
    } yield ()

  private def updateRuntimeAfterStopAndStarting(runtime: Runtime,
                                     targetMachineType: MachineTypeName
                                    )(implicit ev: ApplicativeAsk[F, AppContext]): F[Unit] = for {
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
    _ <- gceRuntimeMonitor.process(runtime.id).compile.drain
  } yield ()

  private def logError(projectAndName: String)(cb: Either[Throwable, Unit]): IO[Unit] = cb match {
    case Left(t)  => F.toIO(logger.error(t)(s"Fail to monitor ${projectAndName}"))
    case Right(_) => IO.unit
  }
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

final case class LeoPubsubMessageSubscriberConfig(gceZoneName: ZoneName)
