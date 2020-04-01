package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant

import _root_.io.chrisdavenport.log4cats.StructuredLogger
import cats.effect.{Async, Concurrent, ContextShift, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import fs2.{Pipe, Stream}
import org.broadinstitute.dsde.workbench.google2.{Event, GoogleSubscriber}
import org.broadinstitute.dsde.workbench.leonardo.RuntimeStatus.{Stopped, Starting}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.config.Config.subscriberConfig
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.google.{GcsObjectName, GcsPath}
import org.broadinstitute.dsde.workbench.model.{ErrorReport, TraceId, WorkbenchException}

import scala.concurrent.ExecutionContext
import scala.util.control.NoStackTrace

class LeoPubsubMessageSubscriber[F[_]: Async: Timer: ContextShift: Concurrent](
  subscriber: GoogleSubscriber[F, LeoPubsubMessage]
)(implicit executionContext: ExecutionContext,
  logger: StructuredLogger[F],
  dbRef: DbReference[F],
  runtimeInstances: RuntimeInstances[F]) {
  private[monitor] def messageResponder(message: LeoPubsubMessage,
                                        now: Instant)(implicit traceId: ApplicativeAsk[F, AppContext]): F[Unit] =
    message match {
      case msg: StopUpdateMessage =>
        handleStopUpdateMessage(msg, now)
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
                  logger.error(e)("Fail to process retryable pubsub message") >> Async[F]
                    .delay(event.consumer.nack())
                else
                  logger.error(e)("Fail to process non-retryable pubsub message") >> ack(event)
              case ee: WorkbenchException if ee.getMessage.contains("Call to Google API failed") =>
                logger
                  .error(e)("Fail to process retryable pubsub message due to Google API call failure") >> Async[F]
                  .delay(event.consumer.nack())
              case _ =>
                logger.error(e)("Fail to process non-retryable pubsub message") >> ack(event)
            }
          case Right(_) => ack(event)
        }
      } yield ()

      res.handleErrorWith { e =>
        logger.error(e)("Fail to process pubsub message") >> Async[F].delay(event.consumer.ack())
      }
    }
  }

  val process: Stream[F, Unit] =
    (Stream.eval(logger.info(s"starting subscriber ${subscriberConfig.projectTopicName}")) ++ (subscriber.messages through messageHandler))
      .handleErrorWith(
        error => Stream.eval(logger.error(error)("Failed to initialize message processor"))
      )

  private def ack(event: Event[LeoPubsubMessage]): F[Unit] =
    logger.info(s"acking message: ${event}") >> Async[F].delay(
      event.consumer.ack()
    )

  private def handleStopUpdateMessage(message: StopUpdateMessage,
                                      now: Instant)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    dbRef
      .inTransaction { clusterQuery.getClusterById(message.runtimeId) }
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
          Async[F].raiseError(
            PubsubHandleMessageError
              .ClusterInvalidState(message.runtimeId, resolvedCluster.projectNameString, resolvedCluster, message)
          )
        case None =>
          Async[F].raiseError(PubsubHandleMessageError.ClusterNotFound(message.runtimeId, message))
      }

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
            patchQuery.getPatchAction(message.runtimePatchDetails)
          }

          result <- clusterOpt match {
            case Some(resolvedCluster)
              if resolvedCluster.status != RuntimeStatus.Stopped && savedMasterMachineType.isDefined =>
              Async[F].raiseError[Unit](
                PubsubHandleMessageError.ClusterNotStopped(resolvedCluster.id,
                  resolvedCluster.projectNameString,
                  resolvedCluster.status,
                  message)
              )
            case Some(resolvedCluster) =>
              savedMasterMachineType match {
                case Some(machineType) =>
                  for {
                    runtimeConfig <- dbRef.inTransaction(
                      RuntimeConfigQueries.getRuntimeConfig(resolvedCluster.runtimeConfigId)
                    )
                    // perform gddao and db updates for new resources
                    _ <- runtimeConfig.cloudService.interpreter
                      .updateMachineType(UpdateMachineTypeParams(resolvedCluster, machineType, now))
                    // start cluster
                    _ <- runtimeConfig.cloudService.interpreter.startRuntime(StartRuntimeParams(resolvedCluster, now))
                    // update runtime status in database
                    _ <- dbRef.inTransaction {
                      clusterQuery.updateClusterStatus(
                        resolvedCluster.id,
                        RuntimeStatus.Starting,
                        now
                      )
                    }
                  } yield ()
                case None => Async[F].unit //the database has no record of a follow-up being needed. This is a no-op
              }

            case None =>
              Async[F].raiseError[Unit](
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
      case _ => Async[F].unit
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
        msg.id,
        Some(clusterResult.asyncRuntimeFields),
        now
      )
      // Save the VM image and async fields in the database
      clusterImage = RuntimeImage(RuntimeImageType.VM, clusterResult.customImage.asString, now)
      _ <- (clusterQuery.updateAsyncClusterCreationFields(updateAsyncClusterCreationFields) >> clusterImageQuery.save(
        msg.id,
        clusterImage
      )).transaction
      _ <- logger.info(
        s"Cluster ${msg.runtimeProjectAndName} was successfully created. Will monitor the creation process."
      )
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
          _ <- (clusterErrorQuery.save(msg.id, RuntimeError(errorMessage, -1, now)) >>
            clusterQuery.updateClusterStatus(msg.id, RuntimeStatus.Error, now)).transaction[F]
        } yield ()
    }
  }

  private[monitor] def handleDeleteRuntimeMessage(msg: DeleteRuntimeMessage, now: Instant)(
    implicit traceId: ApplicativeAsk[F, TraceId]
  ): F[Unit] =
    for {
      runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
      runtime <- runtimeOpt.fold(
        Async[F].raiseError[Runtime](PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      )(Async[F].pure)
      _ <- if (runtime.status != RuntimeStatus.Deleting)
        Async[F].raiseError[Unit](
          PubsubHandleMessageError.ClusterInvalidState(msg.runtimeId, runtime.projectNameString, runtime, msg)
        )
      else Async[F].unit
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      _ <- runtimeConfig.cloudService.interpreter.deleteRuntime(DeleteRuntimeParams(runtime))
    } yield ()

  private[monitor] def handleStopRuntimeMessage(msg: StopRuntimeMessage, now: Instant)(
    implicit traceId: ApplicativeAsk[F, TraceId]
  ): F[Unit] =
    for {
      runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
      runtime <- runtimeOpt.fold(
        Async[F].raiseError[Runtime](PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      )(Async[F].pure)
      _ <- if (runtime.status != RuntimeStatus.Stopping)
        Async[F].raiseError[Unit](
          PubsubHandleMessageError.ClusterInvalidState(msg.runtimeId, runtime.projectNameString, runtime, msg)
        )
      else Async[F].unit
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      _ <- runtimeConfig.cloudService.interpreter
        .stopRuntime(StopRuntimeParams(RuntimeAndRuntimeConfig(runtime, runtimeConfig), now))
    } yield ()

  private[monitor] def handleStartRuntimeMessage(msg: StartRuntimeMessage, now: Instant)(
    implicit traceId: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
      runtime <- runtimeOpt.fold(
        Async[F].raiseError[Runtime](PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      )(Async[F].pure)
      _ <- if (runtime.status != RuntimeStatus.Starting)
        Async[F].raiseError[Unit](
          PubsubHandleMessageError.ClusterInvalidState(msg.runtimeId, runtime.projectNameString, runtime, msg)
        )
      else Async[F].unit
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      _ <- runtimeConfig.cloudService.interpreter.startRuntime(StartRuntimeParams(runtime, now))
    } yield ()

  private[monitor] def handleUpdateRuntimeMessage(msg: UpdateRuntimeMessage, now: Instant)(
    implicit traceId: ApplicativeAsk[F, TraceId]
  ): F[Unit] =
    for {
      runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
      runtime <- runtimeOpt.fold(
        Async[F].raiseError[Runtime](PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      )(Async[F].pure)
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction

      // We assume all validation has already happened in RuntimeServiceInterp

      // Resize the cluster
      _ <- if (msg.newNumWorkers.isDefined || msg.newNumPreemptibles.isDefined) {
        for {
          _ <- runtimeConfig.cloudService.interpreter
            .resizeCluster(ResizeClusterParams(runtime, msg.newNumWorkers, msg.newNumPreemptibles))
          _ <- msg.newNumWorkers.traverse_(
            a => RuntimeConfigQueries.updateNumberOfWorkers(runtime.runtimeConfigId, a, now).transaction
          )
          _ <- msg.newNumPreemptibles.traverse_(
            a =>
              RuntimeConfigQueries.updateNumberOfPreemptibleWorkers(runtime.runtimeConfigId, Some(a), now).transaction
          )
          _ <- clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.Updating, now).transaction.void
        } yield ()
      } else Async[F].unit

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
          } yield ()
        } else {
          for {
            _ <- runtimeConfig.cloudService.interpreter.updateMachineType(UpdateMachineTypeParams(runtime, m, now))
            _ <- RuntimeConfigQueries.updateMachineType(runtime.runtimeConfigId, m, now).transaction
          } yield ()
        }
      }
    } yield ()
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
