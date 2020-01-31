package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.effect.{Async, Concurrent, ContextShift, Timer}
import org.broadinstitute.dsde.workbench.google2.{Event, GoogleSubscriber}
import fs2.{Pipe, Stream}
import io.circe.{Decoder, DecodingFailure, Encoder}
import org.broadinstitute.dsde.workbench.leonardo.MachineConfig
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, clusterQuery, followupQuery}
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterStatus, MachineType}
import org.broadinstitute.dsde.workbench.leonardo.util.ClusterHelper
import _root_.io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus.Stopped
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.model.WorkbenchException
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster

import scala.concurrent.ExecutionContext
import scala.util.control.NoStackTrace

class LeoPubsubMessageSubscriber[F[_]: Async: Timer: ContextShift: Logger: Concurrent](
  subscriber: GoogleSubscriber[F, LeoPubsubMessage],
  clusterHelper: ClusterHelper,
  dbRef: DbReference[F]
)(implicit executionContext: ExecutionContext) {
  private[monitor] def messageResponder(message: LeoPubsubMessage): F[Unit] = {
    message match {
      case msg @ StopUpdateMessage(_, _) =>
        handleStopUpdateMessage(msg)
      case msg @ ClusterTransitionFinishedMessage(_) =>
        handleClusterTransitionFinished(msg)
    }
  }

  private[monitor] def messageHandler: Pipe[F, Event[LeoPubsubMessage], Unit] = in => {
    in.evalMap { event =>
      val res = for {
        _ <- Logger[F].info(s"Subscriber received ${event.msg}")
        res <- messageResponder(event.msg).attempt
        _ <- res match {
          case Left(e) =>
            e match {
              case ee: PubsubHandleMessageError =>
                if(ee.isRetryable)
                  Logger[F].error(e)("Fail to process retryable pubsub message") >> Async[F].delay(event.consumer.nack())
                else
                  Logger[F].error(e)("Fail to process non-retryable pubsub message") >> Async[F].delay(event.consumer.ack())
              case ee: WorkbenchException if ee.getMessage.contains("Call to Google API failed") =>
                Logger[F].error(e)("Fail to process retryable pubsub message due to Google API call failure") >> Async[F].delay(event.consumer.nack())
              case _ =>
                Logger[F].error(e)("Fail to process non-retryable pubsub message") >> Async[F].delay(event.consumer.ack())
            }
          case Right(_) => Async[F].delay(event.consumer.ack())
        }
      } yield ()

      res.handleErrorWith {
        e => Logger[F].error(e)("Fail to process pubsub message") >> Async[F].delay(event.consumer.ack())
      }
    }
  }

  val process: Stream[F, Unit] = (subscriber.messages through messageHandler).repeat

  private def handleStopUpdateMessage(message: StopUpdateMessage): F[Unit] =
    dbRef
      .inTransaction { clusterQuery.getClusterById(message.clusterId) }
      .flatMap {
        case Some(resolvedCluster)
            if ClusterStatus.stoppableStatuses.contains(resolvedCluster.status) && !message.updatedMachineConfig.masterMachineType.isEmpty =>
          val followupDetails = ClusterFollowupDetails(message.clusterId, ClusterStatus.Stopped)

          for {
            _ <- Logger[F].info(
              s"stopping cluster ${resolvedCluster.projectNameString} in messageResponder, and saving a record for ${resolvedCluster.id}"
            )
            _ <- dbRef.inTransaction(
              followupQuery.save(followupDetails, message.updatedMachineConfig.masterMachineType)
            )
            _ <- Async[F].liftIO(clusterHelper.stopCluster(resolvedCluster))
          } yield ()
        case Some(resolvedCluster) =>
          Async[F].raiseError(
            PubsubHandleMessageError.ClusterInvalidState(message.clusterId, resolvedCluster.projectNameString, resolvedCluster, message)
          )
        case None =>
          Async[F].raiseError(PubsubHandleMessageError.ClusterNotFound(message.clusterId, message))
      }

  private def handleClusterTransitionFinished(message: ClusterTransitionFinishedMessage): F[Unit] =
    message.clusterFollowupDetails.clusterStatus match {
      case Stopped =>
        for {
          clusterOpt <- dbRef.inTransaction { clusterQuery.getClusterById(message.clusterFollowupDetails.clusterId) }
          savedMasterMachineType <- dbRef.inTransaction {
            followupQuery.getFollowupAction(message.clusterFollowupDetails)
          }
          result <- clusterOpt match {
            case Some(resolvedCluster) if resolvedCluster.status != ClusterStatus.Stopped =>
              Async[F].raiseError[Unit](
                PubsubHandleMessageError.ClusterNotStopped(resolvedCluster.id, resolvedCluster.projectNameString, resolvedCluster.status, message)
              )
            case Some(resolvedCluster) =>
              savedMasterMachineType match {
                case Some(machineType) =>
                  for {
                    // perform gddao and db updates for new resources
                    _ <- Async[F].liftIO(clusterHelper.updateMasterMachineType(resolvedCluster, MachineType(machineType)))
                    // start cluster
                    _ <- Async[F].liftIO(clusterHelper.internalStartCluster(resolvedCluster))
                    // clean-up info from follow-up table
                    _ <- dbRef.inTransaction { followupQuery.delete(message.clusterFollowupDetails) }
                  } yield ()
                case None => Async[F].unit //the database has no record of a follow-up being needed. This is a no-op
              }

            case None =>
              Async[F].raiseError[Unit](PubsubHandleMessageError.ClusterNotFound(message.clusterFollowupDetails.clusterId, message))
          }
        } yield result

      //No actions for other statuses yet. There is some logic that will be needed for all other cases (i.e. the 'None' case where no cluster is found in the db and possibly the case that checks for the data in the DB)
      // TODO: Refactor once there is more than one case
      case _ => Async[F].unit
    }

}

sealed trait LeoPubsubMessage {
  def messageType: String
}

object LeoPubsubMessage {
  final case class StopUpdateMessage(updatedMachineConfig: MachineConfig, clusterId: Long) extends LeoPubsubMessage {
    val messageType = "stopUpdate"
  }

  case class ClusterTransitionFinishedMessage(clusterFollowupDetails: ClusterFollowupDetails) extends LeoPubsubMessage {
    val messageType = "transitionFinished"
  }

  final case class ClusterFollowupDetails(clusterId: Long, clusterStatus: ClusterStatus)
      extends Product
      with Serializable
}

final case class PubsubException(message: String) extends Exception

object LeoPubsubCodec {
  implicit val stopUpdateMessageDecoder: Decoder[StopUpdateMessage] =
    Decoder.forProduct2("updatedMachineConfig", "clusterId")(StopUpdateMessage.apply)

  implicit val clusterFollowupDetailsDecoder: Decoder[ClusterFollowupDetails] =
    Decoder.forProduct2("clusterId", "clusterStatus")(ClusterFollowupDetails.apply)

  implicit val clusterTransitionFinishedDecoder: Decoder[ClusterTransitionFinishedMessage] =
    Decoder.forProduct1("clusterFollowupDetails")(ClusterTransitionFinishedMessage.apply)

  implicit val leoPubsubMessageDecoder: Decoder[LeoPubsubMessage] = Decoder.instance { message =>
    for {
      messageType <- message.downField("messageType").as[String]
      value <- messageType match {
        case "stopUpdate"         => message.as[StopUpdateMessage]
        case "transitionFinished" => message.as[ClusterTransitionFinishedMessage]
        case other                => Left(DecodingFailure(s"found a message with an unknown type when decoding: ${other}", List.empty))
      }
    } yield value
  }

  implicit val stopUpdateMessageEncoder: Encoder[StopUpdateMessage] =
    Encoder.forProduct3("messageType", "updatedMachineConfig", "clusterId")(
      x => (x.messageType, x.updatedMachineConfig, x.clusterId)
    )

  implicit val clusterFollowupDetailsEncoder: Encoder[ClusterFollowupDetails] =
    Encoder.forProduct2("clusterId", "clusterStatus")(x => (x.clusterId, x.clusterStatus))

  implicit val clusterTransitionFinishedEncoder: Encoder[ClusterTransitionFinishedMessage] =
    Encoder.forProduct2("messageType", "clusterFollowupDetails")(x => (x.messageType, x.clusterFollowupDetails))

  implicit val leoPubsubMessageEncoder: Encoder[LeoPubsubMessage] = Encoder.instance { message =>
    message match {
      case m: StopUpdateMessage                => stopUpdateMessageEncoder(m)
      case m: ClusterTransitionFinishedMessage => clusterTransitionFinishedEncoder(m)
    }
  }
}

sealed trait PubsubHandleMessageError extends NoStackTrace {
  def isRetryable: Boolean
}
object PubsubHandleMessageError {
  final case class ClusterNotFound(clusterId: Long, message: LeoPubsubMessage) extends PubsubHandleMessageError {
    override def getMessage: String = s"Unable to process transition finished message ${message} for cluster ${clusterId} because it was not found in the database"
    val isRetryable: Boolean = false
  }
  final case class ClusterNotStopped(clusterId: Long, projectName: String, clusterStatus: ClusterStatus, message: LeoPubsubMessage) extends PubsubHandleMessageError {
    override def getMessage: String = s"Unable to process message ${message} for cluster ${clusterId}/${projectName} in status ${clusterStatus.toString}, when the monitor signalled it stopped as it is not stopped."
    val isRetryable: Boolean = false
  }
  final case class ClusterInvalidState(clusterId: Long, projectName: String, cluster: Cluster, message: LeoPubsubMessage) extends PubsubHandleMessageError {
    override def getMessage: String = s"${clusterId}, ${projectName}, ${message} | This is likely due to a mismatch in state between the db and the message, or an improperly formatted machineConfig in the message. Cluster details: ${cluster}"
    val isRetryable: Boolean = false
  }
}