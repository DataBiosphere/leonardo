package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.effect.{Async, Concurrent, ContextShift, Timer}
import org.broadinstitute.dsde.workbench.google2.{Event, GoogleSubscriber}
import fs2.{Pipe, Stream}
import io.circe.{Decoder, DecodingFailure, Encoder}
import org.broadinstitute.dsde.workbench.leonardo.MachineConfig
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, followupQuery, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterStatus, MachineType}
import org.broadinstitute.dsde.workbench.leonardo.util.ClusterHelper
import _root_.io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus.Stopped
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.model.WorkbenchException
import cats.implicits._
import scala.concurrent.ExecutionContext

class LeoPubsubMessageSubscriber[F[_]: Async: Timer: ContextShift: Logger: Concurrent](
  subscriber: GoogleSubscriber[F, LeoPubsubMessage],
  clusterHelper: ClusterHelper,
  dbRef: DbReference[F]
)(implicit executionContext: ExecutionContext) {

  def messageResponder(message: LeoPubsubMessage): F[Unit] = {
    val res = message match {
      case msg @ StopUpdateMessage(_, _) =>
        handleStopUpdateMessage(msg)
      case msg @ ClusterTransitionFinishedMessage(_) =>
        handleClusterTransitionFinished(msg)
    }

    res.handleErrorWith {
      case e =>
        for {
          _ <- Logger[F].error(e)(
            s"Unable to process a message in received from pub/sub subscription in messageResponder: ${message}, errorMessage: ${e.getMessage}")
          //clean up the db if there is an error and we have saved records because currently the table's primary key is cluster table's id
          _ <- message match {
            case msg @ ClusterTransitionFinishedMessage(_) =>
              dbRef.inTransaction(followupQuery.delete(msg.clusterFollowupDetails)).void
            case _ => Async[F].unit
          }
        } yield ()
    }
  }

  private def messageHandler: Pipe[F, Event[LeoPubsubMessage], Unit] = in => {
    in.evalMap { event =>
      val res = for {
        _ <- messageResponder(event.msg)
        _ <- Async[F].delay(event.consumer.ack())
      } yield ()

      //This should never fail so that subscriber won't crash
      res.handleErrorWith {
        case e =>
          Logger[F].error(e)("Fail to process pubsub message")
      }
    }
  }

  val process: Stream[F, Unit] = (subscriber.messages through messageHandler).repeat

  private def handleStopUpdateMessage(message: StopUpdateMessage): F[Unit] =
    dbRef
      .inTransaction { clusterQuery.getClusterById(message.clusterId) }
      .flatMap {
        case Some(resolvedCluster)
            if ClusterStatus.stoppableStatuses.contains(resolvedCluster.status) && !message.updatedMachineConfig.masterMachineType.isEmpty => {
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
        }
        case Some(resolvedCluster) =>
          Async[F].raiseError(
            new WorkbenchException(
              s"Failed to process StopUpdateMessage for Cluster ${resolvedCluster.projectNameString}. This is likely due to a mismatch in state between the db and the message, or an improperly formatted machineConfig in the message. Cluster details: ${resolvedCluster}"
            )
          )
        case None =>
          Async[F].raiseError(
            new WorkbenchException(
              s"Could process StopUpdateMessage for cluster with id ${message.clusterId} because it was not found in the database"
            )
          )
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
                new WorkbenchException(
                  s"Unable to process message ${message} for cluster ${resolvedCluster.projectNameString} in status ${resolvedCluster.status.toString}, when the monitor signalled it stopped as it is not stopped."
                )
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
              Async[F].raiseError[Unit](
                new WorkbenchException(
                  s"Unable to process transition finished message ${message} for cluster ${message.clusterFollowupDetails.clusterId} because it was not found in the database"
                )
              )
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
