package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.effect.{Async, Concurrent, ContextShift, IO, Timer}
import org.broadinstitute.dsde.workbench.google2.{Event, GoogleSubscriber}
import fs2.{Pipe, Stream}
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.leonardo.MachineConfig
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterStatus, MachineType}
import org.broadinstitute.dsde.workbench.leonardo.util.ClusterHelper
import _root_.io.chrisdavenport.log4cats.Logger
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus.Stopped
import org.broadinstitute.dsde.workbench.model.WorkbenchException

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class LeoPubsubMessageSubscriber[F[_]: Async: Timer: ContextShift: Logger: Concurrent](
                                                                           subscriber: GoogleSubscriber[IO, LeoPubsubMessage],
                                                                           clusterHelper: ClusterHelper,
                                                                           dbRef: DbReference
                                                                         )(implicit executionContext: ExecutionContext, implicit val cs: ContextShift[IO]) extends LazyLogging {

  //TODO: This may eventually hold things other than MachineConfigs, but for now that is out of scope
  //mutable thread-safe map
  val followupMap: TrieMap[ClusterFollowupDetails, MachineConfig] = new TrieMap[ClusterFollowupDetails, MachineConfig]()
  //ackDeadline
  //credentials?

  def messageResponder(message: LeoPubsubMessage): IO[Unit] =

    message match {
        case msg@StopUpdateMessage(_,_) =>
          handleStopUpdateMessage(msg)
        case msg@ClusterTransitionFinishedMessage(_) =>
          handleClusterTransitionFinished(msg)
        case _ => IO.unit
      }

  def messageHandler: Pipe[IO, Event[LeoPubsubMessage], Unit] = in => {
    in.flatMap { event =>
      for {
        _ <- Stream.eval(messageResponder(event.msg))
//        _ <- Stream.eval(Sync[F].delay(event.consumer.ack()))
        _ <- Stream.eval(IO.pure(event.consumer.ack()))
      } yield ()
    }
  }

  def process(): Stream[IO, Unit] = subscriber.messages through messageHandler


  def handleStopUpdateMessage(message: StopUpdateMessage) = {
    dbRef
      .inTransactionIO { dataAccess =>
        dataAccess.clusterQuery.getClusterById(message.clusterId)
      }
      .flatMap {
        case Some(resolvedCluster)
          if ClusterStatus.stoppableStatuses.contains(resolvedCluster.status) && !message.updatedMachineConfig.masterMachineType.isEmpty => {
            val followupDetails = ClusterFollowupDetails(message.clusterId, ClusterStatus.Stopped)
            followupMap.put(followupDetails, message.updatedMachineConfig)

            logger.info(s"stopping cluster ${resolvedCluster.projectNameString} in messageResponder")
            clusterHelper.stopCluster(resolvedCluster)
          }
        case Some(resolvedCluster) =>
          IO.raiseError(
            new WorkbenchException( s"Failed to process StopUpdateMessage for Cluster ${resolvedCluster.projectNameString}. Cluster details: ${resolvedCluster}")
          )
        case None =>
          IO.raiseError(new WorkbenchException(s"Could process StopUpdateMessage for cluster with id ${message.clusterId} because it was not found in the database"))
      }
  }

  def handleClusterTransitionFinished(message: ClusterTransitionFinishedMessage) = {
    val resolution = message.followupDetails.clusterStatus match {
      case Stopped => {
        dbRef
          .inTransactionIO { dataAccess =>
            dataAccess.clusterQuery.getClusterById(message.followupDetails.clusterId)
          }
          .flatMap {
            case Some(resolvedCluster) if resolvedCluster.status != ClusterStatus.Stopped =>
              IO.raiseError(new WorkbenchException(s"Unable to process message ${message} for cluster ${resolvedCluster.projectNameString} in status ${resolvedCluster.status.toString}, when the monitor signalled it stopped as it is not stopped."))

            case Some(resolvedCluster) if followupMap.contains(message.followupDetails) => {
              // do update
              logger.info("In update of UpdateClusterAfterStop")
              //we always want to delete the follow-up record if there is one saved
              val followupMachineConfig = followupMap.remove(message.followupDetails)
               //if there is a record of a follow-up being needed, perform the follow-up
              if (!followupMachineConfig.isEmpty && !followupMachineConfig.get.masterMachineType.isEmpty) {
                for {
                  // perform gddao and db updates for new resources
                  _ <- clusterHelper.updateMasterMachineType(resolvedCluster, MachineType(followupMachineConfig.get.masterMachineType.get))
                  // start cluster
                  _ <- clusterHelper.internalStartCluster(resolvedCluster)
                } yield ()
              } else IO.raiseError(new WorkbenchException(s"Recieved message ${message} that a cluster finished stopping and wishes to update, " +
                s"but proper followup details (aka machineConfig) were not saved in the subscriber at the time of the initial action. Saved config: ${followupMachineConfig}"))
            }

            case Some(resolvedCluster) =>
             IO.raiseError(
                new WorkbenchException(s"Unable to process message ${message} for Cluster ${resolvedCluster.projectNameString} in status ${resolvedCluster.status} after the monitor signalled it stopped, as no follow-up details were found. ")
              )

            case None => IO.raiseError(new WorkbenchException(s"Could not process ClusterTransitionFinishedMessage because it was not found in the database"))
          }
      }

      //No actions for other statuses yet. There is some logic that will be needed for all other cases (i.e. the 'None' case where no cluster is found in the db and possible the case that checks for a key in the followupMap.
      // TODO: Refactor once there is more than one case
      case _ => {
        logger.info(s"received a message notifying that cluster ${message.followupDetails.clusterId} has transitioned to status ${message.followupDetails.clusterStatus}. This is a Noop.")
        IO.unit
      }
    }

    resolution.unsafeToFuture().failed
      .foreach { e =>
        logger.error(s"Error occurred updating cluster with id ${message.followupDetails.clusterId} after stopping", e)
      }

    resolution
  }

}
sealed trait LeoPubsubMessage {
  def messageType: String
}

final case class ClusterFollowupDetails(clusterId: Long, clusterStatus: ClusterStatus) extends Product with Serializable

abstract class ClusterTransitionRequestedMessage(updatedMachineConfig: MachineConfig, clusterId: Long)  extends LeoPubsubMessage

final case class StopUpdateMessage(updatedMachineConfig: MachineConfig, clusterId: Long)
  extends ClusterTransitionRequestedMessage(updatedMachineConfig: MachineConfig, clusterId: Long) {
  val messageType = "stopUpdate"
}

case class ClusterTransitionFinishedMessage(followupDetails: ClusterFollowupDetails) extends LeoPubsubMessage {
  val messageType = "transitionFinished"
}

final case class PubsubException(message: String) extends Exception

object LeoPubsubCodec {
  implicit val stopUpdateMessageDecoder: Decoder[StopUpdateMessage] =
    Decoder.forProduct2("updatedMachineConfig", "clusterId")(StopUpdateMessage.apply)

  implicit val clusterFollowupDetailsDecoder: Decoder[ClusterFollowupDetails] =
    Decoder.forProduct2("clusterId", "clusterStatus")(ClusterFollowupDetails.apply)

  implicit val clusterTransitionFinishedDecoder: Decoder[ClusterTransitionFinishedMessage] =
    Decoder.forProduct1("followupDetails")(ClusterTransitionFinishedMessage.apply)

  implicit val leoPubsubMessageDecoder: Decoder[LeoPubsubMessage] = {
    for {
      messageType <- Decoder[String].prepare(_.downField("messageType"))
      value <- messageType match {
        case "stopUpdate" => Decoder[StopUpdateMessage]
        case "transitionFinished" => Decoder[ClusterTransitionFinishedMessage]
        case other => throw PubsubException(s"found a message with an unknown type when decoding: ${other}")
      }
    } yield value
  }

  implicit val machineConfigEncoder: Encoder[MachineConfig] =
    Encoder.forProduct7("numberOfWorkers",
      "masterMachineType",
      "masterDiskSize",
      "workerMachineType",
      "workerDiskSize",
      "numberOfWorkerLocalSSDs",
      "numberOfPreemptibleWorkers")(x => MachineConfig.unapply(x).get)

  implicit val stopUpdateMessageEncoder: Encoder[StopUpdateMessage] =
    Encoder.forProduct2("messageType", "updatedMachineConfig")(x => (x.updatedMachineConfig, x.clusterId))

  implicit val clusterFollowupDetailsEncoder: Encoder[ClusterFollowupDetails] =
    Encoder.forProduct2("clusterId", "clusterStatus")(x => (x.clusterId, x.clusterStatus))

  implicit val clusterTransitionFinishedEncoder: Encoder[ClusterTransitionFinishedMessage] =
    Encoder.forProduct1("followupDetails")(x => x.followupDetails)

  implicit val leoPubsubMessageEncoder: Encoder[LeoPubsubMessage] = (message: LeoPubsubMessage) => {
    message.messageType match {
      case "stopUpdate" => stopUpdateMessageEncoder(message.asInstanceOf[StopUpdateMessage])
      case "transitionFinished" => clusterTransitionFinishedEncoder(message.asInstanceOf[ClusterTransitionFinishedMessage])
      case other        => throw PubsubException(s"found a message with an unknown type when encoding: ${other}")
    }
  }
}
