package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.effect.Concurrent
import io.chrisdavenport.log4cats.Logger
import cats.effect.{Async, ContextShift, Timer}
import org.broadinstitute.dsde.workbench.google2.{Event, GoogleSubscriber}
import fs2.{Pipe, Stream}
import cats.effect.Sync
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.leonardo.MachineConfig
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._

class MessageReader[F[_]: Async: Timer: ContextShift: Logger: Concurrent](
  subscriber: GoogleSubscriber[F, LeoPubsubMessage]
) {
  //ackDeadline
  //credentials?
  //
//  val topicName = ProjectTopicName.of(pubsubConfig.pubsubGoogleProject.value, pubsubConfig.topicName)
////  val topic = ProjectSubscriptionName.of(pubsubConfig.pubsubGoogleProject.value, "manualTest")
//  val flowConfig: FlowControlSettingsConfig = FlowControlSettingsConfig(1000L, 1000L)
//
////    val subscriber = GoogleSubscriberInterpreter[F, LeoPubsubMessage]
//  val subscriberConfig: SubscriberConfig = SubscriberConfig(pathToCredentialsJson, topicName, 1 minute, None)
//
//  val subscriber = for {
//    queue <- Stream.eval(InspectableQueue.bounded[IO, Event[LeoPubsubMessage]](1000))
//    subscriber <- Stream.resource(GoogleSubscriber.resource(subscriberConfig, queue))
//  } yield ()

  def messageResponder(message: LeoPubsubMessage): F[Unit] =
    Async[F].unit

  //IO was F originally, changes because it is concrete now
  def messageHandler: Pipe[F, Event[LeoPubsubMessage], Unit] = in => {
    in.flatMap { event =>
      for {
        _ <- Stream.eval(messageResponder(event.msg))
        _ <- Stream.eval(Sync[F].delay(event.consumer.ack()))
      } yield ()
    }
  }

  def process(): Stream[F, Unit] = subscriber.messages through messageHandler

}

sealed trait LeoPubsubMessage {
  def messageType: String
}
final case class StopUpdateMessage(updatedMachineConfig: MachineConfig, clusterId: Long) extends LeoPubsubMessage {
  val messageType = "stopUpdate"
}

final case class PubsubException(message: String) extends Exception

object LeoPubsubCodec {
  implicit val stopUpdateMessageDecoder: Decoder[StopUpdateMessage] = {
    Decoder.forProduct2("updatedMachineConfig", "clusterId")(StopUpdateMessage.apply)
  }

  implicit val leoPubsubMessageDecoder: Decoder[LeoPubsubMessage] = {
    for {
      messageType <- Decoder[String].prepare(_.downField("messageType"))
      value <- messageType match {
        case "stopUpdate" => Decoder[StopUpdateMessage]
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

  implicit val leoPubsubMessageEncoder: Encoder[LeoPubsubMessage] = (message: LeoPubsubMessage) => {
    message.messageType match {
      case "stopUpdate" => stopUpdateMessageEncoder(message.asInstanceOf[StopUpdateMessage])
      case other =>throw PubsubException(s"found a message with an unknown type when encoding: ${other}")
    }
  }
}


