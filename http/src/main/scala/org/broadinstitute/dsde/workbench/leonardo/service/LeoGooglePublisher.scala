package org.broadinstitute.dsde.workbench.leonardo.service

import cats.effect.{Async, ContextShift, Timer}
import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.ProjectTopicName
import org.broadinstitute.dsde.workbench.google2.{GooglePublisher, GooglePublisherInterpreter, PublisherConfig}
import org.broadinstitute.dsde.workbench.RetryConfig
import io.chrisdavenport.log4cats.Logger
import io.circe.{Encoder, Json}
import org.broadinstitute.dsde.workbench.leonardo.MachineConfig
import org.broadinstitute.dsde.workbench.leonardo.config.PubsubConfig
import fs2.{Pipe, Stream}

import scala.concurrent.duration._

trait LeoPubsubMessage
final case class StopUpdateMessage(updatedMachineConfig: MachineConfig) extends LeoPubsubMessage

class LeoGooglePublisher[F[_]: Async: Timer: ContextShift: Logger](publisher: GooglePublisher[F]) {

//  val topic = ProjectTopicName.of(pubsubConfig.pubsubGoogleProject.value, pubsubConfig.topicName)
//  val retryConfig = RetryConfig(1 minute, _ => 1 minute, 5) //TODO ?_?
//  val publisherConfig: PublisherConfig = PublisherConfig(pathToCredentialsJson, topic, retryConfig)
//  val publisher = GooglePublisher.resource(publisherConfig)


  def publish(message: LeoPubsubMessage) = {
    Stream.emits(message) through publisher.publish
//    message through publisher.publish
//    val x = message match {
//      case StopUpdateMessage(_) => mes through publisher
//    }

  }

}

object LeoPublisherCodec {
  implicit val machineConfigEncoder: Encoder[MachineConfig] = (config: MachineConfig) => Json.obj(
    //TODO use forProduct7 as in Welder.scala
    ("numberOfWorkers", config.numberOfWorkers.map(Json.fromInt).getOrElse(Json.Null)),
    ("masterMachineType", config.masterMachineType.map(Json.fromString).getOrElse(Json.Null)),
    ("masterDiskSize", config.masterDiskSize.map(Json.fromInt).getOrElse(Json.Null)),
    ("workerMachineType", config.workerMachineType.map(Json.fromString).getOrElse(Json.Null)),
    ("workerDiskSize", config.workerDiskSize.map(Json.fromInt).getOrElse(Json.Null)),
    ("numberOfWorkerLocalSSDs", config.numberOfWorkerLocalSSDs.map(Json.fromInt).getOrElse(Json.Null)),
    ("numberOfPreemptibleWorkers", config.numberOfPreemptibleWorkers.map(Json.fromInt).getOrElse(Json.Null))
  )

  implicit val StopUpdateMessageEncoder: Encoder[StopUpdateMessage] = (message: StopUpdateMessage) => {
    Json.obj(
      ("updatedMachineConfig",machineConfigEncoder(message.updatedMachineConfig))
    )
  }
}

