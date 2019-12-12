package org.broadinstitute.dsde.workbench.leonardo.service

import cats.effect.{ContextShift, IO}
import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.{ProjectTopicName, PubsubMessage}
import org.broadinstitute.dsde.workbench.google2.{GooglePublisher, GooglePublisherInterpreter}
import org.broadinstitute.dsde.workbench.RetryConfig
import fs2.Pipe
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import io.chrisdavenport.log4cats.Logger
import io.circe.{Encoder, Json}
import org.broadinstitute.dsde.workbench.leonardo.MachineConfig

import scala.concurrent.duration._

trait LeoPubsubMessage
final case class StopUpdateMessage(updatedMachineConfig: MachineConfig) extends LeoPubsubMessage

class LeoPublisher(topic: ProjectTopicName)(implicit val log: Logger[IO],
                                             cs: ContextShift[IO])  {

  //TODO parameterize
  val topicName = "leonardo-pubsub"
  val project = "broad-dsde-dev"

  val publisher: Publisher = Publisher.newBuilder(topic).build

  val retryConfig = RetryConfig(1 minute, _ => 1 minute, 5)
  val interpreter = GooglePublisherInterpreter(publisher ,retryConfig)

  def publish(message: LeoPubsubMessage) = {
    message match {
      case StopUpdateMessage(_) => interpreter.publish[StopUpdateMessage](LeoPublisherCodec.StopUpdateMessageEncoder)
    }
  }

}

object LeoPublisherCodec {
  implicit val StopUpdateMessageEncoder: Encoder[StopUpdateMessage] = (message: StopUpdateMessage) => {
    Json.obj(
      ("updatedMachineConfig",machineConfigEncoder(message.updatedMachineConfig))
    )
  }
}

