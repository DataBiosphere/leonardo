package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.{IO, Resource}
import ch.qos.logback.core.net.QueueFactory
import fs2.concurrent.InspectableQueue
import fs2.Stream
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.google2.{Event, GooglePublisher, GoogleSubscriber}
import org.broadinstitute.dsde.workbench.leonardo.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.notebooks.Welder
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.time.{Minutes, Span}
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, fixture}

@DoNotDiscover
class LeoPubsubSpec extends ClusterFixtureSpec with BeforeAndAfterAll with LeonardoTestUtils {

  "Google publisher should be able to auth" taggedAs Tags.SmokeTest in { clusterFixture =>
    val publisher = GooglePublisher.resource[IO, String](LeonardoConfig.Leonardo.publisherConfig)
    publisher.use {
      _ => IO.unit
    }
  }

  "Google publisher should publish" in { _ =>
    import org.broadinstitute.dsde.workbench.leonardo.LeoPubsubCodec._
    val publisher =  GooglePublisher.resource[IO, String](LeonardoConfig.Leonardo.publisherConfig)
    val queue =  InspectableQueue.bounded[IO, String](100).unsafeRunSync()

    publisher.use { publisher =>
      (queue.dequeue through publisher.publish)
        .compile
        .drain
    }
      .unsafeRunSync()

    queue.enqueue1("automation-test-message").unsafeRunSync()

    eventually(timeout(Span(2, Minutes))) {
      val size  = queue.getSize.unsafeRunSync()
      size shouldBe 0
    }

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
      case other => throw PubsubException(s"found a message with an unknown type when encoding: ${other}")
    }
  }
}
