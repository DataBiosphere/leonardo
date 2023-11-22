package org.broadinstitute.dsde.workbench.leonardo.dao

import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import fs2.Pipe
import io.circe.syntax._
import org.broadinstitute.dsde.workbench.google2.GooglePublisher
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubCodec._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage

sealed trait CloudTopicPublisher[F[_]] {
  def publishMessagePipe: Pipe[F, LeoPubsubMessage, Unit]
}

object CloudTopicPublisher {
  implicit def azurePublisherInstance[F[_]](azurePublisher: AzurePublisher[F]): CloudTopicPublisher[F] =
    ???
  implicit def gcpPublisherInstance[F[_]](googlePublisher: GooglePublisher[F]): CloudTopicPublisher[F] =
    new CloudTopicPublisher[F] {
      override def publishMessagePipe: Pipe[F, LeoPubsubMessage, Unit] =
        convertToPubsubMessagePipe.andThen(googlePublisher.publishNative)

      private def convertToPubsubMessagePipe: Pipe[F, LeoPubsubMessage, PubsubMessage] =
        in =>
          in.map { msg =>
            val stringMessage = msg.asJson.noSpaces
            val byteString = ByteString.copyFromUtf8(stringMessage)
            PubsubMessage
              .newBuilder()
              .setData(byteString)
              .putAttributes("traceId", msg.traceId.map(_.asString).getOrElse("null"))
              .putAttributes("leonardo", "true")
              .build()
          }
    }
}
