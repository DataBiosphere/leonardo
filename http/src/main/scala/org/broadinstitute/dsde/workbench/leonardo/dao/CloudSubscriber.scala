package org.broadinstitute.dsde.workbench.leonardo.dao

import fs2.Stream
import org.broadinstitute.dsde.workbench.google2.{Event, GoogleSubscriber}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.model.TraceId

import java.time.Instant

trait CloudSubscriber[F[_], A] {
  def messages: Stream[F, CloudPubsubEvent[A]]
}
object CloudSubscriber {
  implicit def gcpPublisherInstance[F[_]](
    subscriber: GoogleSubscriber[F, LeoPubsubMessage]
  ): CloudSubscriber[F, LeoPubsubMessage] =
    new CloudSubscriber[F, LeoPubsubMessage] {
      override def messages: Stream[F, CloudPubsubEvent[LeoPubsubMessage]] =
        subscriber.messages.map(e => CloudPubsubEvent.GCP(e))
    }

  implicit def azurePublisherInstance[F[_]](
    subscriber: AzureSubscriber[F, LeoPubsubMessage]
  ): CloudSubscriber[F, LeoPubsubMessage] =
    new CloudSubscriber[F, LeoPubsubMessage] {
      override def messages: Stream[F, CloudPubsubEvent[LeoPubsubMessage]] =
        subscriber.messages.map(e => CloudPubsubEvent.Azure(e))
    }
}

sealed trait CloudPubsubEvent[A] extends Product with Serializable {
  def msg: A
  def traceId: Option[TraceId]
  def publishedTime: Instant
}
object CloudPubsubEvent {
  final case class GCP[A](event: Event[A]) extends CloudPubsubEvent[A] {
    override def msg: A = event.msg
    override def traceId: Option[TraceId] = event.traceId
    override def publishedTime: Instant =
      Instant.ofEpochMilli(com.google.protobuf.util.Timestamps.toMillis(event.publishedTime))
  }
  final case class Azure[A](event: AzureEvent[A]) extends CloudPubsubEvent[A] {
    override def msg: A = event.msg
    override def traceId: Option[TraceId] = event.traceId
    override def publishedTime: Instant = event.publishedTime
  }
}
