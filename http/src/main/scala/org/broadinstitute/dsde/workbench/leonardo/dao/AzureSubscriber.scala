package org.broadinstitute.dsde.workbench.leonardo.dao

import fs2.Stream
import org.broadinstitute.dsde.workbench.model.TraceId

import java.time.Instant

//TODO: Jesus will fill this out
trait AzureSubscriber[F[_], A] {
  def messages: Stream[F, AzureEvent[A]]
}

final case class AzureEvent[A](msg: A, traceId: Option[TraceId] = None, publishedTime: Instant)
