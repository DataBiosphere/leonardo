package org.broadinstitute.dsde.workbench.leonardo.util

import cats.effect.IO
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.google2.Event
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage

import scala.concurrent.ExecutionContext.Implicits.global

object QueueFactory {
  implicit val cs = IO.contextShift(global)
  implicit val t = IO.timer(global)
  val queueSize = 1000

  def makePublisherQueue() =
    InspectableQueue.bounded[IO, LeoPubsubMessage](queueSize).unsafeRunSync()

  def makeSubscriberQueue() =
    InspectableQueue.bounded[IO, Event[LeoPubsubMessage]](queueSize).unsafeRunSync()
}
