package org.broadinstitute.dsde.workbench.leonardo.util

import cats.effect.IO
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import scala.concurrent.ExecutionContext.Implicits.global

object QueueFactory {
  implicit val cs = IO.contextShift(global)
  implicit val t = IO.timer(global)
  val mockQueue = InspectableQueue.bounded[IO, LeoPubsubMessage](1000).unsafeRunSync()

  def makeQueue() = {
    InspectableQueue.bounded[IO, LeoPubsubMessage](1000).unsafeRunSync()
  }
}
