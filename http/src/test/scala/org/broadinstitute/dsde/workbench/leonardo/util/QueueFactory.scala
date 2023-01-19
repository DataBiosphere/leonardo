package org.broadinstitute.dsde.workbench.leonardo.util

import cats.effect.IO
import cats.effect.std.Queue
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.google2.Event
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.monitor.{LeoPubsubMessage, UpdateDateAccessMessage}

object QueueFactory {
  val queueSize = 1000

  def makePublisherQueue() =
    Queue.bounded[IO, LeoPubsubMessage](queueSize).unsafeRunSync()

  def makeSubscriberQueue() =
    Queue.bounded[IO, Event[LeoPubsubMessage]](queueSize).unsafeRunSync()

  def asyncTaskQueue() = Queue.bounded[IO, Task[IO]](queueSize).unsafeRunSync()

  def makeDateAccessedQueue() = Queue.bounded[IO, UpdateDateAccessMessage](10).unsafeRunSync()
}
