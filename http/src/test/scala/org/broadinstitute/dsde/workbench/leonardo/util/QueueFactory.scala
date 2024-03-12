package org.broadinstitute.dsde.workbench.leonardo.util

import cats.effect.IO
import cats.effect.std.Queue
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.monitor.{LeoPubsubMessage, UpdateDateAccessedMessage}
import org.broadinstitute.dsde.workbench.util2.messaging.ReceivedMessage

object QueueFactory {
  val queueSize = 1000

  def makePublisherQueue() =
    Queue.bounded[IO, LeoPubsubMessage](queueSize).unsafeRunSync()

  def makeSubscriberQueue() =
    Queue.bounded[IO, ReceivedMessage[LeoPubsubMessage]](queueSize).unsafeRunSync()

  def asyncTaskQueue() = Queue.bounded[IO, Task[IO]](queueSize).unsafeRunSync()

  def makeDateAccessedQueue() = Queue.bounded[IO, UpdateDateAccessedMessage](10).unsafeRunSync()
}
