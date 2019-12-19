package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.effect.Concurrent
import io.chrisdavenport.log4cats.Logger
import cats.effect.{Async, ContextShift, Timer}
import com.google.pubsub.v1.{ProjectSubscriptionName, ProjectTopicName}
import org.broadinstitute.dsde.workbench.google2.{Event, FlowControlSettingsConfig, GoogleSubscriber, GoogleSubscriberInterpreter, SubscriberConfig}
import org.broadinstitute.dsde.workbench.leonardo.config.PubsubConfig
import org.broadinstitute.dsde.workbench.leonardo.service.LeoPubsubMessage
import fs2.{Pipe, Stream}
import cats.effect.{Sync}
import fs2.concurrent.InspectableQueue

import scala.concurrent.duration._

class LeoGoogleSubscriber[F[_]: Async: Timer: ContextShift: Logger: Concurrent](subscriber: GoogleSubscriber[F, LeoPubsubMessage]) {
  //ackDeadline
  //credentials?
  //
//  val topicName = ProjectTopicName.of(pubsubConfig.pubsubGoogleProject.value, pubsubConfig.topicName)
////  val topic = ProjectSubscriptionName.of(pubsubConfig.pubsubGoogleProject.value, "manualTest")
//  val flowConfig: FlowControlSettingsConfig = FlowControlSettingsConfig(1000L, 1000L)
//
////    val subscriber = GoogleSubscriberInterpreter[F, LeoPubsubMessage]
//  val subscriberConfig: SubscriberConfig = SubscriberConfig(pathToCredentialsJson, topicName, 1 minute, None)
//
//  val subscriber = for {
//    queue <- Stream.eval(InspectableQueue.bounded[IO, Event[LeoPubsubMessage]](1000))
//    subscriber <- Stream.resource(GoogleSubscriber.resource(subscriberConfig, queue))
//  } yield ()

  def messageResponder(message: LeoPubsubMessage): F[Unit] = {
    Async[F].unit
  }

  //IO was F originally, changes because it is concrete now
  def messageHandler: Pipe[F, Event[LeoPubsubMessage], Unit] = in => {
    in.flatMap {
      event =>
        for {
          _ <- Stream.eval(messageResponder(event.msg))
          _ <- Stream.eval(Sync[F].delay(event.consumer.ack()))
        } yield ()
    }
  }

  def process: Stream[F, Unit] = subscriber.messages through messageHandler




}
