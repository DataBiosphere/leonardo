package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.{Concurrent, Timer}
import cats.implicits._
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import fs2.concurrent.InspectableQueue
import fs2.{Pipe, Stream}
import io.chrisdavenport.log4cats.Logger
import io.circe.syntax._
import org.broadinstitute.dsde.workbench.google2.GooglePublisher
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, DbReference, KubernetesServiceDbQueries}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubCodec._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
 * dequeue from publisherQueue and transform it into a native google pubsub message;
 * After pubsub message is published, we update database when necessary
 */
final class LeoPublisher[F[_]: Logger: Timer](
  publisherQueue: InspectableQueue[F, LeoPubsubMessage],
  googlePublisher: GooglePublisher[F]
)(implicit F: Concurrent[F], dbReference: DbReference[F], metrics: OpenTelemetryMetrics[F], ec: ExecutionContext) {
  val process: Stream[F, Unit] = {
    val publishingStream = Stream.eval(Logger[F].info(s"Initializing publisher")) ++ publisherQueue.dequeue.flatMap {
      event =>
        val publishing = Stream
          .eval(F.pure(event))
          .covary[F] through convertToPubsubMessagePipe through googlePublisher.publishNative
        publishing.evalMap(_ => updateDatabase(event))
    }
    Stream(publishingStream, recordMetrics).covary[F].parJoin(2)
  }

  private def recordMetrics: Stream[F, Unit] = {
    val record = Stream.eval(publisherQueue.getSize.flatMap(size => metrics.gauge("publisherQueueSize", size)))
    (record ++ Stream.sleep_(30 seconds)).repeat
  }

  private def convertToPubsubMessagePipe: Pipe[F, LeoPubsubMessage, PubsubMessage] =
    in =>
      in.map { msg =>
        val stringMessage = msg.asJson.noSpaces
        val byteString = ByteString.copyFromUtf8(stringMessage)
        PubsubMessage
          .newBuilder()
          .setData(byteString)
          .putAttributes("traceId", msg.traceId.map(_.asString).getOrElse("null"))
          .build()
      }

  private def updateDatabase(msg: LeoPubsubMessage): F[Unit] =
    for {
      now <- nowInstant[F]
      _ <- msg match {
        case m: LeoPubsubMessage.CreateRuntimeMessage =>
          clusterQuery.updateClusterStatus(m.runtimeId, RuntimeStatus.Creating, now).transaction
        case m: LeoPubsubMessage.StopRuntimeMessage =>
          clusterQuery.updateClusterStatus(m.runtimeId, RuntimeStatus.Stopping, now).transaction
        case m: LeoPubsubMessage.StartRuntimeMessage =>
          clusterQuery.updateClusterStatus(m.runtimeId, RuntimeStatus.Starting, now).transaction
        case m: LeoPubsubMessage.DeleteRuntimeMessage =>
          clusterQuery.markPendingDeletion(m.runtimeId, now).transaction
        case m: LeoPubsubMessage.DeleteAppMessage =>
          KubernetesServiceDbQueries.markPendingDeletion(m.nodepoolId, m.appId, m.diskId, now).transaction
        case m: LeoPubsubMessage.CreateAppMessage =>
          KubernetesServiceDbQueries.markPendingCreating(m.nodepoolId, m.appId, m.cluster).transaction
        case m: LeoPubsubMessage.BatchNodepoolCreateMessage =>
          KubernetesServiceDbQueries.markPendingCreating(m.clusterId, m.nodepools).transaction
        case _ => F.unit
      }
    } yield ()
}
