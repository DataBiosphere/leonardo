package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.Async
import cats.effect.std.Queue
import cats.syntax.all._
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import fs2.{Pipe, Stream}
import io.circe.syntax._
import org.broadinstitute.dsde.workbench.google2.GooglePublisher
import org.broadinstitute.dsde.workbench.leonardo.db.{
  appQuery,
  clusterQuery,
  persistentDiskQuery,
  DbReference,
  KubernetesServiceDbQueries
}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubCodec._
import org.broadinstitute.dsde.workbench.leonardo.monitor.{ClusterNodepoolAction, LeoPubsubMessage}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
 * dequeue from publisherQueue and transform it into a native google pubsub message;
 * After pubsub message is published, we update database when necessary
 */
final class LeoPublisher[F[_]](
  publisherQueue: Queue[F, LeoPubsubMessage],
  publisher: CloudTopicPublisher[F]
)(implicit
  F: Async[F],
  dbReference: DbReference[F],
  metrics: OpenTelemetryMetrics[F],
  ec: ExecutionContext,
  logger: StructuredLogger[F]
) {
  val process: Stream[F, Unit] = {
    val publishingStream =
      Stream.eval(logger.info(s"Initializing publisher")) ++ Stream.fromQueueUnterminated(publisherQueue).flatMap {
        event =>
          val publishStream = publisher match {
            case CloudTopicPublisher.Azure(_) =>
              Stream.raiseError(new NotImplementedError("Azure publisher is not implemented yet"))

            case CloudTopicPublisher.Gcp(googlePublisher) =>
              Stream
                .eval(F.pure(event))
                .covary[F]
                .through(convertToPubsubMessagePipe)
                .through(googlePublisher.publishNative)
          }

          publishStream
            .evalMap(_ => updateDatabase(event))
            .handleErrorWith { t =>
              val loggingCtx = event.traceId.map(t => Map("traceId" -> t.asString)).getOrElse(Map.empty)
              Stream
                .eval(
                  logger.error(loggingCtx, t)(
                    s"Failed to publish message of type ${event.messageType.asString}, message: $event"
                  )
                )
            }
      }
    Stream(publishingStream, recordMetrics).covary[F].parJoin(2)
  }

  private def recordMetrics: Stream[F, Unit] = {
    val record = Stream.eval(publisherQueue.size.flatMap(size => metrics.gauge("publisherQueueSize", size)))
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
          .putAttributes("leonardo", "true")
          .build()
      }

  private def updateDatabase(msg: LeoPubsubMessage): F[Unit] =
    for {
      now <- F.realTimeInstant
      _ <- msg match {
        case m: LeoPubsubMessage.CreateRuntimeMessage =>
          clusterQuery.updateClusterStatus(m.runtimeId, RuntimeStatus.Creating, now).transaction
        case m: LeoPubsubMessage.CreateAzureRuntimeMessage =>
          clusterQuery.updateClusterStatus(m.runtimeId, RuntimeStatus.Creating, now).transaction
        case m: LeoPubsubMessage.DeleteAzureRuntimeMessage =>
          clusterQuery.updateClusterStatus(m.runtimeId, RuntimeStatus.Deleting, now).transaction
        case m: LeoPubsubMessage.CreateDiskMessage =>
          persistentDiskQuery.updateStatus(m.diskId, DiskStatus.Creating, now).transaction
        case m: LeoPubsubMessage.DeleteDiskMessage =>
          persistentDiskQuery.updateStatus(m.diskId, DiskStatus.Deleting, now).transaction
        case m: LeoPubsubMessage.DeleteDiskV2Message =>
          persistentDiskQuery.updateStatus(m.diskId, DiskStatus.Deleting, now).transaction
        case m: LeoPubsubMessage.StopRuntimeMessage =>
          clusterQuery.updateClusterStatus(m.runtimeId, RuntimeStatus.Stopping, now).transaction
        case m: LeoPubsubMessage.StartRuntimeMessage =>
          clusterQuery.updateClusterStatus(m.runtimeId, RuntimeStatus.Starting, now).transaction
        case m: LeoPubsubMessage.DeleteRuntimeMessage =>
          clusterQuery.markPendingDeletion(m.runtimeId, now).transaction
        case m: LeoPubsubMessage.DeleteAppMessage =>
          KubernetesServiceDbQueries.markPendingAppDeletion(m.appId, m.diskId, now).transaction
        case m: LeoPubsubMessage.CreateAppMessage =>
          m.clusterNodepoolAction match {
            case Some(ClusterNodepoolAction.CreateClusterAndNodepool(clusterId, defaultNodepoolId, nodepoolId)) =>
              KubernetesServiceDbQueries
                .markPendingCreating(m.appId, Some(clusterId), Some(defaultNodepoolId), Some(nodepoolId))
                .transaction
            case Some(ClusterNodepoolAction.CreateNodepool(nodepoolId)) =>
              KubernetesServiceDbQueries
                .markPendingCreating(m.appId, None, None, Some(nodepoolId))
                .transaction
            case None =>
              KubernetesServiceDbQueries
                .markPendingCreating(m.appId, None, None, None)
                .transaction
          }
        case m: LeoPubsubMessage.StopAppMessage =>
          appQuery.updateStatus(m.appId, AppStatus.Stopping).transaction
        case m: LeoPubsubMessage.StartAppMessage =>
          appQuery.updateStatus(m.appId, AppStatus.Starting).transaction
        case m: LeoPubsubMessage.UpdateAppMessage =>
          appQuery.updateStatus(m.appId, AppStatus.Updating).transaction
        case _: LeoPubsubMessage.UpdateDiskMessage =>
          F.unit
        case _: LeoPubsubMessage.UpdateRuntimeMessage =>
          F.unit
        case m: LeoPubsubMessage.CreateAppV2Message =>
          KubernetesServiceDbQueries
            .markPendingCreating(m.appId, None, None, None)
            .transaction
        case m: LeoPubsubMessage.DeleteAppV2Message =>
          KubernetesServiceDbQueries
            .markPendingAppDeletion(m.appId, m.diskId, now)
            .transaction
      }
    } yield ()
}

sealed trait CloudTopicPublisher[F[_]] extends Product with Serializable
object CloudTopicPublisher {
  final case class Azure[F[_]](azurePublisher: AzurePublisher[F]) extends CloudTopicPublisher[F]
  final case class Gcp[F[_]](googlePublisher: GooglePublisher[F]) extends CloudTopicPublisher[F]
}

trait AzurePublisher[F[_]] // TODO: Jesus will fill this in and move it to a better place
