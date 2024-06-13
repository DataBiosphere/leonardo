package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import fs2.Stream
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubCodec._
import org.broadinstitute.dsde.workbench.leonardo.monitor.{ClusterNodepoolAction, LeoPubsubMessage}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.util2.messaging.CloudPublisher
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
 * dequeue from publisherQueue and transform it into a native google pubsub message;
 * After pubsub message is published, we update database when necessary
 */
final class LeoPublisher[F[_]](
  publisherQueue: Queue[F, LeoPubsubMessage],
  cloudPublisher: CloudPublisher[F]
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
          Stream
            .eval {
              publishMessageAndSetAttributes(event)
                .flatMap(_ => updateDatabase(event))
                .handleErrorWith { t =>
                  val loggingCtx = event.traceId.map(t => Map("traceId" -> t.asString)).getOrElse(Map.empty)
                  logger.error(loggingCtx, t)(
                    s"Failed to publish message of type ${event.messageType.asString}, message: $event"
                  )
                }
            }
            .covary[F]
      }
    Stream(publishingStream, recordMetrics).covary[F].parJoin(2)
  }

  private def publishMessageAndSetAttributes(event: LeoPubsubMessage): F[Unit] =
    for {
      _ <- cloudPublisher.publishOne(event, createAttributes(event))(leoPubsubMessageEncoder, traceIdAsk(event))
      _ <- logger.info(s"Published message of type ${event.messageType.asString}, message: $event")
    } yield ()

  private def traceIdAsk(message: LeoPubsubMessage): Ask[F, TraceId] =
    Ask.const[F, TraceId](message.traceId.getOrElse(TraceId("None")))

  private def createAttributes(message: LeoPubsubMessage): Map[String, String] = {
    val baseAttributes = Map("leonardo" -> "true")
    message.traceId.fold(baseAttributes)(traceId => baseAttributes + ("traceId" -> traceId.asString))
  }

  private def recordMetrics: Stream[F, Unit] = {
    val record = Stream.eval(publisherQueue.size.flatMap(size => metrics.gauge("publisherQueueSize", size)))
    (record ++ Stream.sleep_(30 seconds)).repeat
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
            case Some(ClusterNodepoolAction.CreateCluster(clusterId)) =>
              KubernetesServiceDbQueries
                .markPendingCreating(m.appId, Some(clusterId), None, None)
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
        // We update this in backleo to prevent apps getting stuck in updating as much as possible, https://broadworkbench.atlassian.net/browse/IA-4749
        case _: LeoPubsubMessage.UpdateAppMessage =>
          F.unit
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
