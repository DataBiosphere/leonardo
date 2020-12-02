package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.{Concurrent, Timer}
import cats.syntax.all._
import cats.mtl.Ask
import fs2.Stream
import io.chrisdavenport.log4cats.StructuredLogger
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.google2.{Event, GoogleComputeService, GoogleSubscriber, InstanceName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, kubernetesClusterQuery, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.monitor.NonLeoMessage.{CryptoMining, DeleteKubernetesClusterMessage}
import org.broadinstitute.dsde.workbench.leonardo.util.{DeleteClusterParams, GKEAlgebra}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * This is subscriber for messages that are not published by Leonardo itself.
 * But rather from cron jobs, or cloud logging sink (triggered from crypto-mining activity)
 */
class NonLeoMessageSubscriber[F[_]: Timer](gkeAlg: GKEAlgebra[F],
                                           computeService: GoogleComputeService[F],
                                           subscriber: GoogleSubscriber[F, NonLeoMessage])(
  implicit logger: StructuredLogger[F],
  F: Concurrent[F],
  metrics: OpenTelemetryMetrics[F],
  dbRef: DbReference[F]
) {
  val process: Stream[F, Unit] = subscriber.messages
    .parEvalMapUnordered(10)(messageHandler)
    .handleErrorWith(error => Stream.eval(logger.error(error)("Failed to initialize message processor")))

  private[monitor] def messageHandler(event: Event[NonLeoMessage]): F[Unit] = {
    val traceId = event.traceId.getOrElse(TraceId("None"))
    for {
      now <- nowInstant[F]
      implicit0(ev: Ask[F, AppContext]) <- F.pure(Ask.const[F, AppContext](AppContext(traceId, now, None)))
      _ <- metrics.incrementCounter(s"NonLeoPubSub/${event.msg.messageType}")
      res <- messageResponder(event.msg).attempt
      _ <- res match {
        case Left(e)  => logger.error(e)("Fail to process pubsub message") >> F.delay(event.consumer.nack())
        case Right(_) => F.delay(event.consumer.ack())
      }
    } yield ()
  }

  private[monitor] def messageResponder(
    message: NonLeoMessage
  )(implicit traceId: Ask[F, AppContext]): F[Unit] = message match {
    case m: DeleteKubernetesClusterMessage => handleDeleteKubernetesClusterMessage(m)
    case m: NonLeoMessage.CryptoMining     => handleCryptoMiningMessage(m)
  }

  private[monitor] def handleDeleteKubernetesClusterMessage(msg: DeleteKubernetesClusterMessage)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      clusterId = msg.clusterId
      _ <- kubernetesClusterQuery.markPendingDeletion(clusterId).transaction
      _ <- gkeAlg
      // TODO: Should we retry failures and with what RetryConfig? If all retries fail, send an alert?
        .deleteAndPollCluster(DeleteClusterParams(msg.clusterId, msg.project))
        .onError {
          case _ =>
            for {
              _ <- logger.error(
                s"An error occurred during clean-up of cluster ${clusterId} in project ${msg.project}. | trace id: ${ctx.traceId}"
              )
              _ <- kubernetesClusterQuery.updateStatus(clusterId, KubernetesClusterStatus.Error).transaction
              // TODO: Create a KUBERNETES_CLUSTER_ERROR table to log the error message?
              // TODO: Need mark the nodepool(s) as Error'ed too?
            } yield ()
        }
    } yield ()

  private[monitor] def handleCryptoMiningMessage(
    msg: NonLeoMessage.CryptoMining
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- if (!msg.textPayload.contains(
                 "CRYPTOMINING_DETECTED"
               )) // needs to match https://github.com/broadinstitute/terra-cryptomining-security-alerts/blob/master/v2/main.go#L24
        F.unit
      else
        for {
          runtimeFromGoogle <- computeService.getInstance(msg.resource.labels.projectId,
                                                          msg.resource.labels.zone,
                                                          InstanceName(msg.resource.labels.instanceId.toString))
          // We mark the runtime as Deleted, and delete the instance.
          // If instance deletion fails for some reason, it will be cleaned up by resource-validator
          _ <- runtimeFromGoogle.traverse { instance =>
            for {
              _ <- clusterQuery
                .markDeleted(msg.resource.labels.projectId,
                             RuntimeName(instance.getName),
                             ctx.now,
                             Some("cryptomining"))
                .transaction
              _ <- computeService.deleteInstance(msg.resource.labels.projectId,
                                                 msg.resource.labels.zone,
                                                 InstanceName(msg.resource.labels.instanceId.toString))
            } yield ()
          }
        } yield ()
    } yield ()
}

object NonLeoMessageSubscriber {
  implicit val deleteKubernetesClusterDecoder: Decoder[DeleteKubernetesClusterMessage] =
    Decoder.forProduct2("clusterId", "project")(DeleteKubernetesClusterMessage.apply)

  implicit val googleLabelsDecoder: Decoder[GoogleLabels] =
    Decoder.forProduct3("instance_id", "project_id", "zone")(GoogleLabels.apply)
  implicit val googleResourceDecoder: Decoder[GoogleResource] = Decoder.forProduct1("labels")(GoogleResource.apply)
  Decoder.forProduct2("textPayload", "resource")(CryptoMining.apply)
  implicit val cryptoMiningDecoder: Decoder[CryptoMining] =
    Decoder.forProduct2("textPayload", "resource")(CryptoMining.apply)

  implicit val nonLeoMessageDecoder: Decoder[NonLeoMessage] = Decoder.instance { x =>
    deleteKubernetesClusterDecoder.tryDecode(x) orElse (cryptoMiningDecoder.tryDecode(x))
  }
}

sealed abstract class NonLeoMessage extends Product with Serializable {
  def messageType: String
}
object NonLeoMessage {
  final case class DeleteKubernetesClusterMessage(clusterId: KubernetesClusterLeoId, project: GoogleProject)
      extends NonLeoMessage {
    val messageType: String = "delete-k8s-cluster"
  }
  final case class CryptoMining(textPayload: String, resource: GoogleResource) extends NonLeoMessage {
    val messageType: String = "crypto-minining"
  }
}
final case class GoogleResource(labels: GoogleLabels)
final case class GoogleLabels(instanceId: Long, projectId: GoogleProject, zone: ZoneName)
