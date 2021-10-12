package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.Async
import cats.syntax.all._
import cats.mtl.Ask
import fs2.Stream
import cats.effect.std.Queue
import org.typelevel.log4cats.StructuredLogger
import io.circe.{Decoder, DecodingFailure, Encoder}
import org.broadinstitute.dsde.workbench.google2.{
  Event,
  GoogleComputeService,
  GooglePublisher,
  GoogleSubscriber,
  InstanceName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.dao.{SamDAO, UserSubjectId}
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, kubernetesClusterQuery, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.monitor.NonLeoMessage.{
  CryptoMining,
  DeleteKubernetesClusterMessage,
  DeleteNodepoolMessage
}
import org.broadinstitute.dsde.workbench.leonardo.util.{DeleteClusterParams, DeleteNodepoolParams, GKEAlgebra}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.google2.JsonCodec.traceIdDecoder
import org.broadinstitute.dsde.workbench.leonardo.ErrorAction.DeleteNodepool
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import NonLeoMessageSubscriber.cryptominingUserMessageEncoder
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import io.circe.syntax._

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * This is subscriber for messages that are not published by Leonardo itself.
 * But rather from cron jobs, or cloud logging sink (triggered from crypto-mining activity)
 */
class NonLeoMessageSubscriber[F[_]](gkeAlg: GKEAlgebra[F],
                                    computeService: GoogleComputeService[F],
                                    samDao: SamDAO[F],
                                    subscriber: GoogleSubscriber[F, NonLeoMessage],
                                    publisher: GooglePublisher[F],
                                    asyncTasks: Queue[F, Task[F]])(
  implicit logger: StructuredLogger[F],
  F: Async[F],
  metrics: OpenTelemetryMetrics[F],
  dbRef: DbReference[F]
) {
  val process: Stream[F, Unit] = subscriber.messages
    .parEvalMapUnordered(10)(messageHandler)
    .handleErrorWith(error => Stream.eval(logger.error(error)("Failed to initialize message processor")))

  private[monitor] def messageHandler(event: Event[NonLeoMessage]): F[Unit] = {
    val traceId = event.traceId.getOrElse(TraceId("None"))
    for {
      now <- F.realTimeInstant
      implicit0(ev: Ask[F, AppContext]) <- F.pure(Ask.const[F, AppContext](AppContext(traceId, now, "", None)))
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
    case m: DeleteNodepoolMessage          => handleDeleteNodepoolMessage(m)
  }

  private[monitor] def handleDeleteKubernetesClusterMessage(msg: DeleteKubernetesClusterMessage)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      clusterId = msg.clusterId
      _ <- logger.info(ctx.loggingCtx)(
        s"Beginning to delete kubernetes cluster ${clusterId} in project ${msg.project} from NonLeoMessageSubscriber"
      )
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
          runtimeFromGoogle <- computeService.getInstance(msg.googleProject,
                                                          msg.resource.labels.zone,
                                                          InstanceName(msg.resource.labels.instanceId.toString))
          // We mark the runtime as Deleted, and delete the instance.
          // If instance deletion fails for some reason, it will be cleaned up by resource-validator
          _ <- runtimeFromGoogle.traverse { instance =>
            for {
              runtimeInfo <- clusterQuery
                .getActiveClusterRecordByName(msg.googleProject, RuntimeName(instance.getName))
                .transaction
              _ <- runtimeInfo.traverse { runtime =>
                for {
                  _ <- clusterQuery
                    .markDeleted(msg.googleProject, RuntimeName(instance.getName), ctx.now, Some("cryptomining"))
                    .transaction
                  _ <- computeService.deleteInstance(msg.googleProject,
                                                     msg.resource.labels.zone,
                                                     InstanceName(msg.resource.labels.instanceId.toString))
                  userSubjectId <- samDao.getUserSubjectId(runtime.auditInfo.creator, runtime.googleProject)
                  _ <- userSubjectId.traverse { sid =>
                    val byteString = ByteString.copyFromUtf8(CryptominingUserMessage(sid).asJson.noSpaces)

                    val message = PubsubMessage
                      .newBuilder()
                      .setData(byteString)
                      .putAttributes("traceId", ctx.traceId.asString)
                      .putAttributes("cryptomining", "true")
                      .build()
                    publisher.publishNativeOne(message)
                  }
                } yield ()
              }
            } yield ()
          }
        } yield ()
    } yield ()

  private[monitor] def handleDeleteNodepoolMessage(
    msg: DeleteNodepoolMessage
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      task = gkeAlg.deleteAndPollNodepool(DeleteNodepoolParams(msg.nodepoolId, msg.googleProject))
      _ <- asyncTasks.offer(
        Task(ctx.traceId,
             task,
             Some(logError(s"${msg.nodepoolId}/${msg.googleProject}", DeleteNodepool.toString)),
             ctx.now)
      )
    } yield ()

  private def logError(resourceAndProject: String, action: String): Throwable => F[Unit] =
    t => logger.error(t)(s"Error in async task for resourceId/googleProject: ${resourceAndProject} | action: ${action}")

}

object NonLeoMessageSubscriber {
  implicit val deleteKubernetesClusterDecoder: Decoder[DeleteKubernetesClusterMessage] =
    Decoder.forProduct2("clusterId", "project")(DeleteKubernetesClusterMessage.apply)

  implicit val googleLabelsDecoder: Decoder[GoogleLabels] =
    Decoder.forProduct2("instance_id", "zone")(GoogleLabels.apply)
  implicit val googleResourceDecoder: Decoder[GoogleResource] = Decoder.forProduct1("labels")(GoogleResource.apply)
  implicit val cryptoMiningDecoder: Decoder[CryptoMining] = Decoder.instance { c =>
    for {
      textpayload <- c.downField("textPayload").as[String]
      resource <- c.downField("resource").as[GoogleResource]
      logName <- c.downField("logName").as[String]
      // logName looks like `projects/general-dev-billing-account/logs/cryptomining`
      // we're extracting google project from logName instead of `labels` because `labels` are easier to spoof.
      // We're using instance id from `labels` which is relatively okay because worst case is we're deleting an instance
      // in this user's own billing project
      splitted = logName.split("/")
      googleProject <- Either
        .catchNonFatal(GoogleProject(splitted(1)))
        .leftMap(t => DecodingFailure(s"can't parse google project from logName due to ${t}", List.empty))
    } yield CryptoMining(textpayload, resource, googleProject)
  }
  implicit val deleteNodepoolDecoder: Decoder[DeleteNodepoolMessage] =
    Decoder.forProduct3("nodepoolId", "googleProject", "traceId")(DeleteNodepoolMessage.apply)

  implicit val nonLeoMessageDecoder: Decoder[NonLeoMessage] = Decoder.instance { x =>
    deleteKubernetesClusterDecoder.tryDecode(x) orElse (cryptoMiningDecoder.tryDecode(x)) orElse (deleteNodepoolDecoder
      .tryDecode(x))
  }

  implicit val userSubjectIdEncoder: Encoder[UserSubjectId] = Encoder.encodeString.contramap(_.asString)
  implicit val cryptominingUserMessageEncoder: Encoder[CryptominingUserMessage] =
    Encoder.forProduct1("userSubjectId")(x => CryptominingUserMessage.unapply(x).get)
}

sealed abstract class NonLeoMessage extends Product with Serializable {
  def messageType: String
}
object NonLeoMessage {
  final case class DeleteKubernetesClusterMessage(clusterId: KubernetesClusterLeoId, project: GoogleProject)
      extends NonLeoMessage {
    val messageType: String = "deleteKubernetesCluster"
  }
  final case class CryptoMining(textPayload: String, resource: GoogleResource, googleProject: GoogleProject)
      extends NonLeoMessage {
    val messageType: String = "crypto-minining"
  }
  final case class DeleteNodepoolMessage(nodepoolId: NodepoolLeoId,
                                         googleProject: GoogleProject,
                                         traceId: Option[TraceId])
      extends NonLeoMessage {
    val messageType: String = "deleteNodepool"
  }
}
final case class GoogleResource(labels: GoogleLabels)
final case class GoogleLabels(instanceId: Long, zone: ZoneName)

final case class CryptominingUserMessage(userSubjectId: UserSubjectId)
