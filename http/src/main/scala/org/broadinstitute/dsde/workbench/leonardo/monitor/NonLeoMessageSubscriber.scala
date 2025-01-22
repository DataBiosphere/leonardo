package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import fs2.Stream
import io.circe.{Decoder, DecodingFailure, Encoder}
import org.broadinstitute.dsde.workbench.google2.JsonCodec.traceIdDecoder
import org.broadinstitute.dsde.workbench.google2.{DeviceName, GoogleComputeService, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.{Task, TaskMetricsTags}
import org.broadinstitute.dsde.workbench.leonardo.ErrorAction.DeleteNodepool
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.dao.sam.SamService
import org.broadinstitute.dsde.workbench.leonardo.dao.{SamDAO, UserSubjectId}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.{ctxConversion, dbioToIO}
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.monitor.NonLeoMessage.{
  CryptoMining,
  CryptoMiningScc,
  DeleteKubernetesClusterMessage,
  DeleteNodepoolMessage
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.NonLeoMessageSubscriber.cryptominingUserMessageEncoder
import org.broadinstitute.dsde.workbench.leonardo.util.{DeleteClusterParams, DeleteNodepoolParams, GKEAlgebra}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.util2.InstanceName
import org.broadinstitute.dsde.workbench.util2.messaging.{CloudPublisher, CloudSubscriber, ReceivedMessage}
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * This is subscriber for messages that are not published by Leonardo itself.
 * But rather from cron jobs, or cloud logging sink (triggered from crypto-mining activity)
 */
class NonLeoMessageSubscriber[F[_]](config: NonLeoMessageSubscriberConfig,
                                    gkeAlg: GKEAlgebra[F],
                                    computeService: GoogleComputeService[F],
                                    samDao: SamDAO[F],
                                    authProvider: LeoAuthProvider[F],
                                    subscriber: CloudSubscriber[F, NonLeoMessage],
                                    publisher: CloudPublisher[F],
                                    asyncTasks: Queue[F, Task[F]],
                                    samService: SamService[F]
)(implicit
  logger: StructuredLogger[F],
  F: Async[F],
  metrics: OpenTelemetryMetrics[F],
  dbRef: DbReference[F]
) {
  val process: Stream[F, Unit] = subscriber.messages
    .parEvalMapUnordered(10)(messageHandler)
    .handleErrorWith(error => Stream.eval(logger.error(error)("Failed to initialize message processor")))

  private[monitor] def messageHandler(event: ReceivedMessage[NonLeoMessage]): F[Unit] =
    for {
      now <- F.realTimeInstant
      uuid <- F.delay(java.util.UUID.randomUUID())
      traceId = event.traceId.getOrElse(TraceId(uuid.toString))
      ctx = AppContext(traceId, now, "", None)
      implicit0(ev: Ask[F, AppContext]) <- F.pure(Ask.const[F, AppContext](ctx))
      _ <- metrics.incrementCounter(s"NonLeoPubSub/${event.msg.messageType}")
      res <- messageResponder(event.msg).attempt
      _ <- res match {
        case Left(e) =>
          logger.error(ctx.loggingCtx, e)("Fail to process pubsub message") >> F.delay(event.ackHandler.nack())
        case Right(_) => F.delay(event.ackHandler.ack())
      }
    } yield ()

  private[monitor] def messageResponder(
    message: NonLeoMessage
  )(implicit traceId: Ask[F, AppContext]): F[Unit] = message match {
    case m: NonLeoMessage.DeleteKubernetesClusterMessage => handleDeleteKubernetesClusterMessage(m)
    case m: NonLeoMessage.CryptoMining                   => handleCryptoMiningMessage(m)
    case m: NonLeoMessage.DeleteNodepoolMessage          => handleDeleteNodepoolMessage(m)
    case m: NonLeoMessage.CryptoMiningScc                => handleCryptoMiningMessageScc(m)
  }

  private[monitor] def handleDeleteKubernetesClusterMessage(msg: DeleteKubernetesClusterMessage)(implicit
    ev: Ask[F, AppContext]
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
        .onError { case e =>
          for {
            _ <- logger.error(ctx.loggingCtx, e)(
              s"An error occurred during clean-up of cluster ${clusterId} in project ${msg.project}."
            )
            _ <- kubernetesClusterQuery.updateStatus(clusterId, KubernetesClusterStatus.Error).transaction
            // TODO: Create a KUBERNETES_CLUSTER_ERROR table to log the error message?
            // TODO: Need mark the nodepool(s) as Error'ed too?
          } yield ()
        }
    } yield ()

  private[monitor] def handleCryptoMiningMessageScc(
    msg: NonLeoMessage.CryptoMiningScc
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    deleteCryptominingRuntime(msg.resource.googleProject, msg.resource.runtimeName, msg.resource.zone, "scc")

  private[monitor] def handleCryptoMiningMessage(
    msg: NonLeoMessage.CryptoMining
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      _ <-
        if (
          !msg.textPayload.contains(
            "CRYPTOMINING_DETECTED"
          )
        ) // needs to match https://github.com/broadinstitute/terra-cryptomining-security-alerts/blob/master/v2/main.go#L24
          F.unit
        else
          for {
            runtimeFromGoogle <- computeService.getInstance(msg.googleProject,
                                                            msg.resource.labels.zone,
                                                            InstanceName(msg.resource.labels.instanceId.toString)
            )
            // We mark the runtime as Deleted, and delete the instance.
            // If instance deletion fails for some reason, it will be cleaned up by resource-validator
            _ <- runtimeFromGoogle.traverse { instance =>
              deleteCryptominingRuntime(msg.googleProject,
                                        RuntimeName(instance.getName),
                                        msg.resource.labels.zone,
                                        "custom detector"
              )
            }
          } yield ()
    } yield ()

  private[monitor] def deleteCryptominingRuntime(
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    zone: ZoneName,
    message: String
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      cloudContext = CloudContext.Gcp(googleProject)
      runtimeInfo <- clusterQuery
        .getActiveClusterRecordByName(cloudContext, runtimeName)
        .transaction
      _ <- runtimeInfo match {
        case None =>
          logger.info(ctx.loggingCtx)(
            s"Detected ${message} activity for ${googleProject.value}/${runtimeName.asString};" +
              s"This might be because the runtime has already been deleted, or the runtime wasn't created by Leonardo"
          )
        case Some(runtime) =>
          for {
            _ <- clusterQuery
              .markDeleted(cloudContext, runtimeName, ctx.now, Some(s"cryptomining: ${message}"))
              .transaction
            diskIdOpt <- RuntimeConfigQueries.getDiskId(runtime.runtimeConfigId).transaction
            _ <- diskIdOpt match {
              case Some(diskId) =>
                for {
                  _ <- persistentDiskQuery
                    .delete(diskId, ctx.now)
                    .transaction
                  _ <- computeService.deleteInstanceWithAutoDeleteDisk(googleProject,
                                                                       zone,
                                                                       InstanceName(runtimeName.asString),
                                                                       Set(config.userDiskDeviceName)
                  )
                } yield ()
              case None => computeService.deleteInstance(googleProject, zone, InstanceName(runtimeName.asString))
            }

            userSubjectId <- samDao.getUserSubjectId(runtime.auditInfo.creator, googleProject)
            _ <- userSubjectId.traverse { sid =>
              implicit val traceId: Ask[F, TraceId] = Ask.const[F, TraceId](ctx.traceId)
              publisher.publishOne(CryptominingUserMessage(sid), Map("cryptomining" -> "true"))
            }
          } yield ()
      }
    } yield ()

  private[monitor] def handleDeleteNodepoolMessage(
    msg: DeleteNodepoolMessage
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      task = for {
        _ <- gkeAlg.deleteAndPollNodepool(DeleteNodepoolParams(msg.nodepoolId, msg.googleProject))
        apps <- appQuery.getNonDeletedAppsByNodepool(msg.nodepoolId).transaction
        // Delete kubernetes-app Sam resources
        _ <- apps.traverse { app =>
          samService
            .getPetServiceAccountToken(app.creator, msg.googleProject)
            .flatMap(petToken => samService.deleteResource(petToken, app.samResourceId))
        }
      } yield ()

      _ <- asyncTasks.offer(
        Task(
          ctx.traceId,
          task,
          Some(logError(s"${msg.nodepoolId}/${msg.googleProject}", DeleteNodepool.toString)),
          ctx.now,
          TaskMetricsTags("deleteNodepool", None, None, CloudProvider.Gcp)
        )
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

  implicit val sccCategoryDecoder: Decoder[SccCategory] = Decoder.decodeString.emap(s => SccCategory.fromString(s))
  implicit val findingDecoder: Decoder[Finding] = Decoder.forProduct1("category")(Finding.apply)
  val zonePatternInName = "zones\\/([a-z]*\\-[a-z|1-9]*\\-[a-z])\\/".r
  implicit val cryptoMiningSccResourceDecoder: Decoder[CryptoMiningSccResource] = Decoder.instance { c =>
    for {
      googleProject <- c.downField("projectDisplayName").as[GoogleProject]
      serviceType <- c.downField("type").as[String]
      cloudService <- serviceType match {
        case "google.compute.Instance" => CloudService.GCE.asRight[DecodingFailure]
        case s => DecodingFailure(s"unsupported cryptomining-scc type ${s}", List.empty).asLeft[CloudService]
      }
      runtimeName <- c.downField("displayName").as[RuntimeName]
      // name field looks like `//compute.googleapis.com/projects/terra-2d61a51b/zones/us-central1-a/instances/5289438569693667937`
      name <- c.downField("name").as[String]
      zone <- Either
        .catchNonFatal(zonePatternInName.findFirstMatchIn(name).map(_.group(1)))
        .leftMap(t =>
          DecodingFailure(s"Can't find zone name in cryptomining message from SCC in ${zonePatternInName} due to ${t}",
                          List.empty
          )
        )
        .flatMap(s =>
          s.fold(
            DecodingFailure(s"Can't find zone name in cryptomining message from SCC in ${zonePatternInName}",
                            List.empty
            ).asLeft[ZoneName]
          )(ss => ZoneName(ss).asRight[DecodingFailure])
        )
    } yield CryptoMiningSccResource(googleProject, cloudService, runtimeName, zone)
  }
  implicit val cryptoMiningSccDecoder: Decoder[CryptoMiningScc] = Decoder.instance { c =>
    for {
      resource <- c.downField("resource").as[CryptoMiningSccResource]
      finding <- c.downField("finding").as[Finding]
    } yield CryptoMiningScc(resource, finding)
  }
  implicit val deleteNodepoolDecoder: Decoder[DeleteNodepoolMessage] =
    Decoder.forProduct3("nodepoolId", "googleProject", "traceId")(DeleteNodepoolMessage.apply)

  implicit val nonLeoMessageDecoder: Decoder[NonLeoMessage] = Decoder.instance { x =>
    deleteKubernetesClusterDecoder.tryDecode(x) orElse (cryptoMiningDecoder.tryDecode(x)) orElse (deleteNodepoolDecoder
      .tryDecode(x)) orElse (cryptoMiningSccDecoder.tryDecode(x))
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
  // Cryptoming messages generated from our custom cryptomining detector
  final case class CryptoMining(textPayload: String, resource: GoogleResource, googleProject: GoogleProject)
      extends NonLeoMessage {
    val messageType: String = "crypto-minining"
  }
  // Cryptoming messages generated from Google's Security Command Center service
  final case class CryptoMiningScc(resource: CryptoMiningSccResource, finding: Finding) extends NonLeoMessage {
    val messageType: String = "crypto-minining-scc"
  }
  final case class DeleteNodepoolMessage(nodepoolId: NodepoolLeoId,
                                         googleProject: GoogleProject,
                                         traceId: Option[TraceId]
  ) extends NonLeoMessage {
    val messageType: String = "deleteNodepool"
  }
}
final case class GoogleResource(labels: GoogleLabels)
final case class GoogleLabels(instanceId: Long, zone: ZoneName)

final case class CryptominingUserMessage(userSubjectId: UserSubjectId)
final case class Finding(category: SccCategory)
final case class SccCategory(asString: String) extends AnyVal
object SccCategory {
  def fromString(s: String): Either[String, SccCategory] = s match {
    case "Execution: Cryptocurrency Mining Combined Detection" => SccCategory(s).asRight[String]
    case "Execution: Cryptocurrency Mining YARA Rule"          => SccCategory(s).asRight[String]
    case "Execution: Cryptocurrency Mining Hash Match"         => SccCategory(s).asRight[String]
    case s                                                     => s"Unsupported SCC category ${s}".asLeft[SccCategory]
  }
}

final case class CryptoMiningSccResource(
  googleProject: GoogleProject,
  cloudService: CloudService,
  runtimeName: RuntimeName,
  zone: ZoneName
)

final case class NonLeoMessageSubscriberConfig(userDiskDeviceName: DeviceName)
