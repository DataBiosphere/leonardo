package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.Status.Failure
import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.StatusCodes
import akka.pattern.pipe
import cats.data.OptionT
import cats.effect.{Async, ContextShift, IO, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.cloud.storage.BucketInfo
import com.typesafe.scalalogging.LazyLogging
import io.grpc.Status.Code
import org.broadinstitute.dsde.workbench.google2.{
  GcsBlobName,
  GetMetadataResponse,
  GoogleComputeService,
  GoogleStorageService,
  InstanceName
}
import org.broadinstitute.dsde.workbench.leonardo.RuntimeStatus.{
  Creating,
  Deleted,
  Deleting,
  Error,
  Running,
  Starting,
  Stopping,
  Unknown,
  Updating
}
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.dao.ToolDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleDataprocDAO, _}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorActor.ClusterMonitorMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorActor._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.ClusterSupervisorMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.ClusterSupervisorMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.RuntimeTransitionMessage
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.util.{addJitter, Retry}
import slick.dbio.DBIOAction

import scala.collection.immutable.Set
import scala.concurrent.duration._

case class ProxyDAONotFound(clusterName: RuntimeName, googleProject: GoogleProject, clusterTool: RuntimeImageType)
    extends LeoException(s"Cluster ${clusterName}/${googleProject} was initialized with invalid tool: ${clusterTool}",
                         StatusCodes.InternalServerError)

object ClusterMonitorActor {
  def getRuntimeUI(runtime: Runtime): RuntimeUI =
    if (runtime.labels.contains(Config.uiConfig.terraLabel)) RuntimeUI.Terra
    else if (runtime.labels.contains(Config.uiConfig.allOfUsLabel)) RuntimeUI.AoU
    else RuntimeUI.Other

  private[monitor] def recordStatusTransitionMetrics[F[_]: Timer: Async](
    startTime: Instant,
    runtimeUI: RuntimeUI,
    origStatus: RuntimeStatus,
    finalStatus: RuntimeStatus,
    cloudService: CloudService
  )(implicit openTelemetry: OpenTelemetryMetrics[F]): F[Unit] =
    for {
      endTime <- Timer[F].clock.realTime(TimeUnit.MILLISECONDS)
      metricsName = s"monitor/transition/${origStatus}_to_${finalStatus}"
      duration = (endTime - startTime.toEpochMilli).millis
      tags = Map("cloudService" -> cloudService.asString, "ui_client" -> runtimeUI.asString)
      _ <- openTelemetry.incrementCounter(metricsName, 1, tags)
      distributionBucket = List(0.5 minutes,
                                1 minutes,
                                1.5 minutes,
                                2 minutes,
                                2.5 minutes,
                                3 minutes,
                                3.5 minutes,
                                4 minutes,
                                4.5 minutes) //Distribution buckets from 0.5 min to 4.5 min
      _ <- openTelemetry.recordDuration(metricsName, duration, distributionBucket, tags)
    } yield ()

  private def findToolImageInfo(images: Set[RuntimeImage], imageConfig: ImageConfig): String = {
    val terraJupyterImage = imageConfig.jupyterImageRegex.r
    val anvilRStudioImage = imageConfig.rstudioImageRegex.r
    val broadDockerhubImageRegex = imageConfig.broadDockerhubImageRegex.r
    images.find(runtimeImage => Set(RuntimeImageType.Jupyter, RuntimeImageType.RStudio) contains runtimeImage.imageType) match {
      case Some(toolImage) =>
        toolImage.imageUrl match {
          case terraJupyterImage(imageType, hash)        => s"GCR/${imageType}/${hash}"
          case anvilRStudioImage(imageType, hash)        => s"GCR/${imageType}/${hash}"
          case broadDockerhubImageRegex(imageType, hash) => s"DockerHub/${imageType}/${hash}"
          case _                                         => "custom_image"
        }
      case None => "unknown"
    }
  }

  private[monitor] def recordClusterCreationMetrics[F[_]: Timer: Async](
    createdDate: Instant,
    images: Set[RuntimeImage],
    imageConfig: ImageConfig,
    cloudService: CloudService
  )(implicit openTelemetry: OpenTelemetryMetrics[F]): F[Unit] =
    for {
      endTime <- Timer[F].clock.realTime(TimeUnit.MILLISECONDS)
      toolImageInfo = findToolImageInfo(images, imageConfig)
      metricsName = s"monitor/runtimeCreation"
      duration = (endTime - createdDate.toEpochMilli).milliseconds
      tags = Map("cloudService" -> cloudService.asString, "image" -> toolImageInfo)
      _ <- openTelemetry.incrementCounter(metricsName, 1, tags)
      distributionBucket = List(1 minutes,
                                1.5 minutes,
                                2 minutes,
                                2.5 minutes,
                                3 minutes,
                                3.5 minutes,
                                4 minutes,
                                4.5 minutes,
                                5 minutes,
                                5.5 minutes,
                                6 minutes) //Distribution buckets from 1 min to 6 min
      _ <- openTelemetry.recordDuration(metricsName, duration, distributionBucket, tags)
    } yield ()

  /**
   * Creates a Props object used for creating a {{{ClusterMonitorActor}}}.
   */
  def props(
    clusterId: Long,
    monitorConfig: MonitorConfig,
    dataprocConfig: DataprocConfig,
    gceConfig: GceConfig,
    imageConfig: ImageConfig,
    clusterBucketConfig: RuntimeBucketConfig,
    gdDAO: GoogleDataprocDAO,
    googleComputeService: GoogleComputeService[IO],
    google2StorageDAO: GoogleStorageService[IO],
    dbRef: DbReference[IO],
    authProvider: LeoAuthProvider[IO],
    publisherQueue: fs2.concurrent.InspectableQueue[IO, LeoPubsubMessage]
  )(implicit openTelemetryMetrics: OpenTelemetryMetrics[IO],
    runtimeToolToToolDao: RuntimeContainerServiceType => ToolDAO[IO, RuntimeContainerServiceType],
    cs: ContextShift[IO],
    timer: Timer[IO],
    runtimeInstances: RuntimeInstances[IO]): Props =
    Props(
      new ClusterMonitorActor(clusterId,
                              monitorConfig,
                              dataprocConfig,
                              gceConfig,
                              imageConfig,
                              clusterBucketConfig,
                              gdDAO,
                              googleComputeService,
                              google2StorageDAO,
                              dbRef,
                              authProvider,
                              publisherQueue)
    )

  // ClusterMonitorActor messages:
  sealed trait ClusterMonitorMessage extends Product with Serializable
  object ClusterMonitorMessage {
    final case object ScheduleMonitorPass extends ClusterMonitorMessage
    final case object QueryForCluster extends ClusterMonitorMessage
    final case class ReadyCluster(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                                  publicIP: IP,
                                  dataprocInstances: Set[DataprocInstance])
        extends ClusterMonitorMessage
    final case class NotReadyCluster(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                                     googleStatus: RuntimeStatus,
                                     dataprocInstances: Set[DataprocInstance],
                                     msg: Option[String] = None)
        extends ClusterMonitorMessage
    final case class FailedCluster(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                                   errorDetails: RuntimeErrorDetails,
                                   dataprocInstances: Set[DataprocInstance])
        extends ClusterMonitorMessage
    final case class DeletedCluster(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig) extends ClusterMonitorMessage
    final case class StoppedCluster(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                                    dataprocInstances: Set[DataprocInstance])
        extends ClusterMonitorMessage
    final case class ShutdownActor(notifyParentMsg: ClusterSupervisorMessage) extends ClusterMonitorMessage
  }
}

/**
 * An actor which monitors the status of a Cluster. Periodically queries Google for the cluster status,
 * and acts appropriately for Running, Deleted, and Failed clusters.
 * @param clusterId the cluster ID to monitor
 * @param monitorConfig monitor configuration properties
 * @param gdDAO the Google dataproc DAO
 * @param dbRef the DB reference
 *              changing something for PR
 */
class ClusterMonitorActor(
  val clusterId: Long,
  val monitorConfig: MonitorConfig,
  val dataprocConfig: DataprocConfig,
  val gceConfig: GceConfig,
  val imageConfig: ImageConfig,
  val clusterBucketConfig: RuntimeBucketConfig,
  val gdDAO: GoogleDataprocDAO,
  val googleComputeService: GoogleComputeService[IO],
  val google2StorageDAO: GoogleStorageService[IO],
  val dbRef: DbReference[IO],
  val authProvider: LeoAuthProvider[IO],
  val publisherQueue: fs2.concurrent.InspectableQueue[IO, LeoPubsubMessage],
  val startTime: Long = System.currentTimeMillis()
)(implicit openTelemetryMetrics: OpenTelemetryMetrics[IO],
  runtimeToolToToolDao: RuntimeContainerServiceType => ToolDAO[IO, RuntimeContainerServiceType],
  cs: ContextShift[IO],
  timer: Timer[IO],
  runtimeInstances: RuntimeInstances[IO])
    extends Actor
    with LazyLogging
    with Retry {
  import context._

  // the Retry trait needs a reference to the ActorSystem
  override val system = context.system

  private val internalError = "Internal Error"

  override def preStart(): Unit = {
    super.preStart()
    logger.info(s"Using monitor status timeouts: ${monitorConfig.monitorStatusTimeouts}")
    scheduleInitialMonitorPass
  }

  override def receive: Receive = {
    case ScheduleMonitorPass =>
      scheduleNextMonitorPass

    case QueryForCluster =>
      implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))
      checkCluster.unsafeToFuture() pipeTo self

    case NotReadyCluster(runtimeAndRuntimeConfig, googleStatus, dataprocInstances, msg) =>
      implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))
      handleNotReadyCluster(runtimeAndRuntimeConfig, googleStatus, dataprocInstances, msg).unsafeToFuture() pipeTo self

    case ReadyCluster(runtimeAndRuntimeConfig, ip, dataprocInstances) =>
      implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))
      handleReadyCluster(runtimeAndRuntimeConfig, ip, dataprocInstances).unsafeToFuture() pipeTo self

    case FailedCluster(runtimeAndRuntimeConfig, errorDetails, dataprocInstances) =>
      implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))
      handleFailedCluster(runtimeAndRuntimeConfig, errorDetails, dataprocInstances).unsafeToFuture() pipeTo self

    case DeletedCluster(runtimeAndRuntimeConfig) =>
      implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))
      val res = handleDeletedCluster(runtimeAndRuntimeConfig).onError {
        case e => IO(logger.warn("Error deleting cluster", e))
      }
      res.unsafeToFuture() pipeTo self

    case StoppedCluster(runtimeAndRuntimeConfig, dataprocInstances) =>
      implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))
      handleStoppedCluster(runtimeAndRuntimeConfig, dataprocInstances).unsafeToFuture() pipeTo self

    case ShutdownActor(notifyParentMsg) =>
      parent ! notifyParentMsg
      stop(self)

    case Failure(e) =>
      // An error occurred, let the supervisor handle it
      logger.error(s"Error occurred monitoring cluster with id $clusterId", e)
      throw e
  }

  private def scheduleInitialMonitorPass(): Unit =
    // Wait anything _up to_ the poll interval for a much wider distribution of cluster monitor start times when Leo starts up
    system.scheduler.scheduleOnce(addJitter(0 seconds, monitorConfig.pollPeriod), self, QueryForCluster)

  private def scheduleNextMonitorPass(): Unit =
    system.scheduler.scheduleOnce(addJitter(monitorConfig.pollPeriod), self, QueryForCluster)

  /**
   * Handles a dataproc cluster which is not ready yet. We don't take any action, just
   * schedule another monitor pass.
   * @param runtimeAndRuntimeConfig the cluster being monitored
   * @param googleStatus the ClusterStatus from Google
   * @param dataprocInstances the cluster's instances in Google
   * @return ScheduleMonitorPass
   */
  private def handleNotReadyCluster(
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
    googleStatus: RuntimeStatus,
    dataprocInstances: Set[DataprocInstance],
    msg: Option[String]
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[ClusterMonitorMessage] = {
    val currTimeElapsed: FiniteDuration = (System.currentTimeMillis() - startTime).millis
    monitorConfig.monitorStatusTimeouts.get(runtimeAndRuntimeConfig.runtime.status) match {
      case Some(timeLimit) if currTimeElapsed > timeLimit =>
        logger.info(
          s"Detected that ${runtimeAndRuntimeConfig.runtime.projectNameString} has been stuck in status ${runtimeAndRuntimeConfig.runtime.status} too long."
        )

        // Take care not to Error out a cluster if it timed out in Starting status
        if (runtimeAndRuntimeConfig.runtime.status == Starting) {
          for {
            _ <- persistInstances(runtimeAndRuntimeConfig, dataprocInstances)
            now <- IO(Instant.now)
            _ <- runtimeAndRuntimeConfig.runtimeConfig.cloudService.interpreter
              .stopRuntime(StopRuntimeParams(runtimeAndRuntimeConfig, now))
            _ <- dbRef.inTransaction(clusterQuery.setToStopping(runtimeAndRuntimeConfig.runtime.id, now))
          } yield ScheduleMonitorPass
        } else {
          handleFailedCluster(
            runtimeAndRuntimeConfig,
            RuntimeErrorDetails(
              Code.DEADLINE_EXCEEDED.value,
              Some(
                s"Failed to transition ${runtimeAndRuntimeConfig.runtime.projectNameString} from status ${runtimeAndRuntimeConfig.runtime.status} within the time limit: ${timeLimit.toSeconds} seconds"
              )
            ),
            dataprocInstances
          )
        }

      case _ =>
        IO(
          logger.info(
            s"Dataproc cluster ${runtimeAndRuntimeConfig.runtime.projectNameString} is not ready yet and has taken ${currTimeElapsed.toSeconds} seconds so far (Dataproc cluster status = $googleStatus, GCE instance statuses = ${dataprocInstances
              .groupBy(_.status)
              .mapValues(_.size)}). Checking again in ${monitorConfig.pollPeriod.toString}. ${msg.getOrElse("")}"
          )
        ) >> persistInstances(runtimeAndRuntimeConfig, dataprocInstances).as(ScheduleMonitorPass)
    }
  }

  /**
   * Handles a dataproc cluster which is ready. We update the status and IP in the database,
   * then shut down this actor.
   * @param publicIp the cluster public IP, according to Google
   * @return ShutdownActor
   */
  private def handleReadyCluster(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                                 publicIp: IP,
                                 dataprocInstances: Set[DataprocInstance])(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[ClusterMonitorMessage] =
    for {
      // create or update instances in the DB
      _ <- persistInstances(runtimeAndRuntimeConfig, dataprocInstances)
      // update DB after auth futures finish
      now <- IO(Instant.now)
      _ <- dbRef.inTransaction(clusterQuery.setToRunning(runtimeAndRuntimeConfig.runtime.id, publicIp, now))
      // Record metrics in NewRelic
      _ <- recordStatusTransitionMetrics(
        Instant.ofEpochMilli(startTime),
        getRuntimeUI(runtimeAndRuntimeConfig.runtime),
        runtimeAndRuntimeConfig.runtime.status,
        RuntimeStatus.Running,
        runtimeAndRuntimeConfig.runtimeConfig.cloudService
      )
      _ <- if (runtimeAndRuntimeConfig.runtime.status == RuntimeStatus.Creating)
        recordClusterCreationMetrics(
          runtimeAndRuntimeConfig.runtime.auditInfo.createdDate,
          runtimeAndRuntimeConfig.runtime.runtimeImages,
          imageConfig,
          runtimeAndRuntimeConfig.runtimeConfig.cloudService
        )
      else IO.unit
      traceId <- ev.ask
      _ <- publisherQueue.enqueue1(
        RuntimeTransitionMessage(RuntimePatchDetails(clusterId, runtimeAndRuntimeConfig.runtime.status), Some(traceId))
      )
      // Finally pipe a shutdown message to this actor
      _ <- IO(logger.info(s"Cluster ${runtimeAndRuntimeConfig.runtime.projectNameString} is ready for use!"))
    } yield ShutdownActor(RemoveFromList(runtimeAndRuntimeConfig.runtime))

  /**
   * Handles a dataproc cluster which has failed. We delete the cluster in Google, and then:
   * - if this is a recoverable error, recreate the cluster
   * - otherwise, just set the status to Error and stop monitoring the cluster
   * @param errorDetails cluster error details from Google
   * @return ShutdownActor
   */
  private def handleFailedCluster(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                                  errorDetails: RuntimeErrorDetails,
                                  dataprocInstances: Set[DataprocInstance])(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[ClusterMonitorMessage] =
    for {
      _ <- List(
        // Delete the cluster in Google
        runtimeAndRuntimeConfig.runtimeConfig.cloudService.interpreter
          .deleteRuntime(DeleteRuntimeParams(runtimeAndRuntimeConfig.runtime)),
        // create or update instances in the DB
        persistInstances(runtimeAndRuntimeConfig, dataprocInstances),
        //save cluster error in the DB
        persistClusterErrors(errorDetails, runtimeAndRuntimeConfig.runtime)
      ).parSequence_

      // Record metrics in NewRelic
      _ <- recordStatusTransitionMetrics(
        Instant.ofEpochMilli(startTime),
        getRuntimeUI(runtimeAndRuntimeConfig.runtime),
        runtimeAndRuntimeConfig.runtime.status,
        RuntimeStatus.Error,
        runtimeAndRuntimeConfig.runtimeConfig.cloudService
      )

      now <- IO(Instant.now)

      // Decide if we should try recreating the cluster
      res <- if (shouldRecreateCluster(runtimeAndRuntimeConfig, errorDetails.code, errorDetails.message)) {
        // Update the database record to Deleting, shutdown this actor, and register a callback message
        // to the supervisor telling it to recreate the cluster.
        IO(
          logger.info(
            s"Cluster ${runtimeAndRuntimeConfig.runtime.projectNameString} is in an error state with $errorDetails. Attempting to recreate..."
          )
        ) >>
          dbRef
            .inTransaction {
              clusterQuery.markPendingDeletion(runtimeAndRuntimeConfig.runtime.id, now)
            }
            .as(ShutdownActor(ClusterDeleted(runtimeAndRuntimeConfig.runtime, recreate = true)))
      } else {
        for {
          // Update the database record to Error and shutdown this actor.
          _ <- IO(
            logger.warn(
              s"Cluster ${runtimeAndRuntimeConfig.runtime.projectNameString} is in an error state with $errorDetails'. Unable to recreate cluster."
            )
          )
          // update the cluster status to Error
          _ <- dbRef.inTransaction {
            clusterQuery.updateClusterStatus(runtimeAndRuntimeConfig.runtime.id, RuntimeStatus.Error, now)
          }
          tags = Map(
            "cloudService" -> runtimeAndRuntimeConfig.runtimeConfig.cloudService.asString,
            "dataprocErrorCode" -> errorDetails.code.toString
          )
          _ <- openTelemetryMetrics.incrementCounter(s"runtimeCreationFailure", 1, tags)

          // Remove the Dataproc Worker IAM role for the pet service account
          // Only happens if the cluster was created with the pet service account.
        } yield ShutdownActor(RemoveFromList(runtimeAndRuntimeConfig.runtime))
      }
    } yield res

  private def shouldRecreateCluster(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                                    code: Int,
                                    message: Option[String]): Boolean = {
    // TODO: potentially add more checks here as we learn which errors are recoverable
    logger.info(s"determining if we should re-create cluster ${runtimeAndRuntimeConfig.runtime.projectNameString}")
    monitorConfig.recreateCluster && (code == Code.UNKNOWN.value)
  }

  /**
   * Handles a dataproc cluster which has been deleted.
   * We update the status to Deleted in the database, notify the auth provider,
   * and shut down this actor.
   * @return error or ShutdownActor
   */
  private def handleDeletedCluster(
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[ClusterMonitorMessage] = {
    logger.info(s"Runtime ${runtimeAndRuntimeConfig.runtime.projectNameString} has been deleted.")

    for {
      // delete the init bucket so we don't continue to accrue costs after cluster is deleted
      _ <- deleteInitBucket(runtimeAndRuntimeConfig.runtime)

      // set the staging bucket to be deleted in ten days so that logs are still accessible until then
      _ <- setStagingBucketLifecycle(runtimeAndRuntimeConfig.runtime)

      // delete instances in the DB
      _ <- persistInstances(runtimeAndRuntimeConfig, Set.empty)

      now <- IO(Instant.now)

      _ <- dbRef.inTransaction {
        clusterQuery.completeDeletion(runtimeAndRuntimeConfig.runtime.id, now)
      }
      _ <- authProvider
        .notifyClusterDeleted(
          runtimeAndRuntimeConfig.runtime.internalId,
          runtimeAndRuntimeConfig.runtime.auditInfo.creator,
          runtimeAndRuntimeConfig.runtime.auditInfo.creator,
          runtimeAndRuntimeConfig.runtime.googleProject,
          runtimeAndRuntimeConfig.runtime.runtimeName
        )

      _ <- runtimeAndRuntimeConfig.runtimeConfig.cloudService.interpreter
        .finalizeDelete(FinalizeDeleteParams(runtimeAndRuntimeConfig.runtime))

      // Record metrics in NewRelic
      _ <- recordStatusTransitionMetrics(
        Instant.ofEpochMilli(startTime),
        getRuntimeUI(runtimeAndRuntimeConfig.runtime),
        runtimeAndRuntimeConfig.runtime.status,
        RuntimeStatus.Deleted,
        runtimeAndRuntimeConfig.runtimeConfig.cloudService
      )

    } yield ShutdownActor(RemoveFromList(runtimeAndRuntimeConfig.runtime))
  }

  /**
   * Handles a dataproc cluster which has been stopped.
   * We update the status to Stopped in the database and shut down this actor.
   * @return ShutdownActor
   */
  private def handleStoppedCluster(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                                   dataprocInstances: Set[DataprocInstance])(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[ClusterMonitorMessage] = {
    logger.info(s"Cluster ${runtimeAndRuntimeConfig.runtime.projectNameString} has been stopped.")
    for {
      // create or update instances in the DB
      _ <- persistInstances(runtimeAndRuntimeConfig, dataprocInstances)
      now <- IO(Instant.now)
      // this sets the cluster status to stopped and clears the cluster IP
      _ <- dbRef.inTransaction {
        clusterQuery.updateClusterStatus(runtimeAndRuntimeConfig.runtime.id, RuntimeStatus.Stopped, now)
      }
      // reset the time at which the kernel was last found to be busy
      _ <- dbRef.inTransaction(clusterQuery.clearKernelFoundBusyDate(runtimeAndRuntimeConfig.runtime.id, now))
      traceId <- ev.ask
      _ <- publisherQueue.enqueue1(
        RuntimeTransitionMessage(RuntimePatchDetails(clusterId, RuntimeStatus.Stopped), Some(traceId))
      )
      _ <- recordStatusTransitionMetrics(
        Instant.ofEpochMilli(startTime),
        getRuntimeUI(runtimeAndRuntimeConfig.runtime),
        runtimeAndRuntimeConfig.runtime.status,
        RuntimeStatus.Stopped,
        runtimeAndRuntimeConfig.runtimeConfig.cloudService
      )
    } yield ShutdownActor(RemoveFromList(runtimeAndRuntimeConfig.runtime))
  }

  private def checkCluster(implicit ev: ApplicativeAsk[IO, TraceId]): IO[ClusterMonitorMessage] =
    for {
      runtimeAndRuntimeConfig <- getDbRuntimeAndRuntimeConfig
      next <- runtimeAndRuntimeConfig.runtime.status match {
        case status if status.isMonitored =>
          checkRuntimeInGoogle(runtimeAndRuntimeConfig)
        case status =>
          IO(
            logger.info(
              s"Stopping monitoring of cluster ${runtimeAndRuntimeConfig.runtime.projectNameString} in status ${status}"
            )
          ) >>
            IO.pure(ShutdownActor(RemoveFromList(runtimeAndRuntimeConfig.runtime)))
      }
    } yield next

  /**
   * Queries Google for the cluster status and takes appropriate action depending on the result.
   * @return ClusterMonitorMessage
   */
  private def checkRuntimeInGoogle(
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[ClusterMonitorMessage] =
    runtimeAndRuntimeConfig.runtimeConfig.cloudService match {
      case CloudService.GCE =>
        IO.raiseError(new Exception("ClusterMonitor shouldn't care about GCE instances"))
      case CloudService.Dataproc =>
        for {
          runtimeStatus <- runtimeInstances
            .interpreter(CloudService.Dataproc)
            .getRuntimeStatus(
              GetRuntimeStatusParams(
                runtimeAndRuntimeConfig.runtime.googleProject,
                runtimeAndRuntimeConfig.runtime.runtimeName,
                None
              )
            )
          r <- continueCheckRuntimeInGoogle(runtimeAndRuntimeConfig, runtimeStatus)
        } yield r
    }

  private def continueCheckRuntimeInGoogle(
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
    runtimeStatus: RuntimeStatus
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[ClusterMonitorMessage] =
    for {
      dataprocInstances <- runtimeAndRuntimeConfig.runtimeConfig.cloudService match {
        case CloudService.GCE      => IO.pure(Set.empty[DataprocInstance])
        case CloudService.Dataproc => getDataprocInstances(runtimeAndRuntimeConfig.runtime)
      }

      runningInstanceCount = dataprocInstances.count(_.status == GceInstanceStatus.Running)
      stoppedInstanceCount = dataprocInstances.count(i =>
        i.status == GceInstanceStatus.Stopped || i.status == GceInstanceStatus.Terminated
      )

      result <- runtimeStatus match {
        case Unknown | Creating | Updating | Stopping | Starting =>
          IO.pure(NotReadyCluster(runtimeAndRuntimeConfig, runtimeStatus, dataprocInstances))
        // Take care we don't restart a Deleting or Stopping cluster if google hasn't updated their status yet
        case Running
            if runtimeAndRuntimeConfig.runtime.status != Deleting && runtimeAndRuntimeConfig.runtime.status != Stopping && runningInstanceCount == dataprocInstances.size =>
          getMasterIp(runtimeAndRuntimeConfig).flatMap {
            case Some(ip) => checkClusterTools(runtimeAndRuntimeConfig, ip, dataprocInstances)
            case None =>
              IO.pure(
                NotReadyCluster(runtimeAndRuntimeConfig,
                                RuntimeStatus.Running,
                                dataprocInstances,
                                Some("Could not retrieve master IP"))
              )
          }
        // Take care we don't fail a Deleting or Stopping cluster if google hasn't updated their status yet
        case Error
            if runtimeAndRuntimeConfig.runtime.status != Deleting && runtimeAndRuntimeConfig.runtime.status != Stopping =>
          runtimeAndRuntimeConfig.runtimeConfig.cloudService match {
            // TODO implement getErrorDetails in wb-libs
            case CloudService.GCE =>
              IO.pure(FailedCluster(runtimeAndRuntimeConfig, RuntimeErrorDetails(-1, None), dataprocInstances))
            case CloudService.Dataproc =>
              IO.fromFuture(
                  IO(
                    gdDAO
                      .getClusterErrorDetails(runtimeAndRuntimeConfig.runtime.asyncRuntimeFields.map(_.operationName))
                  )
                )
                .map {
                  case Some(errorDetails) => FailedCluster(runtimeAndRuntimeConfig, errorDetails, dataprocInstances)
                  case None =>
                    FailedCluster(runtimeAndRuntimeConfig,
                                  RuntimeErrorDetails(Code.INTERNAL.value, Some(internalError)),
                                  dataprocInstances)
                }
          }
        case Deleted =>
          IO.pure(DeletedCluster(runtimeAndRuntimeConfig))
        // if the cluster only contains stopped instances, it's a stopped cluster
        case _
            if runtimeAndRuntimeConfig.runtime.status != Starting && runtimeAndRuntimeConfig.runtime.status != Deleting && stoppedInstanceCount == dataprocInstances.size =>
          IO.pure(StoppedCluster(runtimeAndRuntimeConfig, dataprocInstances))
        case _ => IO.pure(NotReadyCluster(runtimeAndRuntimeConfig, runtimeStatus, dataprocInstances))
      }
    } yield result

  private def checkClusterTools(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                                ip: IP,
                                dataprocInstances: Set[DataprocInstance]): IO[ClusterMonitorMessage] =
    // Update the Host IP in the database so DNS cache can be properly populated with the first cache miss
    // Otherwise, when a cluster is resumed and transitions from Starting to Running, we get stuck
    // in that state - at least with the way HttpJupyterDAO.isProxyAvailable works
    for {
      now <- IO(Instant.now)
      images <- dbRef.inTransaction {
        for {
          _ <- clusterQuery.updateClusterHostIp(runtimeAndRuntimeConfig.runtime.id, Some(ip), now)
          images <- clusterImageQuery.getAllForCluster(runtimeAndRuntimeConfig.runtime.id)
        } yield images.toList
      }
      availableTools <- images.traverseFilter { image =>
        RuntimeContainerServiceType.imageTypeToRuntimeContainerServiceType
          .get(image.imageType)
          .traverse(
            _.isProxyAvailable(runtimeAndRuntimeConfig.runtime.googleProject,
                               runtimeAndRuntimeConfig.runtime.runtimeName).map(b => (image.imageType, b))
          )
      }
    } yield availableTools match {
      case a if a.forall(_._2) =>
        ReadyCluster(runtimeAndRuntimeConfig, ip, dataprocInstances)
      case a =>
        NotReadyCluster(
          runtimeAndRuntimeConfig,
          RuntimeStatus.Running,
          dataprocInstances,
          Some(s"Services not available: ${a.collect { case x if x._2 == false => x._1 }}")
        )
    }

  private def persistInstances(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                               dataprocInstances: Set[DataprocInstance]): IO[Unit] =
    runtimeAndRuntimeConfig.runtimeConfig.cloudService match {
      case CloudService.GCE => IO.unit
      case CloudService.Dataproc =>
        dbRef.inTransaction {
          clusterQuery.mergeInstances(runtimeAndRuntimeConfig.runtime.copy(dataprocInstances = dataprocInstances))
        }.void
    }

  private def saveClusterError(runtime: Runtime, errorMessage: String, errorCode: Int): IO[Unit] =
    dbRef
      .inTransaction {
        val clusterId = clusterQuery.getIdByUniqueKey(runtime)
        clusterId flatMap {
          case Some(a) => clusterErrorQuery.save(a, RuntimeError(errorMessage, errorCode, Instant.now))
          case None => {
            logger.warn(
              s"Could not find Id for Cluster ${runtime.projectNameString}  with google cluster ID ${runtime.asyncRuntimeFields
                .map(_.googleId)}."
            )
            DBIOAction.successful(0)
          }
        }
      }
      .void
      .adaptError {
        case e => new Exception(s"Error persisting cluster error with message '${errorMessage}' to database: ${e}", e)
      }

  private def persistClusterErrors(errorDetails: RuntimeErrorDetails, runtime: Runtime): IO[Unit] = {
    val result = runtime.asyncRuntimeFields.map(_.stagingBucket) match {
      case Some(stagingBucketName) => {
        for {
          metadata <- google2StorageDAO
            .getObjectMetadata(stagingBucketName, GcsBlobName("userscript_output.txt"), None)
            .compile
            .last
          userscriptFailed = metadata match {
            case Some(GetMetadataResponse.Metadata(_, metadataMap, _)) => metadataMap.exists(_ == "passed" -> "false")
            case _                                                     => false
          }
          _ <- if (userscriptFailed) {
            saveClusterError(runtime,
                             s"Userscript failed. See output in gs://${stagingBucketName}/userscript_output.txt",
                             errorDetails.code)
          } else {
            // save dataproc cluster errors to the DB
            saveClusterError(runtime, errorDetails.message.getOrElse("Error not available"), errorDetails.code)
          }
        } yield ()
      }
      case None => {
        // in the case of an internal error, the staging bucket field is usually None
        saveClusterError(runtime, errorDetails.message.getOrElse("Error not available"), errorDetails.code)
      }
    }
    result.onError {
      case e =>
        IO(
          logger.error(s"Failed to persist cluster errors for cluster ${runtime.projectNameString}: ${e.getMessage}", e)
        )
    }
  }

  private def getDataprocInstances(
    runtime: Runtime
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Set[DataprocInstance]] =
    for {
      map <- IO.fromFuture(IO(gdDAO.getClusterInstances(runtime.googleProject, runtime.runtimeName)))
      now <- IO(Instant.now)
      instances <- map.toList.flatTraverse {
        case (role, keys) =>
          keys.toList.traverseFilter { key =>
            googleComputeService.getInstance(key.project, key.zone, key.name).map { instanceOpt =>
              instanceOpt.map { instance =>
                DataprocInstance(
                  key,
                  BigInt(instance.getId),
                  GceInstanceStatus.withNameInsensitive(instance.getStatus),
                  getInstanceIP(instance),
                  role,
                  parseGoogleTimestamp(instance.getCreationTimestamp).getOrElse(now)
                )
              }
            }
          }
      }
    } yield instances.toSet

  private def getMasterIp(
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Option[IP]] = {
    val transformed = for {
      (project, zone, name) <- runtimeAndRuntimeConfig.runtimeConfig.cloudService match {
        case CloudService.GCE =>
          OptionT.liftF(
            IO.pure(
              (runtimeAndRuntimeConfig.runtime.googleProject,
               gceConfig.zoneName,
               InstanceName(runtimeAndRuntimeConfig.runtime.runtimeName.asString))
            )
          )
        case CloudService.Dataproc =>
          OptionT(
            IO.fromFuture(
              IO(
                gdDAO.getClusterMasterInstance(runtimeAndRuntimeConfig.runtime.googleProject,
                                               runtimeAndRuntimeConfig.runtime.runtimeName)
              )
            )
          ).map(key => (key.project, key.zone, key.name))
      }
      masterInstance <- OptionT(googleComputeService.getInstance(project, zone, name))
      masterIp <- OptionT.fromOption[IO](getInstanceIP(masterInstance))
    } yield masterIp

    transformed.value
  }

  private def deleteInitBucket(runtime: Runtime): IO[Unit] =
    // Get the init bucket path for this cluster, then delete the bucket in Google.
    dbRef.inTransaction {
      clusterQuery.getInitBucket(runtime.googleProject, runtime.runtimeName)
    } flatMap {
      case None =>
        IO(logger.warn(s"Could not lookup init bucket for cluster ${runtime.projectNameString}: cluster not in db"))
      case Some(bucketPath) =>
        google2StorageDAO
          .deleteBucket(runtime.googleProject, bucketPath.bucketName, isRecursive = true)
          .compile
          .drain <*
          IO(
            logger.debug(s"Deleted init bucket $bucketPath for cluster ${runtime.googleProject}/${runtime.runtimeName}")
          )
    }

  private def setStagingBucketLifecycle(runtime: Runtime): IO[Unit] =
    // Get the staging bucket path for this cluster, then set the age for it to be deleted the specified number of days after the deletion of the cluster.
    dbRef.inTransaction {
      clusterQuery.getStagingBucket(runtime.googleProject, runtime.runtimeName)
    } flatMap {
      case None =>
        IO(
          logger.warn(s"Could not lookup staging bucket for cluster ${runtime.projectNameString}: cluster not in db")
        )
      case Some(bucketPath) =>
        val ageToDelete = runtime.auditInfo.createdDate
          .until(Instant.now(), ChronoUnit.DAYS)
          .toInt + clusterBucketConfig.stagingBucketExpiration.toDays.toInt
        val condition = BucketInfo.LifecycleRule.LifecycleCondition.newBuilder().setAge(ageToDelete).build()
        val action = BucketInfo.LifecycleRule.LifecycleAction.newDeleteAction()
        val rule = new BucketInfo.LifecycleRule(action, condition)
        google2StorageDAO.setBucketLifecycle(bucketPath.bucketName, List(rule), None).compile.drain.map { _ =>
          logger.debug(
            s"Set staging bucket $bucketPath for cluster ${runtime.projectNameString} to be deleted in ${ageToDelete} days."
          )
        } handleErrorWith { error =>
          IO(
            logger.error(s"Error occurred setting staging bucket lifecycle for cluster ${runtime.projectNameString}",
                         error)
          )
        }
    }

  private def getDbRuntimeAndRuntimeConfig: IO[RuntimeAndRuntimeConfig] =
    for {
      clusterOpt <- dbRef.inTransaction(clusterQuery.getClusterById(clusterId))
      cluster <- IO.fromEither(
        clusterOpt.toRight(new Exception(s"Cluster with id ${clusterId} not found in the database"))
      )
      runtimeConfig <- dbRef.inTransaction(RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId))
    } yield RuntimeAndRuntimeConfig(cluster, runtimeConfig)
}
