package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import akka.actor.Status.Failure
import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.StatusCodes
import akka.pattern.pipe
import cats.data.OptionT
import cats.effect.{ContextShift, IO}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.typesafe.scalalogging.LazyLogging
import io.grpc.Status.Code
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
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
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  RuntimeFollowupDetails,
  RuntimeTransitionMessage
}
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.{GcsLifecycleTypes, GoogleProject}
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics
import org.broadinstitute.dsde.workbench.util.{addJitter, Retry}
import slick.dbio.DBIOAction

import scala.collection.immutable.Set
import scala.concurrent.duration._

case class ProxyDAONotFound(clusterName: RuntimeName, googleProject: GoogleProject, clusterTool: RuntimeImageType)
    extends LeoException(s"Cluster ${clusterName}/${googleProject} was initialized with invalid tool: ${clusterTool}",
                         StatusCodes.InternalServerError)

object ClusterMonitorActor {

  /**
   * Creates a Props object used for creating a {{{ClusterMonitorActor}}}.
   */
  def props(
    clusterId: Long,
    monitorConfig: MonitorConfig,
    dataprocConfig: DataprocConfig,
    gceConfig: GceConfig,
    imageConfig: ImageConfig,
    clusterBucketConfig: ClusterBucketConfig,
    gdDAO: GoogleDataprocDAO,
    googleComputeService: GoogleComputeService[IO],
    googleStorageDAO: GoogleStorageDAO,
    google2StorageDAO: GoogleStorageService[IO],
    dbRef: DbReference[IO],
    authProvider: LeoAuthProvider[IO],
    publisherQueue: fs2.concurrent.InspectableQueue[IO, LeoPubsubMessage]
  )(implicit metrics: NewRelicMetrics[IO],
    runtimeToolToToolDao: RuntimeContainerServiceType => ToolDAO[RuntimeContainerServiceType],
    cs: ContextShift[IO],
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
                              googleStorageDAO,
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
 */
class ClusterMonitorActor(
  val clusterId: Long,
  val monitorConfig: MonitorConfig,
  val dataprocConfig: DataprocConfig,
  val gceConfig: GceConfig,
  val imageConfig: ImageConfig,
  val clusterBucketConfig: ClusterBucketConfig,
  val gdDAO: GoogleDataprocDAO,
  val googleComputeService: GoogleComputeService[IO],
  val googleStorageDAO: GoogleStorageDAO,
  val google2StorageDAO: GoogleStorageService[IO],
  val dbRef: DbReference[IO],
  val authProvider: LeoAuthProvider[IO],
  val publisherQueue: fs2.concurrent.InspectableQueue[IO, LeoPubsubMessage],
  val startTime: Long = System.currentTimeMillis()
)(implicit metrics: NewRelicMetrics[IO],
  runtimeToolToToolDao: RuntimeContainerServiceType => ToolDAO[RuntimeContainerServiceType],
  cs: ContextShift[IO],
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
            _ <- dbRef.inTransaction { clusterQuery.setToStopping(runtimeAndRuntimeConfig.runtime.id, now) }
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
            s"Cluster ${runtimeAndRuntimeConfig.runtime.projectNameString} is not ready yet and has taken ${currTimeElapsed.toSeconds} seconds so far (Dataproc cluster status = $googleStatus, GCE instance statuses = ${dataprocInstances
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
                                 dataprocInstances: Set[DataprocInstance]): IO[ClusterMonitorMessage] =
    for {
      // Remove credentials from instance metadata.
      // Only happens if an notebook service account was used.
      _ <- if (runtimeAndRuntimeConfig.runtime.status == RuntimeStatus.Creating)
        removeCredentialsFromMetadata(runtimeAndRuntimeConfig.runtime)
      else IO.unit
      // create or update instances in the DB
      _ <- persistInstances(runtimeAndRuntimeConfig, dataprocInstances)
      // update DB after auth futures finish
      now <- IO(Instant.now)
      _ <- dbRef.inTransaction { clusterQuery.setToRunning(runtimeAndRuntimeConfig.runtime.id, publicIp, now) }
      // Record metrics in NewRelic
      _ <- recordStatusTransitionMetrics(getRuntimeUI(runtimeAndRuntimeConfig.runtime),
                                         runtimeAndRuntimeConfig.runtime.status,
                                         RuntimeStatus.Running)
      _ <- if (runtimeAndRuntimeConfig.runtime.status == RuntimeStatus.Creating)
        recordClusterCreationMetrics(runtimeAndRuntimeConfig.runtime.auditInfo.createdDate,
                                     runtimeAndRuntimeConfig.runtime.runtimeImages)
      else IO.unit
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
      _ <- recordStatusTransitionMetrics(getRuntimeUI(runtimeAndRuntimeConfig.runtime),
                                         runtimeAndRuntimeConfig.runtime.status,
                                         RuntimeStatus.Error)

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
          _ <- metrics.incrementCounter(s"AsyncClusterCreationFailure/${errorDetails.code}")

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
    logger.info(s"Cluster ${runtimeAndRuntimeConfig.runtime.projectNameString} has been deleted.")

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
      _ <- recordStatusTransitionMetrics(getRuntimeUI(runtimeAndRuntimeConfig.runtime),
                                         runtimeAndRuntimeConfig.runtime.status,
                                         RuntimeStatus.Deleted)

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
      _ <- dbRef.inTransaction { clusterQuery.clearKernelFoundBusyDate(runtimeAndRuntimeConfig.runtime.id, now) }
      traceId <- ev.ask
      _ <- publisherQueue.enqueue1(
        RuntimeTransitionMessage(RuntimeFollowupDetails(clusterId, RuntimeStatus.Stopped), Some(traceId))
      )
      // Record metrics in NewRelic
      _ <- recordStatusTransitionMetrics(getRuntimeUI(runtimeAndRuntimeConfig.runtime),
                                         runtimeAndRuntimeConfig.runtime.status,
                                         RuntimeStatus.Stopped)
    } yield ShutdownActor(RemoveFromList(runtimeAndRuntimeConfig.runtime))
  }

  private def checkCluster(implicit ev: ApplicativeAsk[IO, TraceId]): IO[ClusterMonitorMessage] =
    for {
      runtimeAndRuntimeConfig <- getDbRuntimeAndRuntimeConfig
      next <- runtimeAndRuntimeConfig.runtime.status match {
        case status if status.isMonitored =>
          checkClusterInGoogle(runtimeAndRuntimeConfig)
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
  private def checkClusterInGoogle(
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[ClusterMonitorMessage] =
    for {
      googleStatus <- runtimeAndRuntimeConfig.runtimeConfig.cloudService match {
        case CloudService.GCE =>
          googleComputeService
            .getInstance(runtimeAndRuntimeConfig.runtime.googleProject,
                         gceConfig.zoneName,
                         InstanceName(runtimeAndRuntimeConfig.runtime.runtimeName.asString))
            .map { instanceOpt =>
              instanceOpt.fold[RuntimeStatus](RuntimeStatus.Deleted)(
                instance => RuntimeStatus.withNameInsensitiveOption(instance.getStatus).getOrElse(RuntimeStatus.Unknown)
              )
            }
        case CloudService.Dataproc =>
          IO.fromFuture(
            IO(
              gdDAO.getClusterStatus(runtimeAndRuntimeConfig.runtime.googleProject,
                                     runtimeAndRuntimeConfig.runtime.runtimeName)
            )
          )
      }

      dataprocInstances <- runtimeAndRuntimeConfig.runtimeConfig.cloudService match {
        case CloudService.GCE      => IO.pure(Set.empty[DataprocInstance])
        case CloudService.Dataproc => getDataprocInstances(runtimeAndRuntimeConfig.runtime)
      }

      runningInstanceCount = dataprocInstances.count(_.status == InstanceStatus.Running)
      stoppedInstanceCount = dataprocInstances.count(
        i => i.status == InstanceStatus.Stopped || i.status == InstanceStatus.Terminated
      )

      result <- googleStatus match {
        case Unknown | Creating | Updating | Stopping | Starting =>
          IO.pure(NotReadyCluster(runtimeAndRuntimeConfig, googleStatus, dataprocInstances))
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
        case _ => IO.pure(NotReadyCluster(runtimeAndRuntimeConfig, googleStatus, dataprocInstances))
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
                  InstanceStatus.withNameInsensitive(instance.getStatus),
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
        IO.fromFuture(IO(googleStorageDAO.deleteBucket(bucketPath.bucketName, recurse = true))) <*
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
        IO.fromFuture(
          IO(googleStorageDAO.setBucketLifecycle(bucketPath.bucketName, ageToDelete, GcsLifecycleTypes.Delete))
        ) map { _ =>
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

  private def removeCredentialsFromMetadata(runtime: Runtime): IO[Unit] =
    runtime.serviceAccountInfo.notebookServiceAccount match {
      // No notebook service account: don't remove creds from metadata! We need them.
      case None => IO.unit

      // Remove credentials from instance metadata.
      // We want to ensure that _only_ the notebook service account is used;
      // users should not be able to yank the cluster SA credentials from the metadata server.
      case Some(_) =>
        // TODO https://github.com/DataBiosphere/leonardo/issues/128
        IO.unit
    }

  private def getDbRuntimeAndRuntimeConfig: IO[RuntimeAndRuntimeConfig] =
    for {
      clusterOpt <- dbRef.inTransaction { clusterQuery.getClusterById(clusterId) }
      cluster <- IO.fromEither(
        clusterOpt.toRight(new Exception(s"Cluster with id ${clusterId} not found in the database"))
      )
      runtimeConfig <- dbRef.inTransaction { RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId) }
    } yield RuntimeAndRuntimeConfig(cluster, runtimeConfig)

  private def recordStatusTransitionMetrics(clusterUI: RuntimeUI,
                                            origStatus: RuntimeStatus,
                                            finalStatus: RuntimeStatus): IO[Unit] =
    for {
      endTime <- IO(System.currentTimeMillis)
      baseName = s"ClusterMonitor/${clusterUI.asString}/${origStatus}->${finalStatus}"
      counterName = s"${baseName}/count"
      timerName = s"${baseName}/timer"
      duration = (endTime - startTime).millis
      _ <- metrics.incrementCounter(counterName).runAsync(_ => IO.unit).toIO
      _ <- metrics.recordResponseTime(timerName, duration).runAsync(_ => IO.unit).toIO
    } yield ()

  private def findToolImageInfo(images: Set[RuntimeImage]): String = {
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

  private def recordClusterCreationMetrics(createdDate: Instant, images: Set[RuntimeImage]): IO[Unit] = {
    val res = for {
      endTime <- IO(Instant.now())
      toolImageInfo = findToolImageInfo(images)
      baseName = s"ClusterMonitor/ClusterCreation/${toolImageInfo}"
      counterName = s"${baseName}/count"
      timerName = s"${baseName}/timer"
      duration = Duration(ChronoUnit.MILLIS.between(createdDate, endTime), MILLISECONDS)
      _ <- metrics.incrementCounter(counterName)
      _ <- metrics.recordResponseTime(timerName, duration)
    } yield ()

    res.runAsync(_ => IO.unit).toIO
  }

  def getRuntimeUI(runtime: Runtime): RuntimeUI =
    if (runtime.labels.contains(Config.uiConfig.terraLabel)) RuntimeUI.Terra
    else if (runtime.labels.contains(Config.uiConfig.allOfUsLabel)) RuntimeUI.AoU
    else RuntimeUI.Other
}
