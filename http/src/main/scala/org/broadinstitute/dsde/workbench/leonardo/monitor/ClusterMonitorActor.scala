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
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GetMetadataResponse, GoogleComputeService, GoogleStorageService}
import org.broadinstitute.dsde.workbench.leonardo.RuntimeStatus.{Creating, Deleted, Deleting, Error, Running, Starting, Stopping, Unknown, Updating}
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.dao.ToolDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleDataprocDAO, _}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorActor.ClusterMonitorMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorActor._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.{ClusterDeleted, ClusterSupervisorMessage, RemoveFromList}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{ClusterFollowupDetails, ClusterTransition}
import org.broadinstitute.dsde.workbench.leonardo.util.{DataprocAlgebra, DeleteRuntimeParams, FinalizeDeleteParams, StopRuntimeParams}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.{GcsLifecycleTypes, GoogleProject}
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics
import org.broadinstitute.dsde.workbench.util.{Retry, addJitter}
import slick.dbio.DBIOAction

import scala.collection.immutable.Set
import scala.concurrent.duration._

case class ProxyDAONotFound(clusterName: ClusterName, googleProject: GoogleProject, clusterTool: RuntimeImageType)
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
             imageConfig: ImageConfig,
             clusterBucketConfig: ClusterBucketConfig,
             gdDAO: GoogleDataprocDAO,
             googleComputeService: GoogleComputeService[IO],
             googleStorageDAO: GoogleStorageDAO,
             google2StorageDAO: GoogleStorageService[IO],
             dbRef: DbReference[IO],
             authProvider: LeoAuthProvider[IO],
             dataprocAlg: DataprocAlgebra[IO],
             publisherQueue: fs2.concurrent.InspectableQueue[IO, LeoPubsubMessage]
  )(implicit metrics: NewRelicMetrics[IO],
    runtimeToolToToolDao: RuntimeContainerServiceType => ToolDAO[RuntimeContainerServiceType],
    cs: ContextShift[IO]): Props =
    Props(
      new ClusterMonitorActor(clusterId,
                              monitorConfig,
                              dataprocConfig,
                              imageConfig,
                              clusterBucketConfig,
                              gdDAO,
                              googleComputeService,
                              googleStorageDAO,
                              google2StorageDAO,
                              dbRef,
                              authProvider,
        dataprocAlg,
                              publisherQueue)
    )

  // ClusterMonitorActor messages:
  sealed trait ClusterMonitorMessage extends Product with Serializable
  object ClusterMonitorMessage {
    case object ScheduleMonitorPass extends ClusterMonitorMessage
    case object QueryForCluster extends ClusterMonitorMessage
    case class ReadyCluster(cluster: Cluster, publicIP: IP, googleInstances: Set[Instance])
        extends ClusterMonitorMessage
    case class NotReadyCluster(cluster: Cluster,
                               googleStatus: RuntimeStatus,
                               googleInstances: Set[Instance],
                               msg: Option[String] = None)
        extends ClusterMonitorMessage
    case class FailedCluster(cluster: Cluster, errorDetails: RuntimeErrorDetails, googleInstances: Set[Instance])
        extends ClusterMonitorMessage
    case class DeletedCluster(cluster: Cluster) extends ClusterMonitorMessage
    case class StoppedCluster(cluster: Cluster, googleInstances: Set[Instance]) extends ClusterMonitorMessage
    case class ShutdownActor(notifyParentMsg: ClusterSupervisorMessage) extends ClusterMonitorMessage
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
  val imageConfig: ImageConfig,
  val clusterBucketConfig: ClusterBucketConfig,
  val gdDAO: GoogleDataprocDAO,
  val googleComputeService: GoogleComputeService[IO],
  val googleStorageDAO: GoogleStorageDAO,
  val google2StorageDAO: GoogleStorageService[IO],
  val dbRef: DbReference[IO],
  val authProvider: LeoAuthProvider[IO],
  val dataprocAlg: DataprocAlgebra[IO],
  val publisherQueue: fs2.concurrent.InspectableQueue[IO, LeoPubsubMessage],
  val startTime: Long = System.currentTimeMillis()
)(implicit metrics: NewRelicMetrics[IO],
  runtimeToolToToolDao: RuntimeContainerServiceType => ToolDAO[RuntimeContainerServiceType],
  cs: ContextShift[IO])
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

    case NotReadyCluster(cluster, googleStatus, googleInstances, msg) =>
      implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))
      handleNotReadyCluster(cluster, googleStatus, googleInstances, msg).unsafeToFuture() pipeTo self

    case ReadyCluster(cluster, ip, googleInstances) =>
      handleReadyCluster(cluster, ip, googleInstances).unsafeToFuture() pipeTo self

    case FailedCluster(cluster, errorDetails, googleInstances) =>
      implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))
      handleFailedCluster(cluster, errorDetails, googleInstances).unsafeToFuture() pipeTo self

    case DeletedCluster(cluster) =>
      implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))
      val res = handleDeletedCluster(cluster).onError { case e => IO(logger.warn("Error deleting cluster", e)) }
      res.unsafeToFuture() pipeTo self

    case StoppedCluster(cluster, googleInstances) =>
      handleStoppedCluster(cluster, googleInstances).unsafeToFuture() pipeTo self

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
   * @param cluster the cluster being monitored
   * @param googleStatus the ClusterStatus from Google
   * @param googleInstances the cluster's instances in Google
   * @return ScheduleMonitorPass
   */
  private def handleNotReadyCluster(
    cluster: Cluster,
    googleStatus: RuntimeStatus,
    googleInstances: Set[Instance],
    msg: Option[String]
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[ClusterMonitorMessage] = {
    val currTimeElapsed: FiniteDuration = (System.currentTimeMillis() - startTime).millis
    monitorConfig.monitorStatusTimeouts.get(cluster.status) match {
      case Some(timeLimit) if currTimeElapsed > timeLimit =>
        logger.info(s"Detected that ${cluster.projectNameString} has been stuck in status ${cluster.status} too long.")

        // Take care not to Error out a cluster if it timed out in Starting status
        if (cluster.status == Starting) {
          for {
            _ <- persistInstances(cluster, googleInstances)
            runtimeConfig <- dbRef.inTransaction(
              RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId)
            )
            _ <- dataprocAlg.stopRuntime(StopRuntimeParams(cluster, runtimeConfig))
            now <- IO(Instant.now)
            _ <- dbRef.inTransaction { clusterQuery.setToStopping(cluster.id, now) }
          } yield ScheduleMonitorPass
        } else {
          handleFailedCluster(
            cluster,
            RuntimeErrorDetails(
              Code.DEADLINE_EXCEEDED.value,
              Some(
                s"Failed to transition ${cluster.projectNameString} from status ${cluster.status} within the time limit: ${timeLimit.toSeconds} seconds"
              )
            ),
            googleInstances
          )
        }

      case _ =>
        IO(
          logger.info(
            s"Cluster ${cluster.projectNameString} is not ready yet and has taken ${currTimeElapsed.toSeconds} seconds so far (Dataproc cluster status = $googleStatus, GCE instance statuses = ${googleInstances
              .groupBy(_.status)
              .mapValues(_.size)}). Checking again in ${monitorConfig.pollPeriod.toString}. ${msg.getOrElse("")}"
          )
        ) >> persistInstances(cluster, googleInstances).as(ScheduleMonitorPass)
    }
  }

  /**
   * Handles a dataproc cluster which is ready. We update the status and IP in the database,
   * then shut down this actor.
   * @param publicIp the cluster public IP, according to Google
   * @return ShutdownActor
   */
  private def handleReadyCluster(cluster: Cluster,
                                 publicIp: IP,
                                 googleInstances: Set[Instance]): IO[ClusterMonitorMessage] =
    for {
      // Remove credentials from instance metadata.
      // Only happens if an notebook service account was used.
      _ <- if (cluster.status == RuntimeStatus.Creating) removeCredentialsFromMetadata(cluster) else IO.unit
      // create or update instances in the DB
      _ <- persistInstances(cluster, googleInstances)
      // update DB after auth futures finish
      now <- IO(Instant.now)
      _ <- dbRef.inTransaction { clusterQuery.setToRunning(cluster.id, publicIp, now) }
      // Record metrics in NewRelic
      _ <- recordStatusTransitionMetrics(getRuntimeUI(cluster), cluster.status, RuntimeStatus.Running)
      _ <- if (cluster.status == RuntimeStatus.Creating)
        recordClusterCreationMetrics(cluster.auditInfo.createdDate, cluster.runtimeImages)
      else IO.unit
      // Finally pipe a shutdown message to this actor
      _ <- IO(logger.info(s"Cluster ${cluster.googleProject}/${cluster.runtimeName} is ready for use!"))
    } yield ShutdownActor(RemoveFromList(cluster))

  /**
   * Handles a dataproc cluster which has failed. We delete the cluster in Google, and then:
   * - if this is a recoverable error, recreate the cluster
   * - otherwise, just set the status to Error and stop monitoring the cluster
   * @param errorDetails cluster error details from Google
   * @return ShutdownActor
   */
  private def handleFailedCluster(cluster: Cluster,
                                  errorDetails: RuntimeErrorDetails,
                                  googleInstances: Set[Instance])(implicit ev: ApplicativeAsk[IO, TraceId]): IO[ClusterMonitorMessage] =
    for {
      _ <- List(
        // Delete the cluster in Google
        dataprocAlg.deleteRuntime(DeleteRuntimeParams(cluster)),
        // create or update instances in the DB
        persistInstances(cluster, googleInstances),
        //save cluster error in the DB
        persistClusterErrors(errorDetails, cluster)
      ).parSequence_

      // Record metrics in NewRelic
      _ <- recordStatusTransitionMetrics(getRuntimeUI(cluster), cluster.status, RuntimeStatus.Error)

      now <- IO(Instant.now)

      // Decide if we should try recreating the cluster
      res <- if (shouldRecreateCluster(cluster, errorDetails.code, errorDetails.message)) {
        // Update the database record to Deleting, shutdown this actor, and register a callback message
        // to the supervisor telling it to recreate the cluster.
        IO(
          logger.info(
            s"Cluster ${cluster.projectNameString} is in an error state with $errorDetails. Attempting to recreate..."
          )
        ) >>
          dbRef
            .inTransaction {
              clusterQuery.markPendingDeletion(cluster.id, now)
            }
            .as(ShutdownActor(ClusterDeleted(cluster, recreate = true)))
      } else {
        for {
          // Update the database record to Error and shutdown this actor.
          _ <- IO(
            logger.warn(
              s"Cluster ${cluster.projectNameString} is in an error state with $errorDetails'. Unable to recreate cluster."
            )
          )
          // update the cluster status to Error
          _ <- dbRef.inTransaction {
            clusterQuery.updateClusterStatus(cluster.id, RuntimeStatus.Error, now)
          }
          _ <- metrics.incrementCounter(s"AsyncClusterCreationFailure/${errorDetails.code}")

          // Remove the Dataproc Worker IAM role for the pet service account
          // Only happens if the cluster was created with the pet service account.
        } yield ShutdownActor(RemoveFromList(cluster))
      }
    } yield res

  private def shouldRecreateCluster(cluster: Cluster, code: Int, message: Option[String]): Boolean = {
    // TODO: potentially add more checks here as we learn which errors are recoverable
    logger.info(s"determining if we should re-create cluster ${cluster.projectNameString}")
    monitorConfig.recreateCluster && (code == Code.UNKNOWN.value)
  }

  /**
   * Handles a dataproc cluster which has been deleted.
   * We update the status to Deleted in the database, notify the auth provider,
   * and shut down this actor.
   * @return error or ShutdownActor
   */
  private def handleDeletedCluster(
    cluster: Cluster
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[ClusterMonitorMessage] = {
    logger.info(s"Cluster ${cluster.projectNameString} has been deleted.")

    for {
      // delete the init bucket so we don't continue to accrue costs after cluster is deleted
      _ <- deleteInitBucket(cluster)

      // set the staging bucket to be deleted in ten days so that logs are still accessible until then
      _ <- setStagingBucketLifecycle(cluster)

      // delete instances in the DB
      _ <- persistInstances(cluster, Set.empty)

      now <- IO(Instant.now)

      _ <- dbRef.inTransaction {
        clusterQuery.completeDeletion(cluster.id, now)
      }
      _ <- authProvider
        .notifyClusterDeleted(cluster.internalId,
                              cluster.auditInfo.creator,
                              cluster.auditInfo.creator,
                              cluster.googleProject,
                              cluster.runtimeName)

      _ <- dataprocAlg.finalizeDelete(FinalizeDeleteParams(cluster))

      // Record metrics in NewRelic
      _ <- recordStatusTransitionMetrics(getRuntimeUI(cluster), cluster.status, RuntimeStatus.Deleted)

    } yield ShutdownActor(RemoveFromList(cluster))
  }

  /**
   * Handles a dataproc cluster which has been stopped.
   * We update the status to Stopped in the database and shut down this actor.
   * @return ShutdownActor
   */
  private def handleStoppedCluster(cluster: Cluster, googleInstances: Set[Instance]): IO[ClusterMonitorMessage] = {
    logger.info(s"Cluster ${cluster.projectNameString} has been stopped.")

    for {
      // create or update instances in the DB
      _ <- persistInstances(cluster, googleInstances)
      now <- IO(Instant.now)
      // this sets the cluster status to stopped and clears the cluster IP
      _ <- dbRef.inTransaction {
        clusterQuery.updateClusterStatus(cluster.id, RuntimeStatus.Stopped, now)
      }
      // reset the time at which the kernel was last found to be busy
      _ <- dbRef.inTransaction { clusterQuery.clearKernelFoundBusyDate(cluster.id, now) }
      traceId = TraceId(UUID.randomUUID())
      _ <- publisherQueue.enqueue1(
        ClusterTransition(ClusterFollowupDetails(clusterId, RuntimeStatus.Stopped), Some(traceId))
      )
      // Record metrics in NewRelic
      _ <- recordStatusTransitionMetrics(getRuntimeUI(cluster), cluster.status, RuntimeStatus.Stopped)
    } yield ShutdownActor(RemoveFromList(cluster))
  }

  private def checkCluster(implicit ev: ApplicativeAsk[IO, TraceId]): IO[ClusterMonitorMessage] =
    for {
      dbCluster <- getDbCluster
      next <- dbCluster.status match {
        case status if status.isMonitored =>
          checkClusterInGoogle(dbCluster)
        case status =>
          IO(logger.info(s"Stopping monitoring of cluster ${dbCluster.projectNameString} in status ${status}")) >>
            IO.pure(ShutdownActor(RemoveFromList(dbCluster)))
      }
    } yield next

  /**
   * Queries Google for the cluster status and takes appropriate action depending on the result.
   * @return ClusterMonitorMessage
   */
  private def checkClusterInGoogle(
    cluster: Cluster
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[ClusterMonitorMessage] =
    for {
      googleStatus <- IO.fromFuture(IO(gdDAO.getClusterStatus(cluster.googleProject, cluster.runtimeName)))

      googleInstances <- getClusterInstances(cluster)

      runningInstanceCount = googleInstances.count(_.status == InstanceStatus.Running)
      stoppedInstanceCount = googleInstances.count(
        i => i.status == InstanceStatus.Stopped || i.status == InstanceStatus.Terminated
      )

      result <- googleStatus match {
        case Unknown | Creating | Updating =>
          IO.pure(NotReadyCluster(cluster, googleStatus, googleInstances))
        // Take care we don't restart a Deleting or Stopping cluster if google hasn't updated their status yet
        case Running
            if cluster.status != Deleting && cluster.status != Stopping && runningInstanceCount == googleInstances.size =>
          getMasterIp(cluster).flatMap {
            case Some(ip) => checkClusterTools(cluster, ip, googleInstances)
            case None =>
              IO.pure(
                NotReadyCluster(cluster, RuntimeStatus.Running, googleInstances, Some("Could not retrieve master IP"))
              )
          }
        // Take care we don't fail a Deleting or Stopping cluster if google hasn't updated their status yet
        case Error if cluster.status != Deleting && cluster.status != Stopping =>
          IO.fromFuture(IO(gdDAO.getClusterErrorDetails(cluster.asyncRuntimeFields.map(_.operationName)))).map {
            case Some(errorDetails) => FailedCluster(cluster, errorDetails, googleInstances)
            case None =>
              FailedCluster(cluster, RuntimeErrorDetails(Code.INTERNAL.value, Some(internalError)), googleInstances)
          }
        // Take care we don't delete a Creating cluster if google hasn't updated their status yet
        case Deleted if cluster.status == Creating =>
          IO.pure(NotReadyCluster(cluster, RuntimeStatus.Creating, googleInstances))
        case Deleted =>
          IO.pure(DeletedCluster(cluster))
        // if the cluster only contains stopped instances, it's a stopped cluster
        case _
            if cluster.status != Starting && cluster.status != Deleting && stoppedInstanceCount == googleInstances.size =>
          IO.pure(StoppedCluster(cluster, googleInstances))
        case _ => IO.pure(NotReadyCluster(cluster, googleStatus, googleInstances))
      }
    } yield result

  private def checkClusterTools(cluster: Cluster, ip: IP, googleInstances: Set[Instance]): IO[ClusterMonitorMessage] =
    // Update the Host IP in the database so DNS cache can be properly populated with the first cache miss
    // Otherwise, when a cluster is resumed and transitions from Starting to Running, we get stuck
    // in that state - at least with the way HttpJupyterDAO.isProxyAvailable works
    for {
      now <- IO(Instant.now)
      images <- dbRef.inTransaction {
        for {
          _ <- clusterQuery.updateClusterHostIp(cluster.id, Some(ip), now)
          images <- clusterImageQuery.getAllForCluster(cluster.id)
        } yield images.toList
      }
      availableTools <- images.traverseFilter { image =>
        RuntimeContainerServiceType.imageTypeToRuntimeContainerServiceType
          .get(image.imageType)
          .traverse(
            _.isProxyAvailable(cluster.googleProject, cluster.runtimeName).map(b => (image.imageType, b))
          )
      }
    } yield availableTools match {
      case a if a.forall(_._2) =>
        ReadyCluster(cluster, ip, googleInstances)
      case a =>
        NotReadyCluster(
          cluster,
          RuntimeStatus.Running,
          googleInstances,
          Some(s"Services not available: ${a.collect { case x if x._2 == false => x._1 }}")
        )
    }

  private def persistInstances(cluster: Cluster, googleInstances: Set[Instance]): IO[Unit] =
    dbRef.inTransaction {
      clusterQuery.mergeInstances(cluster.copy(dataprocInstances = googleInstances))
    }.void

  private def saveClusterError(cluster: Cluster, errorMessage: String, errorCode: Int): IO[Unit] =
    dbRef
      .inTransaction {
        val clusterId = clusterQuery.getIdByUniqueKey(cluster)
        clusterId flatMap {
          case Some(a) => clusterErrorQuery.save(a, RuntimeError(errorMessage, errorCode, Instant.now))
          case None => {
            logger.warn(
              s"Could not find Id for Cluster ${cluster.projectNameString}  with google cluster ID ${cluster.asyncRuntimeFields
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

  private def persistClusterErrors(errorDetails: RuntimeErrorDetails, cluster: Cluster): IO[Unit] = {
    val result = cluster.asyncRuntimeFields.map(_.stagingBucket) match {
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
            saveClusterError(cluster,
                             s"Userscript failed. See output in gs://${stagingBucketName}/userscript_output.txt",
                             errorDetails.code)
          } else {
            // save dataproc cluster errors to the DB
            saveClusterError(cluster, errorDetails.message.getOrElse("Error not available"), errorDetails.code)
          }
        } yield ()
      }
      case None => {
        // in the case of an internal error, the staging bucket field is usually None
        saveClusterError(cluster, errorDetails.message.getOrElse("Error not available"), errorDetails.code)
      }
    }
    result.onError {
      case e =>
        IO(logger.error(s"Failed to persist cluster errors for cluster ${cluster}: ${e.getMessage}", e))
    }
  }

  private def getClusterInstances(cluster: Cluster)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Set[Instance]] =
    for {
      map <- IO.fromFuture(IO(gdDAO.getClusterInstances(cluster.googleProject, cluster.runtimeName)))
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

  private def getMasterIp(cluster: Cluster)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Option[IP]] = {
    val transformed = for {
      masterKey <- OptionT(
        IO.fromFuture(IO(gdDAO.getClusterMasterInstance(cluster.googleProject, cluster.runtimeName)))
      )
      masterInstance <- OptionT(googleComputeService.getInstance(masterKey.project, masterKey.zone, masterKey.name))
      masterIp <- OptionT.fromOption[IO](getInstanceIP(masterInstance))
    } yield masterIp

    transformed.value
  }

  private def deleteInitBucket(cluster: Cluster): IO[Unit] =
    // Get the init bucket path for this cluster, then delete the bucket in Google.
    dbRef.inTransaction {
      clusterQuery.getInitBucket(cluster.googleProject, cluster.runtimeName)
    } flatMap {
      case None =>
        IO(logger.warn(s"Could not lookup init bucket for cluster ${cluster.projectNameString}: cluster not in db"))
      case Some(bucketPath) =>
        IO.fromFuture(IO(googleStorageDAO.deleteBucket(bucketPath.bucketName, recurse = true))) <*
          IO(
            logger.debug(s"Deleted init bucket $bucketPath for cluster ${cluster.googleProject}/${cluster.runtimeName}")
          )
    }

  private def setStagingBucketLifecycle(cluster: Cluster): IO[Unit] =
    // Get the staging bucket path for this cluster, then set the age for it to be deleted the specified number of days after the deletion of the cluster.
    dbRef.inTransaction {
      clusterQuery.getStagingBucket(cluster.googleProject, cluster.runtimeName)
    } flatMap {
      case None =>
        IO(
          logger.warn(s"Could not lookup staging bucket for cluster ${cluster.projectNameString}: cluster not in db")
        )
      case Some(bucketPath) =>
        val ageToDelete = cluster.auditInfo.createdDate
          .until(Instant.now(), ChronoUnit.DAYS)
          .toInt + clusterBucketConfig.stagingBucketExpiration.toDays.toInt
        IO.fromFuture(
          IO(googleStorageDAO.setBucketLifecycle(bucketPath.bucketName, ageToDelete, GcsLifecycleTypes.Delete))
        ) map { _ =>
          logger.debug(
            s"Set staging bucket $bucketPath for cluster ${cluster.googleProject}/${cluster.runtimeName} to be deleted in ${ageToDelete} days."
          )
        } handleErrorWith { error =>
          IO(
            logger.error(s"Error occurred setting staging bucket lifecycle for cluster ${cluster.projectNameString}",
                         error)
          )
        }
    }

  private def removeCredentialsFromMetadata(cluster: Cluster): IO[Unit] =
    cluster.serviceAccountInfo.notebookServiceAccount match {
      // No notebook service account: don't remove creds from metadata! We need them.
      case None => IO.unit

      // Remove credentials from instance metadata.
      // We want to ensure that _only_ the notebook service account is used;
      // users should not be able to yank the cluster SA credentials from the metadata server.
      case Some(_) =>
        // TODO https://github.com/DataBiosphere/leonardo/issues/128
        IO.unit
    }

  private def getDbCluster: IO[Cluster] =
    for {
      clusterOpt <- dbRef.inTransaction { clusterQuery.getClusterById(clusterId) }
      cluster <- IO.fromEither(
        clusterOpt.toRight(new Exception(s"Cluster with id ${clusterId} not found in the database"))
      )
    } yield cluster

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

  private def findToolImageInfo(images: Set[ClusterImage]): String = {
    val terraJupyterImage = imageConfig.jupyterImageRegex.r
    val anvilRStudioImage = imageConfig.rstudioImageRegex.r
    val broadDockerhubImageRegex = imageConfig.broadDockerhubImageRegex.r
    images.find(clusterImage => Set(RuntimeImageType.Jupyter, RuntimeImageType.RStudio) contains clusterImage.imageType) match {
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

  private def recordClusterCreationMetrics(createdDate: Instant, clusterImages: Set[ClusterImage]): IO[Unit] = {
    val res = for {
      endTime <- IO(Instant.now())
      toolImageInfo = findToolImageInfo(clusterImages)
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
