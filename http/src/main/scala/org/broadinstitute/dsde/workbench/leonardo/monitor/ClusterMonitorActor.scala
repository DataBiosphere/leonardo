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
import cats.effect.IO
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.typesafe.scalalogging.LazyLogging
import io.grpc.Status.Code
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GetMetadataResponse, GoogleStorageService}
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterBucketConfig, DataprocConfig, MonitorConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.ToolDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus._
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterStatus, IP, _}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorActor.ClusterMonitorMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorActor._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.{
  ClusterDeleted,
  ClusterSupervisorMessage,
  RemoveFromList
}
import org.broadinstitute.dsde.workbench.leonardo.util.ClusterHelper
import org.broadinstitute.dsde.workbench.model.google.{GcsLifecycleTypes, GcsObjectName, GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{ErrorReport, TraceId}
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics
import org.broadinstitute.dsde.workbench.util.{addJitter, Retry}
import slick.dbio.DBIOAction

import scala.collection.immutable.Set
import scala.concurrent.Future
import scala.concurrent.duration._

case class ProxyDAONotFound(clusterName: ClusterName, googleProject: GoogleProject, clusterTool: ClusterTool)
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
    clusterBucketConfig: ClusterBucketConfig,
    gdDAO: GoogleDataprocDAO,
    googleComputeDAO: GoogleComputeDAO,
    googleStorageDAO: GoogleStorageDAO,
    google2StorageDAO: GoogleStorageService[IO],
    dbRef: DbReference,
    authProvider: LeoAuthProvider[IO],
    clusterHelper: ClusterHelper
  )(implicit metrics: NewRelicMetrics[IO], clusterToolToToolDao: ClusterTool => ToolDAO[ClusterTool]): Props =
    Props(
      new ClusterMonitorActor(clusterId,
                              monitorConfig,
                              dataprocConfig,
                              clusterBucketConfig,
                              gdDAO,
                              googleComputeDAO,
                              googleStorageDAO,
                              google2StorageDAO,
                              dbRef,
                              authProvider,
                              clusterHelper)
    )

  // ClusterMonitorActor messages:
  sealed trait ClusterMonitorMessage extends Product with Serializable
  object ClusterMonitorMessage {
    case object ScheduleMonitorPass extends ClusterMonitorMessage
    case object QueryForCluster extends ClusterMonitorMessage
    case class ReadyCluster(cluster: Cluster, publicIP: IP, googleInstances: Set[Instance])
        extends ClusterMonitorMessage
    case class NotReadyCluster(cluster: Cluster,
                               googleStatus: ClusterStatus,
                               googleInstances: Set[Instance],
                               msg: Option[String] = None)
        extends ClusterMonitorMessage
    case class FailedCluster(cluster: Cluster, errorDetails: ClusterErrorDetails, googleInstances: Set[Instance])
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
  val clusterBucketConfig: ClusterBucketConfig,
  val gdDAO: GoogleDataprocDAO,
  val googleComputeDAO: GoogleComputeDAO,
  val googleStorageDAO: GoogleStorageDAO,
  val google2StorageDAO: GoogleStorageService[IO],
  val dbRef: DbReference,
  val authProvider: LeoAuthProvider[IO],
  val clusterHelper: ClusterHelper,
  val startTime: Long = System.currentTimeMillis()
)(implicit metrics: NewRelicMetrics[IO], clusterToolToToolDao: ClusterTool => ToolDAO[ClusterTool])
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
      checkCluster pipeTo self

    case NotReadyCluster(cluster, googleStatus, googleInstances, msg) =>
      handleNotReadyCluster(cluster, googleStatus, googleInstances, msg) pipeTo self

    case ReadyCluster(cluster, ip, googleInstances) =>
      handleReadyCluster(cluster, ip, googleInstances) pipeTo self

    case FailedCluster(cluster, errorDetails, googleInstances) =>
      handleFailedCluster(cluster, errorDetails, googleInstances) pipeTo self

    case DeletedCluster(cluster) =>
      implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))
      handleDeletedCluster(cluster) pipeTo self

    case StoppedCluster(cluster, googleInstances) =>
      handleStoppedCluster(cluster, googleInstances) pipeTo self

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
  private def handleNotReadyCluster(cluster: Cluster,
                                    googleStatus: ClusterStatus,
                                    googleInstances: Set[Instance],
                                    msg: Option[String]): Future[ClusterMonitorMessage] = {
    val currTimeElapsed: FiniteDuration = (System.currentTimeMillis() - startTime).millis
    monitorConfig.monitorStatusTimeouts.get(cluster.status) match {
      case Some(timeLimit) if currTimeElapsed > timeLimit =>
        logger.info(s"Detected that ${cluster.projectNameString} has been stuck in status ${cluster.status} too long.")

        // Take care not to Error out a cluster if it timed out in Starting status
        if (cluster.status == Starting) {
          for {
            _ <- persistInstances(cluster, googleInstances)
            _ <- clusterHelper.stopCluster(cluster).unsafeToFuture()
            now <- IO(Instant.now).unsafeToFuture()
            _ <- dbRef.inTransaction { _.clusterQuery.setToStopping(cluster.id, now) }
          } yield ScheduleMonitorPass
        } else {
          handleFailedCluster(
            cluster,
            ClusterErrorDetails(
              Code.DEADLINE_EXCEEDED.value,
              Some(
                s"Failed to transition ${cluster.projectNameString} from status ${cluster.status} within the time limit: ${timeLimit.toSeconds} seconds"
              )
            ),
            googleInstances
          )
        }

      case _ =>
        logger.info(
          s"Cluster ${cluster.projectNameString} is not ready yet and has taken ${currTimeElapsed.toSeconds} seconds so far (Dataproc cluster status = $googleStatus, GCE instance statuses = ${googleInstances
            .groupBy(_.status)
            .mapValues(_.size)}). Checking again in ${monitorConfig.pollPeriod.toString}. ${msg.getOrElse("")}"
        )
        persistInstances(cluster, googleInstances).as(ScheduleMonitorPass)
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
                                 googleInstances: Set[Instance]): Future[ClusterMonitorMessage] =
    for {
      // Remove credentials from instance metadata.
      // Only happens if an notebook service account was used.
      _ <- if (cluster.status == ClusterStatus.Creating) removeCredentialsFromMetadata(cluster) else Future.unit
      // create or update instances in the DB
      _ <- persistInstances(cluster, googleInstances)
      // update DB after auth futures finish
      now <- IO(Instant.now).unsafeToFuture()
      _ <- dbRef.inTransaction { _.clusterQuery.setToRunning(cluster.id, publicIp, now) }
      // Record metrics in NewRelic
      _ <- recordMetrics(cluster.status, ClusterStatus.Running).unsafeToFuture()
    } yield {
      // Finally pipe a shutdown message to this actor
      logger.info(s"Cluster ${cluster.googleProject}/${cluster.clusterName} is ready for use!")
      ShutdownActor(RemoveFromList(cluster))
    }

  /**
   * Handles a dataproc cluster which has failed. We delete the cluster in Google, and then:
   * - if this is a recoverable error, recreate the cluster
   * - otherwise, just set the status to Error and stop monitoring the cluster
   * @param errorDetails cluster error details from Google
   * @return ShutdownActor
   */
  private def handleFailedCluster(cluster: Cluster,
                                  errorDetails: ClusterErrorDetails,
                                  googleInstances: Set[Instance]): Future[ClusterMonitorMessage] =
    for {
      _ <- Future.sequence(
        List(
          // Delete the cluster in Google
          gdDAO.deleteCluster(cluster.googleProject, cluster.clusterName),
          // Remove the service account key in Google, if present.
          // Only happens if the cluster was NOT created with the pet service account.
          removeServiceAccountKey(cluster),
          // create or update instances in the DB
          persistInstances(cluster, googleInstances),
          //save cluster error in the DB
          persistClusterErrors(errorDetails, cluster)
        )
      )

      // Record metrics in NewRelic
      _ <- recordMetrics(cluster.status, ClusterStatus.Error).unsafeToFuture()

      now <- IO(Instant.now).unsafeToFuture()

      // Decide if we should try recreating the cluster
      res <- if (shouldRecreateCluster(cluster, errorDetails.code, errorDetails.message)) {
        // Update the database record to Deleting, shutdown this actor, and register a callback message
        // to the supervisor telling it to recreate the cluster.
        logger.info(
          s"Cluster ${cluster.projectNameString} is in an error state with $errorDetails. Attempting to recreate..."
        )
        dbRef.inTransaction { dataAccess =>
          dataAccess.clusterQuery.markPendingDeletion(cluster.id, now)
        } map { _ =>
          ShutdownActor(ClusterDeleted(cluster, recreate = true))
        }
      } else {
        // Update the database record to Error and shutdown this actor.
        logger.warn(
          s"Cluster ${cluster.projectNameString} is in an error state with $errorDetails'. Unable to recreate cluster."
        )
        for {
          // update the cluster status to Error
          _ <- dbRef.inTransaction { _.clusterQuery.updateClusterStatus(cluster.id, ClusterStatus.Error, now) }
          _ <- metrics.incrementCounterFuture(s"AsyncClusterCreationFailure/${errorDetails.code}")

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
  )(implicit ev: ApplicativeAsk[IO, TraceId]): Future[ClusterMonitorMessage] = {
    logger.info(s"Cluster ${cluster.projectNameString} has been deleted.")

    for {
      // delete the init bucket so we don't continue to accrue costs after cluster is deleted
      _ <- deleteInitBucket(cluster)

      // set the staging bucket to be deleted in ten days so that logs are still accessible until then
      _ <- setStagingBucketLifecycle(cluster)

      // delete instances in the DB
      _ <- persistInstances(cluster, Set.empty)

      now <- IO(Instant.now).unsafeToFuture()

      _ <- dbRef.inTransaction { dataAccess =>
        dataAccess.clusterQuery.completeDeletion(cluster.id, now)
      }
      _ <- authProvider
        .notifyClusterDeleted(cluster.internalId,
                              cluster.auditInfo.creator,
                              cluster.auditInfo.creator,
                              cluster.googleProject,
                              cluster.clusterName)
        .unsafeToFuture()
      // Remove the Dataproc Worker IAM role for the cluster service account.
      // Only happens if the cluster was created with a service account other
      // than the compute engine default service account.
      _ <- clusterHelper.removeClusterIamRoles(cluster.googleProject, cluster.serviceAccountInfo).unsafeToFuture()

      // Remove member from the Google Group that has the IAM role to pull the Dataproc image
      _ <- clusterHelper
        .updateDataprocImageGroupMembership(cluster.googleProject, createCluster = false)
        .unsafeToFuture()

      // Record metrics in NewRelic
      _ <- recordMetrics(cluster.status, ClusterStatus.Deleted).unsafeToFuture()
    } yield ShutdownActor(RemoveFromList(cluster))
  }

  /**
   * Handles a dataproc cluster which has been stopped.
   * We update the status to Stopped in the database and shut down this actor.
   * @return ShutdownActor
   */
  private def handleStoppedCluster(cluster: Cluster, googleInstances: Set[Instance]): Future[ClusterMonitorMessage] = {
    logger.info(s"Cluster ${cluster.projectNameString} has been stopped.")

    for {
      // create or update instances in the DB
      _ <- persistInstances(cluster, googleInstances)
      now <- IO(Instant.now).unsafeToFuture()
      // this sets the cluster status to stopped and clears the cluster IP
      _ <- dbRef.inTransaction { _.clusterQuery.updateClusterStatus(cluster.id, ClusterStatus.Stopped, now) }
      // reset the time at which the kernel was last found to be busy
      _ <- dbRef.inTransaction { _.clusterQuery.clearKernelFoundBusyDate(cluster.id, now) }
      // Record metrics in NewRelic
      _ <- recordMetrics(cluster.status, ClusterStatus.Stopped).unsafeToFuture()
    } yield ShutdownActor(RemoveFromList(cluster))
  }

  private def createClusterInGoogle(
    cluster: Cluster
  )(implicit ev: ApplicativeAsk[IO, TraceId]): Future[ClusterMonitorMessage] = {
    val createClusterFuture = for {
      _ <- IO(logger.info(s"Attempting to create cluster ${cluster.projectNameString} in Google...")).unsafeToFuture()
      clusterResult <- clusterHelper.createCluster(cluster).unsafeToFuture()
      now <- IO(Instant.now).unsafeToFuture()
      (cluster, initBucket, saKey) = clusterResult
      _ <- dbRef.inTransaction {
        _.clusterQuery.updateAsyncClusterCreationFields(Some(GcsPath(initBucket, GcsObjectName(""))),
                                                        saKey,
                                                        cluster,
                                                        now)
      }
      _ <- IO(
        logger.info(
          s"Cluster ${cluster.projectNameString} was successfully created. Will monitor the creation process."
        )
      ).unsafeToFuture()
    } yield NotReadyCluster(cluster, cluster.status, Set.empty)

    createClusterFuture.recoverWith {
      case e =>
        for {
          _ <- IO(logger.error(s"Failed to create cluster ${cluster.projectNameString} in Google", e)).unsafeToFuture()
          errorMessage = e match {
            case leoEx: LeoException =>
              ErrorReport.loggableString(leoEx.toErrorReport)
            case _ =>
              s"Failed to create cluster ${cluster.projectNameString} due to ${e.toString}"
          }
        } yield FailedCluster(cluster, ClusterErrorDetails(-1, Some(errorMessage)), Set.empty)
    }
  }

  private def checkCluster(implicit ev: ApplicativeAsk[IO, TraceId]): Future[ClusterMonitorMessage] =
    for {
      dbCluster <- getDbCluster

      next <- dbCluster.status match {
        case status if status.isMonitored =>
          if (dbCluster.dataprocInfo.isDefined) {
            checkClusterInGoogle(dbCluster)
          } else {
            createClusterInGoogle(dbCluster)
          }
        case status =>
          IO(logger.info(s"Stopping monitoring of cluster ${dbCluster.projectNameString} in status ${status}"))
            .unsafeToFuture() >>
            Future.successful(ShutdownActor(RemoveFromList(dbCluster)))
      }
    } yield next

  /**
   * Queries Google for the cluster status and takes appropriate action depending on the result.
   * @return ClusterMonitorMessage
   */
  private def checkClusterInGoogle(cluster: Cluster): Future[ClusterMonitorMessage] =
    for {
      googleStatus <- gdDAO.getClusterStatus(cluster.googleProject, cluster.clusterName)

      googleInstances <- getClusterInstances(cluster)

      runningInstanceCount = googleInstances.count(_.status == InstanceStatus.Running)
      stoppedInstanceCount = googleInstances.count(
        i => i.status == InstanceStatus.Stopped || i.status == InstanceStatus.Terminated
      )

      result <- googleStatus match {
        case Unknown | Creating | Updating =>
          Future.successful(NotReadyCluster(cluster, googleStatus, googleInstances))
        // Take care we don't restart a Deleting or Stopping cluster if google hasn't updated their status yet
        case Running
            if cluster.status != Deleting && cluster.status != Stopping && runningInstanceCount == googleInstances.size =>
          getMasterIp(cluster).flatMap {
            case Some(ip) => checkClusterTools(cluster, ip, googleInstances)
            case None =>
              Future.successful(
                NotReadyCluster(cluster, ClusterStatus.Running, googleInstances, Some("Could not retrieve master IP"))
              )
          }
        // Take care we don't fail a Deleting or Stopping cluster if google hasn't updated their status yet
        case Error if cluster.status != Deleting && cluster.status != Stopping =>
          gdDAO.getClusterErrorDetails(cluster.dataprocInfo.map(_.operationName)).map {
            case Some(errorDetails) => FailedCluster(cluster, errorDetails, googleInstances)
            case None =>
              FailedCluster(cluster, ClusterErrorDetails(Code.INTERNAL.value, Some(internalError)), googleInstances)
          }
        // Take care we don't delete a Creating cluster if google hasn't updated their status yet
        case Deleted if cluster.status == Creating =>
          Future.successful(NotReadyCluster(cluster, ClusterStatus.Creating, googleInstances))
        case Deleted =>
          Future.successful(DeletedCluster(cluster))
        // if the cluster only contains stopped instances, it's a stopped cluster
        case _
            if cluster.status != Starting && cluster.status != Deleting && stoppedInstanceCount == googleInstances.size =>
          Future.successful(StoppedCluster(cluster, googleInstances))
        case _ => Future.successful(NotReadyCluster(cluster, googleStatus, googleInstances))
      }
    } yield result

  private def checkClusterTools(cluster: Cluster,
                                ip: IP,
                                googleInstances: Set[Instance]): Future[ClusterMonitorMessage] =
    // Update the Host IP in the database so DNS cache can be properly populated with the first cache miss
    // Otherwise, when a cluster is resumed and transitions from Starting to Running, we get stuck
    // in that state - at least with the way HttpJupyterDAO.isProxyAvailable works
    for {
      now <- IO(Instant.now).unsafeToFuture()
      images <- dbRef.inTransaction { dataAccess =>
        for {
          _ <- dataAccess.clusterQuery.updateClusterHostIp(cluster.id, Some(ip), now)
          images <- dataAccess.clusterImageQuery.getAllForCluster(cluster.id)
        } yield images

      }
      availableTools <- images.toList.traverse { image =>
        image.tool.isProxyAvailable(cluster.googleProject, cluster.clusterName).map(b => (image.tool, b))
      }
    } yield availableTools match {
      case a if a.forall(x => x._2 == true) =>
        ReadyCluster(cluster, ip, googleInstances)
      case a =>
        NotReadyCluster(
          cluster,
          ClusterStatus.Running,
          googleInstances,
          Some(s"Services not available: ${a.collect { case x if x._2 == false => x._1 }}")
        )
    }

  private def persistInstances(cluster: Cluster, googleInstances: Set[Instance]): Future[Unit] = {
    logger.debug(s"Persisting instances for cluster ${cluster.projectNameString}: $googleInstances")
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.mergeInstances(cluster.copy(instances = googleInstances))
    }.void
  }

  private def saveClusterError(cluster: Cluster, errorMessage: String, errorCode: Int): Future[Unit] =
    dbRef
      .inTransaction { dataAccess =>
        val clusterId = dataAccess.clusterQuery.getIdByUniqueKey(cluster)
        clusterId flatMap {
          case Some(a) => dataAccess.clusterErrorQuery.save(a, ClusterError(errorMessage, errorCode, Instant.now))
          case None => {
            logger.warn(
              s"Could not find Id for Cluster ${cluster.projectNameString}  with google cluster ID ${cluster.dataprocInfo
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

  private def persistClusterErrors(errorDetails: ClusterErrorDetails, cluster: Cluster): Future[Unit] = {
    val result = cluster.dataprocInfo.map(_.stagingBucket) match {
      case Some(stagingBucketName) => {
        for {
          metadata <- google2StorageDAO
            .getObjectMetadata(stagingBucketName, GcsBlobName("userscript_output.txt"), None)
            .compile
            .last
            .unsafeToFuture()
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
    result.handleError { e =>
      logger.error(s"Failed to persist cluster errors for cluster ${cluster}: ${e.getMessage}", e)
    }
  }

  private def getClusterInstances(cluster: Cluster): Future[Set[Instance]] =
    for {
      map <- gdDAO.getClusterInstances(cluster.googleProject, cluster.clusterName)
      instances <- map.toList.flatTraverse {
        case (role, instances) =>
          instances.toList.traverseFilter(
            instance => googleComputeDAO.getInstance(instance).map(_.map(_.copy(dataprocRole = Some(role))))
          )
      }
    } yield instances.toSet

  private def getMasterIp(cluster: Cluster): Future[Option[IP]] = {
    val transformed = for {
      masterKey <- OptionT(gdDAO.getClusterMasterInstance(cluster.googleProject, cluster.clusterName))
      masterInstance <- OptionT(googleComputeDAO.getInstance(masterKey))
      masterIp <- OptionT.fromOption[Future](masterInstance.ip)
    } yield masterIp

    transformed.value
  }

  private def removeServiceAccountKey(cluster: Cluster): Future[Unit] =
    // Delete the notebook service account key in Google, if present
    for {
      keyOpt <- dbRef.inTransaction {
        _.clusterQuery.getServiceAccountKeyId(cluster.googleProject, cluster.clusterName)
      }
      _ <- clusterHelper
        .removeServiceAccountKey(cluster.googleProject, cluster.serviceAccountInfo.notebookServiceAccount, keyOpt)
        .unsafeToFuture()
    } yield ()

  private def deleteInitBucket(cluster: Cluster): Future[Unit] =
    // Get the init bucket path for this cluster, then delete the bucket in Google.
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.getInitBucket(cluster.googleProject, cluster.clusterName)
    } flatMap {
      case None =>
        Future.successful(
          logger.warn(s"Could not lookup init bucket for cluster ${cluster.projectNameString}: cluster not in db")
        )
      case Some(bucketPath) =>
        googleStorageDAO.deleteBucket(bucketPath.bucketName, recurse = true) map { _ =>
          logger.debug(s"Deleted init bucket $bucketPath for cluster ${cluster.googleProject}/${cluster.clusterName}")
        }
    }

  private def setStagingBucketLifecycle(cluster: Cluster): Future[Unit] =
    // Get the staging bucket path for this cluster, then set the age for it to be deleted the specified number of days after the deletion of the cluster.
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.getStagingBucket(cluster.googleProject, cluster.clusterName)
    } flatMap {
      case None =>
        Future.successful(
          logger.warn(s"Could not lookup staging bucket for cluster ${cluster.projectNameString}: cluster not in db")
        )
      case Some(bucketPath) =>
        val ageToDelete = cluster.auditInfo.createdDate
          .until(Instant.now(), ChronoUnit.DAYS)
          .toInt + clusterBucketConfig.stagingBucketExpiration.toDays.toInt
        googleStorageDAO.setBucketLifecycle(bucketPath.bucketName, ageToDelete, GcsLifecycleTypes.Delete) map { _ =>
          logger.debug(
            s"Set staging bucket $bucketPath for cluster ${cluster.googleProject}/${cluster.clusterName} to be deleted in ${ageToDelete} days."
          )
        } handleErrorWith { error =>
          IO(
            logger.error(s"Error occurred setting staging bucket lifecycle for cluster ${cluster.projectNameString}",
                         error)
          ).unsafeToFuture()
        }
    }

  private def removeCredentialsFromMetadata(cluster: Cluster): Future[Unit] =
    cluster.serviceAccountInfo.notebookServiceAccount match {
      // No notebook service account: don't remove creds from metadata! We need them.
      case None => Future.successful(())

      // Remove credentials from instance metadata.
      // We want to ensure that _only_ the notebook service account is used;
      // users should not be able to yank the cluster SA credentials from the metadata server.
      case Some(_) =>
        // TODO https://github.com/DataBiosphere/leonardo/issues/128
        Future.successful(())
    }

  private def getDbCluster: Future[Cluster] =
    for {
      clusterOpt <- dbRef.inTransaction { _.clusterQuery.getClusterById(clusterId) }
      cluster <- clusterOpt.fold(
        Future.failed[Cluster](new Exception(s"Cluster with id ${clusterId} not found in the database"))
      )(Future.successful)
    } yield cluster

  private def recordMetrics(origStatus: ClusterStatus, finalStatus: ClusterStatus): IO[Unit] =
    for {
      endTime <- IO(System.currentTimeMillis)
      baseName = s"ClusterMonitor/${origStatus}->${finalStatus}"
      counterName = s"${baseName}/count"
      timerName = s"${baseName}/timer"
      duration = (endTime - startTime).millis
      _ <- metrics.incrementCounter(counterName)
      _ <- metrics.recordResponseTime(timerName, duration)
    } yield ()
}
