package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.{Actor, Props}
import akka.pattern.pipe
import com.typesafe.scalalogging.LazyLogging
import io.grpc.Status.Code
import org.broadinstitute.dsde.workbench.leonardo.config.MonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.{CallToGoogleApiFailedException, DataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterStatus._
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterErrorDetails, ClusterStatus}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorActor._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.ClusterDeleted
import org.broadinstitute.dsde.workbench.util.addJitter

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure

object ClusterMonitorActor {
  /**
    * Creates a Props object used for creating a {{{ClusterMonitorActor}}}.
    */
  def props(cluster: Cluster, monitorConfig: MonitorConfig, gdDAO: DataprocDAO, dbRef: DbReference): Props =
    Props(new ClusterMonitorActor(cluster, monitorConfig, gdDAO, dbRef))

  // ClusterMonitorActor messages:

  private[monitor] sealed trait ClusterMonitorMessage extends Product with Serializable
  private[monitor] case object ScheduleMonitorPass extends ClusterMonitorMessage
  private[monitor] case object QueryForCluster extends ClusterMonitorMessage
  private[monitor] case class ReadyCluster(publicIP: String) extends ClusterMonitorMessage
  private[monitor] case class NotReadyCluster(status: ClusterStatus) extends ClusterMonitorMessage
  private[monitor] case class FailedCluster(errorDetails: ClusterErrorDetails) extends ClusterMonitorMessage
  private[monitor] case object DeletedCluster extends ClusterMonitorMessage
  private[monitor] case class ShutdownActor(notifyParentMsg: Option[Any] = None) extends ClusterMonitorMessage
}

/**
  * An actor which monitors the status of a Cluster. Periodically queries Google for the cluster status,
  * and acts appropriately for Running, Deleted, and Failed clusters.
  * @param cluster the Cluster to monitor
  * @param monitorConfig monitor configuration properties
  * @param gdDAO the Google dataproc DAO
  * @param dbRef the DB reference
  */
class ClusterMonitorActor(val cluster: Cluster,
                          val monitorConfig: MonitorConfig,
                          val gdDAO: DataprocDAO,
                          val dbRef: DbReference) extends Actor with LazyLogging {
  import context._

  override def preStart(): Unit = {
    super.preStart()
    scheduleInitialMonitorPass
  }

  override def receive: Receive = {
    case ScheduleMonitorPass =>
      scheduleNextMonitorPass

    case QueryForCluster =>
      checkCluster pipeTo self

    case NotReadyCluster(status) =>
      handleNotReadyCluster(status) pipeTo self

    case ReadyCluster(ip) =>
      handleReadyCluster(ip) pipeTo self

    case FailedCluster(errorDetails) =>
      handleFailedCluster(errorDetails) pipeTo self

    case DeletedCluster =>
      handleDeletedCluster pipeTo self

    case ShutdownActor(notifyParentMsg) =>
      notifyParentMsg.foreach(msg => parent ! msg)
      stop(self)

    case Failure(e) =>
      // An error occurred, let the supervisor handle it
      logger.error(s"Error occurred monitoring cluster ${cluster.googleProject}/${cluster.clusterName}", e)
      throw e
  }

  private def scheduleInitialMonitorPass: Unit = {
    // Wait anything _up to_ the poll interval for a much wider distribution of cluster monitor start times when Leo starts up
    system.scheduler.scheduleOnce(addJitter(0 seconds, monitorConfig.pollPeriod), self, QueryForCluster)
  }

  private def scheduleNextMonitorPass: Unit = {
    system.scheduler.scheduleOnce(addJitter(monitorConfig.pollPeriod), self, QueryForCluster)
  }

  /**
    * Handles a dataproc cluster which is not ready yet. We don't take any action, just
    * schedule another monitor pass.
    * @param status the ClusterStatus from Google
    * @return ScheduleMonitorPass
    */
  private def handleNotReadyCluster(status: ClusterStatus): Future[ClusterMonitorMessage] = {
    logger.info(s"Cluster ${cluster.googleProject}/${cluster.clusterName} is not ready yet ($status). Checking again in ${monitorConfig.pollPeriod.toString}.")
    Future.successful(ScheduleMonitorPass)
  }

  /**
    * Handles a dataproc cluster which is ready. We update the status and IP in the database,
    * then shut down this actor.
    * @param publicIp the cluster public IP, according to Google
    * @return ShutdownActor
    */
  private def handleReadyCluster(publicIp: String): Future[ClusterMonitorMessage] = {
    logger.info(s"Cluster ${cluster.googleProject}/${cluster.clusterName} is ready for use!")
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.setToRunning(cluster.googleId, publicIp)
    }.map(_ => ShutdownActor())
  }

  /**
    * Handles a dataproc cluster which has failed. We delete the cluster in Google, and then:
    * - if this is a recoverable error, recreate the cluster
    * - otherwise, just set the status to Error and stop monitoring the cluster
    * @param errorDetails cluster error details from Google
    * @return ShutdownActor
    */
  private def handleFailedCluster(errorDetails: ClusterErrorDetails): Future[ClusterMonitorMessage] = {
    // Delete the cluster in Google
    gdDAO.deleteCluster(cluster.googleProject, cluster.clusterName).flatMap { _ =>
      if (shouldRecreateCluster(errorDetails.code, errorDetails.message)) {
        // Update the database record to Deleting, shutdown this actor, and register a callback message
        // to the supervisor telling it to recreate the cluster.
        logger.info(s"Cluster ${cluster.googleProject}/${cluster.clusterName} is in an error state with $errorDetails. Attempting to recreate...")
        dbRef.inTransaction { dataAccess =>
          dataAccess.clusterQuery.markPendingDeletion(cluster.googleId)
        } map { _ =>
          ShutdownActor(Some(ClusterDeleted(cluster, recreate = true)))
        }
      } else {
        // Update the database record to Error and shutdown this actor.
        logger.warn(s"Cluster ${cluster.googleProject}/${cluster.clusterName} is in an error state with $errorDetails'. Unable to recreate cluster.")
        dbRef.inTransaction { dataAccess =>
          dataAccess.clusterQuery.updateClusterStatus(cluster.googleId, ClusterStatus.Error)
        } map { _ =>
          ShutdownActor()
        }
      }
    }
  }

  private def shouldRecreateCluster(code: Int, message: Option[String]): Boolean = {
    // TODO: potentially add more checks here as we learn which errors are recoverable
    monitorConfig.recreateCluster && (code == Code.UNKNOWN.value)
  }

  /**
    * Handles a dataproc cluster which has been deleted. We update the status to Deleted in the database,
    * and shut down this actor.
    * @return error or ShutdownActor
    */
  private def handleDeletedCluster(): Future[ClusterMonitorMessage] = {
    logger.info(s"Cluster ${cluster.googleProject}/${cluster.clusterName} has been deleted.")
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.completeDeletion(cluster.googleId, cluster.clusterName)
    }.map(_ => ShutdownActor())
  }

  /**
    * Queries Google for the cluster status and takes appropriate action depending on the result.
    * @return ClusterMonitorMessage
    */
  private def checkCluster: Future[ClusterMonitorMessage] = {
    val result = for {
      googleStatus <- gdDAO.getClusterStatus(cluster.googleProject, cluster.clusterName)
      result <- googleStatus match {
        case Unknown | Creating | Updating => Future.successful(NotReadyCluster(googleStatus))
        // Take care we don't restart a Deleting cluster if google hasn't updated their status yet
        case Running if cluster.status != Deleting =>
          gdDAO.getClusterMasterInstanceIp(cluster.googleProject, cluster.clusterName).map {
            case Some(ip) => ReadyCluster(ip)
            case None => NotReadyCluster(ClusterStatus.Running)
          }
        case Error =>
          gdDAO.getClusterErrorDetails(cluster.operationName).map {
            case Some(errorDetails) => FailedCluster(errorDetails)
            case None => NotReadyCluster(ClusterStatus.Error)
          }
        case Deleted => Future.successful(DeletedCluster)
        case _ => Future.successful(NotReadyCluster(googleStatus))
      }
    } yield result

    // Recover from Google 404 errors, and assume the cluster is deleted
    result.recover {
      case CallToGoogleApiFailedException(_, _, 404, _) => DeletedCluster
    }
  }
}
