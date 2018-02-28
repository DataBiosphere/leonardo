package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.time.Instant

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import cats.data.OptionT
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.grpc.Status.Code
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.leonardo.config.{DataprocConfig, MonitorConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache.{ClusterReady, GetClusterResponse, ProcessReadyCluster}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus._
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterStatus, IP, _}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorActor._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.ClusterDeleted
import org.broadinstitute.dsde.workbench.leonardo.service.ClusterNotReadyException
import org.broadinstitute.dsde.workbench.util.addJitter

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import akka.pattern.ask
import akka.util.Timeout
import org.broadinstitute.dsde.workbench.leonardo.service.ClusterNotReadyException
import slick.dbio.DBIOAction

object ClusterMonitorActor {
  /**
    * Creates a Props object used for creating a {{{ClusterMonitorActor}}}.
    */
  def props(cluster: Cluster, monitorConfig: MonitorConfig, dataprocConfig: DataprocConfig, gdDAO: GoogleDataprocDAO, googleIamDAO: GoogleIamDAO, googleStorageDAO: GoogleStorageDAO, dbRef: DbReference, clusterDnsCache: ActorRef, authProvider: LeoAuthProvider): Props =
    Props(new ClusterMonitorActor(cluster, monitorConfig, dataprocConfig, gdDAO, googleIamDAO, googleStorageDAO, dbRef, clusterDnsCache, authProvider))

  // ClusterMonitorActor messages:

  private[monitor] sealed trait ClusterMonitorMessage extends Product with Serializable
  private[monitor] case object ScheduleMonitorPass extends ClusterMonitorMessage
  private[monitor] case object QueryForCluster extends ClusterMonitorMessage
  private[monitor] case class ReadyCluster(publicIP: IP) extends ClusterMonitorMessage
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
                          val dataprocConfig: DataprocConfig,
                          val gdDAO: GoogleDataprocDAO,
                          val googleIamDAO: GoogleIamDAO,
                          val googleStorageDAO: GoogleStorageDAO,
                          val dbRef: DbReference,
                          val clusterDnsCache: ActorRef,
                          val authProvider: LeoAuthProvider) extends Actor with LazyLogging {
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
      logger.error(s"Error occurred monitoring cluster ${cluster.projectNameString}", e)
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
    logger.info(s"Cluster ${cluster.projectNameString} is not ready yet ($status). Checking again in ${monitorConfig.pollPeriod.toString}.")
    Future.successful(ScheduleMonitorPass)
  }

  /**
    * Handles a dataproc cluster which is ready. We update the status and IP in the database,
    * then shut down this actor.
    * @param publicIp the cluster public IP, according to Google
    * @return ShutdownActor
    */
  private def handleReadyCluster(publicIp: IP): Future[ClusterMonitorMessage] = {
    for {
      // Delete the init bucket
      _ <- deleteInitBucket
      // Remove credentials from instance metadata.
      // Only happens if an notebook service account was used.
      _ <- removeCredentialsFromMetadata
      // Ensure the cluster is ready for proxying but updating the IP -> DNS cache
      _ <- ensureClusterReadyForProxying(publicIp)
      // update DB after auth futures finish
      _ <- dbRef.inTransaction { dataAccess =>
        dataAccess.clusterQuery.setToRunning(cluster.googleId, publicIp)
      }
      // Remove the Dataproc Worker IAM role for the cluster service account.
      // Only happens if the cluster was created with a service account other
      // than the compute engine default service account.
      _ <- removeIamRolesForUser
    } yield {
      // Finally pipe a shutdown message to this actor
      logger.info(s"Cluster ${cluster.googleProject}/${cluster.clusterName} is ready for use!")
      ShutdownActor()
    }
  }

  /**
    * Handles a dataproc cluster which has failed. We delete the cluster in Google, and then:
    * - if this is a recoverable error, recreate the cluster
    * - otherwise, just set the status to Error and stop monitoring the cluster
    * @param errorDetails cluster error details from Google
    * @return ShutdownActor
    */
  private def handleFailedCluster(errorDetails: ClusterErrorDetails): Future[ClusterMonitorMessage] = {

      dbRef.inTransaction { dataAccess =>
        val clusterId = dataAccess.clusterQuery.getIdByGoogleId(cluster.googleId)
        clusterId flatMap {
          case Some(a) => dataAccess.clusterErrorQuery.save(a, ClusterError(errorDetails.message.getOrElse("Error not available"), errorDetails.code, Instant.now))
          case None => {
            logger.warn(s"Could not find Id for Cluster ${cluster.projectNameString}  with google cluster ID ${cluster.googleId}.")
            DBIOAction.successful(0)
          }
        }
      }

    // Decide if we should try recreating the cluster
    if (shouldRecreateCluster(errorDetails.code, errorDetails.message)) {
        // Update the database record to Deleting, shutdown this actor, and register a callback message
        // to the supervisor telling it to recreate the cluster.
        logger.info(s"Cluster ${cluster.projectNameString} is in an error state with $errorDetails. Attempting to recreate...")
        dbRef.inTransaction { dataAccess =>
          dataAccess.clusterQuery.markPendingDeletion(cluster.googleId)
        } flatMap  {_ =>
          gdDAO.deleteCluster(cluster.googleProject, cluster.clusterName)
          removeServiceAccountKey
        } map { _ =>
          ShutdownActor(Some(ClusterDeleted(cluster, recreate = true)))
        }
    } else {
        // Update the database record to Error and shutdown this actor.
        logger.warn(s"Cluster ${cluster.projectNameString} is in an error state with $errorDetails'. Unable to recreate cluster.")
        for {
          _ <- dbRef.inTransaction { _.clusterQuery.updateClusterStatus(cluster.googleId, ClusterStatus.Error) }
          _ <- gdDAO.deleteCluster(cluster.googleProject, cluster.clusterName)
          _ <- removeServiceAccountKey
          // Remove the Dataproc Worker IAM role for the pet service account
          // Only happens if the cluster was created with the pet service account.
          _ <-  removeIamRolesForUser
        } yield ShutdownActor()
      }

  }

  private def shouldRecreateCluster(code: Int, message: Option[String]): Boolean = {
    // TODO: potentially add more checks here as we learn which errors are recoverable
    monitorConfig.recreateCluster && (code == Code.UNKNOWN.value)
  }

  /**
    * Handles a dataproc cluster which has been deleted.
    * We update the status to Deleted in the database, notify the auth provider,
    * and shut down this actor.
    * @return error or ShutdownActor
    */
  private def handleDeletedCluster(): Future[ClusterMonitorMessage] = {
    logger.info(s"Cluster ${cluster.projectNameString} has been deleted.")

    for {
      _ <- dbRef.inTransaction { dataAccess =>
        dataAccess.clusterQuery.completeDeletion(cluster.googleId, cluster.clusterName)
      }
      _ <- authProvider.notifyClusterDeleted(cluster.creator, cluster.creator, cluster.googleProject, cluster.clusterName)
    } yield ShutdownActor()
  }

  /**
    * Queries Google for the cluster status and takes appropriate action depending on the result.
    * @return ClusterMonitorMessage
    */
  private def checkCluster: Future[ClusterMonitorMessage] = {
    for {
      googleStatus <- gdDAO.getClusterStatus(cluster.googleProject, cluster.clusterName)
      result <- googleStatus match {
        case Unknown | Creating | Updating =>
          Future.successful(NotReadyCluster(googleStatus))
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
  }

  private def removeIamRolesForUser: Future[Unit] = {
    // Remove the Dataproc Worker IAM role for the cluster service account
    cluster.serviceAccountInfo.clusterServiceAccount match {
      case None => Future.successful(())
      case Some(serviceAccountEmail) =>
        // Only remove the Dataproc Worker role if there are no other clusters with the same owner
        // in the DB with CREATING status. This prevents situations where we prematurely yank pet SA
        // roles when the same user is creating multiple clusters.
        dbRef.inTransaction { dataAccess =>
          dataAccess.clusterQuery.countByClusterServiceAccountAndStatus(serviceAccountEmail, ClusterStatus.Creating)
        } flatMap { count =>
          if (count > 0) {
            Future.successful(())
          } else {
            googleIamDAO.removeIamRolesForUser(cluster.googleProject, serviceAccountEmail, Set("roles/dataproc.worker"))
          }
        }
    }
  }

  private def removeServiceAccountKey: Future[Unit] = {
    // Delete the notebook service account key in Google, if present
    val tea = for {
      key <- OptionT(dbRef.inTransaction { _.clusterQuery.getServiceAccountKeyId(cluster.googleProject, cluster.clusterName) })
      serviceAccountEmail <- OptionT.fromOption[Future](cluster.serviceAccountInfo.notebookServiceAccount)
      _ <- OptionT.liftF(googleIamDAO.removeServiceAccountKey(dataprocConfig.leoGoogleProject, serviceAccountEmail, key))
    } yield ()

    tea.value.void
  }

  private def deleteInitBucket: Future[Unit] = {
    // Get the init bucket path for this cluster, then delete the bucket in Google.
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.getInitBucket(cluster.googleProject, cluster.clusterName)
    } flatMap {
      case None => Future.successful( logger.warn(s"Could not lookup bucket for cluster ${cluster.projectNameString}: cluster not in db") )
      case Some(bucketPath) =>
        googleStorageDAO.deleteBucket(bucketPath.bucketName, recurse = true) map { _ =>
          logger.debug(s"Deleted init bucket $bucketPath for cluster ${cluster.googleProject}/${cluster.clusterName}")
        }
    }
  }

  private def ensureClusterReadyForProxying(ip: IP): Future[Unit] = {
    // Ensure if the cluster's IP has been picked up by the DNS cache and is ready for proxying.
    implicit val timeout: Timeout = Timeout(5 seconds)
    (clusterDnsCache ? ProcessReadyCluster(cluster.copy(hostIp = Some(ip))))
      .mapTo[Either[Throwable, GetClusterResponse]]
      .map {
        case Left(throwable) => throw throwable
        case Right(ClusterReady(_)) => ()
        case Right(_) => throw ClusterNotReadyException(cluster.googleProject, cluster.clusterName)
      }
  }

  private def removeCredentialsFromMetadata: Future[Unit] = {
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
  }
}
