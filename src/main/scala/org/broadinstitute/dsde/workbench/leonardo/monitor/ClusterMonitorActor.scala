package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.time.Instant

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import cats.data.OptionT
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.grpc.Status.Code
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.leonardo.config.{DataprocConfig, MonitorConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.JupyterDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache.{HostReady, HostStatus}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus._
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterStatus, IP, _}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorActor._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.ClusterDeleted
import org.broadinstitute.dsde.workbench.leonardo.service.ClusterNotReadyException
import org.broadinstitute.dsde.workbench.util.addJitter
import slick.dbio.DBIOAction

import scala.collection.immutable.Set
import scala.concurrent.Future
import scala.concurrent.duration._

object ClusterMonitorActor {
  /**
    * Creates a Props object used for creating a {{{ClusterMonitorActor}}}.
    */
  def props(cluster: Cluster, monitorConfig: MonitorConfig, dataprocConfig: DataprocConfig, gdDAO: GoogleDataprocDAO, googleComputeDAO: GoogleComputeDAO, googleIamDAO: GoogleIamDAO, googleStorageDAO: GoogleStorageDAO, dbRef: DbReference, authProvider: LeoAuthProvider, jupyterProxyDAO: JupyterDAO): Props =
    Props(new ClusterMonitorActor(cluster, monitorConfig, dataprocConfig, gdDAO, googleComputeDAO, googleIamDAO, googleStorageDAO, dbRef, authProvider, jupyterProxyDAO))

  // ClusterMonitorActor messages:

  private[monitor] sealed trait ClusterMonitorMessage extends Product with Serializable
  private[monitor] case object ScheduleMonitorPass extends ClusterMonitorMessage
  private[monitor] case object QueryForCluster extends ClusterMonitorMessage
  private[monitor] case class ReadyCluster(publicIP: IP, instances: Set[Instance]) extends ClusterMonitorMessage
  private[monitor] case class NotReadyCluster(status: ClusterStatus, instances: Set[Instance]) extends ClusterMonitorMessage
  private[monitor] case class FailedCluster(errorDetails: ClusterErrorDetails, instances: Set[Instance]) extends ClusterMonitorMessage
  private[monitor] case object DeletedCluster extends ClusterMonitorMessage
  private[monitor] case class StoppedCluster(instances: Set[Instance]) extends ClusterMonitorMessage
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
                          val googleComputeDAO: GoogleComputeDAO,
                          val googleIamDAO: GoogleIamDAO,
                          val googleStorageDAO: GoogleStorageDAO,
                          val dbRef: DbReference,
                          val authProvider: LeoAuthProvider,
                          val jupyterProxyDAO: JupyterDAO) extends Actor with LazyLogging {
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

    case NotReadyCluster(status, instances) =>
      handleNotReadyCluster(status, instances) pipeTo self

    case ReadyCluster(ip, instances) =>
      handleReadyCluster(ip, instances) pipeTo self

    case FailedCluster(errorDetails, instances) =>
      handleFailedCluster(errorDetails, instances) pipeTo self

    case DeletedCluster =>
      handleDeletedCluster pipeTo self

    case StoppedCluster(instances) =>
      handleStoppedCluster(instances) pipeTo self

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
  private def handleNotReadyCluster(status: ClusterStatus, instances: Set[Instance]): Future[ClusterMonitorMessage] = {
    logger.info(s"Cluster ${cluster.projectNameString} is not ready yet (cluster status = $status, instance statuses = ${instances.groupBy(_.status).mapValues(_.size)}). Checking again in ${monitorConfig.pollPeriod.toString}.")
    persistInstances(instances).map { _ =>
      ScheduleMonitorPass
    }
  }

  /**
    * Handles a dataproc cluster which is ready. We update the status and IP in the database,
    * then shut down this actor.
    * @param publicIp the cluster public IP, according to Google
    * @return ShutdownActor
    */
  private def handleReadyCluster(publicIp: IP, instances: Set[Instance]): Future[ClusterMonitorMessage] = {
    for {
      clusterStatus <- getDbClusterStatus
      // Remove credentials from instance metadata.
      // Only happens if an notebook service account was used.
      _ <- if (clusterStatus == ClusterStatus.Creating) removeCredentialsFromMetadata else Future.successful(())
      // create or update instances in the DB
      _ <- persistInstances(instances)
      // update DB after auth futures finish
      _ <- dbRef.inTransaction { _.clusterQuery.setToRunning(cluster.id, publicIp) }

      // TODO Double-check we don't need the following two lines anymore
// nsureClusterReadyForProxying(publicIp, clusterStatus)

      // Remove the Dataproc Worker IAM role for the cluster service account.
      // Only happens if the cluster was created with a service account other
      // than the compute engine default service account.
      _ <- if (clusterStatus == ClusterStatus.Creating) removeIamRolesForUser else Future.successful(())
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
  private def handleFailedCluster(errorDetails: ClusterErrorDetails, instances: Set[Instance]): Future[ClusterMonitorMessage] = {
    val deleteFuture = Future.sequence(Seq(
      // Delete the cluster in Google
      gdDAO.deleteCluster(cluster.googleProject, cluster.clusterName),
      // Remove the service account key in Google, if present.
      // Only happens if the cluster was NOT created with the pet service account.
      removeServiceAccountKey,
      // create or update instances in the DB
      persistInstances(instances),
      // save cluster errors to the DB
      dbRef.inTransaction { dataAccess =>
        val clusterId = dataAccess.clusterQuery.getIdByUniqueKey(cluster)
        clusterId flatMap {
          case Some(a) => dataAccess.clusterErrorQuery.save(a, ClusterError(errorDetails.message.getOrElse("Error not available"), errorDetails.code, Instant.now))
          case None => {
            logger.warn(s"Could not find Id for Cluster ${cluster.projectNameString}  with google cluster ID ${cluster.dataprocInfo.googleId}.")
            DBIOAction.successful(0)
          }
        }
      }
    ))

    deleteFuture.flatMap { _ =>
      // Decide if we should try recreating the cluster
      if (shouldRecreateCluster(errorDetails.code, errorDetails.message)) {
        // Update the database record to Deleting, shutdown this actor, and register a callback message
        // to the supervisor telling it to recreate the cluster.
        logger.info(s"Cluster ${cluster.projectNameString} is in an error state with $errorDetails. Attempting to recreate...")
        dbRef.inTransaction { dataAccess =>
          dataAccess.clusterQuery.markPendingDeletion(cluster.id)
        } map { _ =>
          ShutdownActor(Some(ClusterDeleted(cluster, recreate = true)))
        }
      } else {
        // Update the database record to Error and shutdown this actor.
        logger.warn(s"Cluster ${cluster.projectNameString} is in an error state with $errorDetails'. Unable to recreate cluster.")
        for {
          // update the cluster status to Error
          _ <- dbRef.inTransaction { _.clusterQuery.updateClusterStatus(cluster.id, ClusterStatus.Error) }
          // Remove the Dataproc Worker IAM role for the pet service account
          // Only happens if the cluster was created with the pet service account.
          _ <-  removeIamRolesForUser
        } yield ShutdownActor()
      }
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
  private def handleDeletedCluster: Future[ClusterMonitorMessage] = {
    logger.info(s"Cluster ${cluster.projectNameString} has been deleted.")

    for {
      // delete instances in the DB
      _ <- persistInstances(Set.empty)

      _ <- dbRef.inTransaction { dataAccess =>
        dataAccess.clusterQuery.completeDeletion(cluster.id)
      }
      _ <- authProvider.notifyClusterDeleted(cluster.auditInfo.creator, cluster.auditInfo.creator, cluster.googleProject, cluster.clusterName)
    } yield ShutdownActor()
  }

  /**
    * Handles a dataproc cluster which has been stopped.
    * We update the status to Stopped in the database and shut down this actor.
    * @return ShutdownActor
    */
  private def handleStoppedCluster(instances: Set[Instance]): Future[ClusterMonitorMessage] = {
    logger.info(s"Cluster ${cluster.projectNameString} has been stopped.")

    for {
      // create or update instances in the DB
      _ <- persistInstances(instances)
      // this sets the cluster status to stopped and clears the cluster IP
      _ <- dbRef.inTransaction { _.clusterQuery.updateClusterStatus(cluster.id, ClusterStatus.Stopped) }
    } yield ShutdownActor()
  }

  private def checkCluster: Future[ClusterMonitorMessage] = {
    getDbClusterStatus.flatMap {
      case status if status.isMonitored => checkClusterInGoogle(status)
      case status =>
        logger.info(s"Stopping monitoring of cluster ${cluster.projectNameString} in status ${status}")
        Future.successful(ShutdownActor())
    }
  }

  /**
    * Queries Google for the cluster status and takes appropriate action depending on the result.
    * @return ClusterMonitorMessage
    */
  private def checkClusterInGoogle(leoClusterStatus: ClusterStatus): Future[ClusterMonitorMessage] = {
    for {
      googleStatus <- gdDAO.getClusterStatus(cluster.googleProject, cluster.clusterName)

      googleInstances <- getClusterInstances

      runningInstanceCount = googleInstances.count(_.status == InstanceStatus.Running)
      stoppedInstanceCount = googleInstances.count(i => i.status == InstanceStatus.Stopped || i.status == InstanceStatus.Terminated)

      result <- googleStatus match {
        case Unknown | Creating | Updating =>
          Future.successful(NotReadyCluster(googleStatus, googleInstances))
        // Take care we don't restart a Deleting or Stopping cluster if google hasn't updated their status yet
        case Running if leoClusterStatus != Deleting && leoClusterStatus != Stopping && leoClusterStatus != Starting && runningInstanceCount == googleInstances.size =>
          getMasterIp.map {
            case Some(ip) => ReadyCluster(ip, googleInstances)
            case None => NotReadyCluster(ClusterStatus.Running, googleInstances)
          }
        case Running if leoClusterStatus == Starting && runningInstanceCount == googleInstances.size =>
          getMasterIp.flatMap {
            case Some(ip) =>
              dbRef.inTransaction { dataAccess => dataAccess.clusterQuery.updateClusterHostIp(cluster.id, Some(ip)) }.flatMap { _ =>
                isProxyAvailable(leoClusterStatus, ip).map {
                  case true => ReadyCluster(ip, googleInstances)
                  case false => NotReadyCluster(ClusterStatus.Running, googleInstances)
                }
              }
            case None => Future.successful(NotReadyCluster(ClusterStatus.Running, googleInstances))
          }
        // Take care we don't fail a Deleting or Stopping cluster if google hasn't updated their status yet
        case Error if leoClusterStatus != Deleting && leoClusterStatus != Stopping =>
          gdDAO.getClusterErrorDetails(cluster.dataprocInfo.operationName).map {
            case Some(errorDetails) => FailedCluster(errorDetails, googleInstances)
            case None => NotReadyCluster(ClusterStatus.Error, googleInstances)
          }
        case Deleted => Future.successful(DeletedCluster)
        // if the cluster only contains stopped instances, it's a stopped cluster
        case _ if leoClusterStatus != Starting && leoClusterStatus != Deleting && stoppedInstanceCount == googleInstances.size =>
          Future.successful(StoppedCluster(googleInstances))
        case _ => Future.successful(NotReadyCluster(googleStatus, googleInstances))
      }
    } yield result
  }

  private def isProxyAvailable(clusterStatus: ClusterStatus, ip: IP): Future[Boolean] = {
    for {
      proxyAvailable <- jupyterProxyDAO.getStatus(cluster.googleProject, cluster.clusterName)
    } yield {
      proxyAvailable
    }
  }

  private def persistInstances(instances: Set[Instance]): Future[Unit] = {
    logger.debug(s"Persisting instances for cluster ${cluster.projectNameString}: ${instances}")
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.mergeInstances(cluster.copy(instances = instances))
    }.void
  }

  private def getClusterInstances: Future[Set[Instance]] = {
    for {
      map <- gdDAO.getClusterInstances(cluster.googleProject, cluster.clusterName)
      instances <- Future.traverse(map) { case (role, instances) =>
        Future.traverse(instances) { instance =>
          googleComputeDAO.getInstance(instance).map(_.map(_.copy(dataprocRole = Some(role))))
        }
      }
    } yield instances.flatten.flatten.toSet
  }

  private def getMasterIp: Future[Option[IP]] = {
    val transformed = for {
      masterKey <- OptionT(gdDAO.getClusterMasterInstance(cluster.googleProject, cluster.clusterName))
      masterInstance <- OptionT(googleComputeDAO.getInstance(masterKey))
      masterIp <- OptionT.fromOption[Future](masterInstance.ip)
    } yield masterIp

    transformed.value
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

  private def getDbClusterStatus: Future[ClusterStatus] = {
    dbRef.inTransaction { _.clusterQuery.getClusterStatus(cluster.id) } map { _.getOrElse(ClusterStatus.Unknown) }
  }
}
