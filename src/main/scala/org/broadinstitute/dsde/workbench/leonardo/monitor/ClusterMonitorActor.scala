package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.{Actor, Props}
import akka.pattern.pipe
import cats.data._
import cats.implicits._
import com.google.api.services.compute.model.Instance
import com.google.api.services.dataproc.model.{Operation, Cluster => GoogleCluster}
import com.typesafe.scalalogging.LazyLogging
import io.grpc.Status.Code
import org.broadinstitute.dsde.workbench.leonardo.config.MonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.{CallToGoogleApiFailedException, DataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterStatus._
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterStatus, LeoException}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorActor._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.ClusterDeleted

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Failure

object ClusterMonitorActor {
  /**
    * Creates a Props object used for creating a {{{ClusterMonitorActor}}}.
    */
  def props(cluster: Cluster, monitorConfig: MonitorConfig, gdDAO: DataprocDAO, dbRef: DbReference): Props =
    Props(new ClusterMonitorActor(cluster, monitorConfig, gdDAO, dbRef))

  // ClusterMonitorActor messages:

  private[monitor] sealed trait ClusterMonitorMessage extends Product with Serializable
  private[monitor] case class ScheduleMonitorPass() extends ClusterMonitorMessage
  private[monitor] case class QueryForCluster() extends ClusterMonitorMessage
  private[monitor] case class ReadyCluster(publicIP: String) extends ClusterMonitorMessage
  private[monitor] case class NotReadyCluster(status: ClusterStatus) extends ClusterMonitorMessage
  private[monitor] case class FailedCluster(code: Int, message: Option[String]) extends ClusterMonitorMessage
  private[monitor] case class DeletedCluster() extends ClusterMonitorMessage
  private[monitor] case class ShutdownActor(notifyParentMsg: Option[Any] = None) extends ClusterMonitorMessage

  /** Used for tracking internal exceptions in ClusterMonitorActor. This exception is not returned to users. */
  private[monitor] case class ClusterMonitorException(errMsg: String) extends LeoException(errMsg)
}

/**
  * An actor which monitors the status of a Cluster. Periodically queries Google for the cluster status,
  * and acts appropriately for Running, Deleted, and Failed clusters.
  * @param cluster the Cluster to monitor
  * @param gdDAO the Google dataproc DAO
  * @param dbRef the DB reference
  */
class ClusterMonitorActor(val cluster: Cluster,
                          val monitorConfig: MonitorConfig,
                          val gdDAO: DataprocDAO,
                          val dbRef: DbReference) extends Actor with LazyLogging {
  import context._

  override def preStart(): Unit = {
    scheduleMonitorPass
    super.preStart()
  }

  override def receive: Receive = {
    case Right(msg) =>
      logger.debug(s"Received message $msg")

      msg match {
        case ScheduleMonitorPass() => scheduleMonitorPass
        case QueryForCluster() => getGoogleCluster.value pipeTo self
        case NotReadyCluster(status) => handleNotReadyCluster(status).value pipeTo self
        case ReadyCluster(ip) => handleReadyCluster(ip).value pipeTo self
        case FailedCluster(code, message) => handleFailedCluster(code, message).value pipeTo self
        case DeletedCluster() => handleDeletedCluster.value pipeTo self
        case ShutdownActor(notifyParentMsg) =>
          notifyParentMsg.foreach(msg => parent ! msg)
          stop(self)
      }

    case Left(exception: Throwable) =>
      // An error occurred, let the supervisor handle & log it
      throw exception

    case Failure(exception) =>
      // Shouldn't really happen as all Futures are lifted into EitherT.
      // But take the same action anyway.
      throw exception
  }

  private def scheduleMonitorPass: Unit = {
    system.scheduler.scheduleOnce(monitorConfig.pollPeriod, self, Right(QueryForCluster()))
  }

  /**
    * Handles a dataproc cluster which is not ready yet. We don't take any action, just
    * schedule another monitor pass.
    * @param status the Google status
    * @return error or ScheduleMonitorPass
    */
  private def handleNotReadyCluster(status: ClusterStatus): EitherT[Future, Throwable, ScheduleMonitorPass] = {
    Future.successful(ScheduleMonitorPass()).attemptT
  }

  /**
    * Handles a dataproc cluster which is ready. We update the status and IP in the database,
    * then shut down this actor.
    * @param publicIp the cluster public IP, according to Google
    * @return error or ShutdownActor
    */
  private def handleReadyCluster(publicIp: String): EitherT[Future, Throwable, ShutdownActor] = {
    logger.info(s"Cluster ${cluster.googleProject}/${cluster.clusterName} is ready for use!")
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.updateClusterStatus(cluster.googleId, ClusterStatus.Running) andThen
        dataAccess.clusterQuery.updateIpByGoogleId(cluster.googleId, publicIp)
    }.map(_ => ShutdownActor()).attemptT
  }

  /**
    * Handles a dataproc cluster which has failed. We update the status to Error in the database, and
    * shut down this actor. Depending on the status code from Google, we might try to delete
    * and recreate the cluster.
    * @param code Google error code
    * @param message Google error message
    * @return error or ShutdownActor
    */
  private def handleFailedCluster(code: Int, message: Option[String]): EitherT[Future, Throwable, ShutdownActor] = {
    if (shouldRecreateCluster(code, message)) {
      // Delete the cluster in Google and update the database record to Deleting.
      // Then shutdown this actor, but register a callback message to the supervisor
      // telling it to recreate the cluster.
      logger.info(s"Cluster ${cluster.googleProject}/${cluster.clusterName} is in an error state with code $code and message $message. Attempting to delete and recreate...")
      for {
        operation <- gdDAO.deleteCluster(cluster.googleProject, cluster.clusterName).attemptT
        _ <- dbRef.inTransaction { dataAccess =>
          dataAccess.clusterQuery.deleteCluster(cluster.googleId) andThen
            dataAccess.clusterQuery.updateClusterOperation(cluster.googleId, operation.map(_.getName))
        }.attemptT
      } yield ShutdownActor(Some(ClusterDeleted(cluster, recreate = true)))
    } else {
      // Update the database record to Error and shutdown this actor.
      logger.warn(s"Cluster ${cluster.googleProject}/${cluster.clusterName} is in an error state with code $code and message $message'. Unable to recreate cluster.")
      dbRef.inTransaction(_.clusterQuery.updateClusterStatus(cluster.googleId, ClusterStatus.Error)).map { _ =>
        ShutdownActor()
      }.attemptT
    }
  }

  private def shouldRecreateCluster(code: Int, message: Option[String]): Boolean = {
    monitorConfig.canRecreateCluster && (code == Code.UNKNOWN.value)
  }

  /**
    * Handles a dataproc cluster which has been deleted. We update the status to Deleted in the database,
    * and shut down this actor.
    * @return error or ShutdownActor
    */
  private def handleDeletedCluster(): EitherT[Future, Throwable, ShutdownActor] = {
    logger.info(s"Cluster ${cluster.googleProject}/${cluster.clusterName} has been deleted.")
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.updateClusterStatus(cluster.googleId, ClusterStatus.Deleted)
    }.map(_ => ShutdownActor()).attemptT
  }

  /**
    * Queries Google for a dataproc cluster and takes appropriate action depending on the status.
    * @return error or ClusterMonitorMessage
    */
  private def getGoogleCluster: EitherT[Future, Throwable, ClusterMonitorMessage] = {
    val errorOrMessage: EitherT[Future, Throwable, ClusterMonitorMessage] = for {
      googleCluster <- gdDAO.getCluster(cluster.googleProject, cluster.clusterName).attemptT
      status <- getGoogleClusterStatus(googleCluster)
      result <- status match {
        case Unknown | Creating | Updating => Future.successful(NotReadyCluster(status)).attemptT
        case Running => processRunningCluster(googleCluster)
        case Error => processFailedCluster()
        case Deleted => Future.successful(DeletedCluster()).attemptT
      }
    } yield result

    // Recover from Google 404 errors, and assume the cluster is deleted
    errorOrMessage.recover {
      case CallToGoogleApiFailedException(_, _, 404, _) => DeletedCluster()
    }
  }

  /**
    * Processes a running Google dataproc cluster, with error handling.
    * @param cluster Google dataproc cluster
    * @return error or ReadyCluster
    */
  private def processRunningCluster(cluster: GoogleCluster): EitherT[Future, Throwable, ReadyCluster] = {
    for {
      masterInstanceName <- getMasterInstanceName(cluster)
      zone <- getMasterInstanceZone(cluster)
      instance <- gdDAO.getInstance(cluster.getProjectId, zone, masterInstanceName).attemptT
      ip <- getInstanceIP(instance)
    } yield ReadyCluster(ip)
  }

  /**
    * Process a failed Google dataproc cluster, with error handling.
    * @return error or FailedCluster
    */
  private def processFailedCluster(): EitherT[Future, Throwable, FailedCluster] = {
    for {
      op <- gdDAO.getOperation(cluster.operationName).attemptT
      codeAndMessage <- getErrorCodeAndMessage(op)
    } yield FailedCluster(codeAndMessage._1, codeAndMessage._2)
  }

  /**
    * Gets the Leo ClusterStatus from a dataproc cluster, with error handling.
    * @param cluster the Google dataproc cluster
    * @return error or ClusterStatus
    */
  private def getGoogleClusterStatus(cluster: GoogleCluster): EitherT[Future, Throwable, ClusterStatus] = {
    val errorOrGoogleStatus = for {
      status <- Option(cluster.getStatus)      .toRight(ClusterMonitorException("Cluster status is null"))
      state <-  Option(status.getState)        .toRight(ClusterMonitorException("Cluster state is null"))
      googleStatus <- withNameIgnoreCase(state).toRight(ClusterMonitorException(s"Unknown Google cluster status: $state"))
    } yield googleStatus

    EitherT(Future.successful(errorOrGoogleStatus))
  }

  /**
    * Gets the master instance name from a dataproc cluster, with error handling.
    * @param cluster the Google dataproc cluster
    * @return error or master instance name
    */
  private def getMasterInstanceName(cluster: GoogleCluster): EitherT[Future, Throwable, String] = {
    val errorOrMasterInstanceName = for {
      config <- Option(cluster.getConfig)                   .toRight(ClusterMonitorException("Cluster config is null"))
      masterConfig <- Option(config.getMasterConfig)        .toRight(ClusterMonitorException("Cluster master config is null"))
      instanceNames <- Option(masterConfig.getInstanceNames).toRight(ClusterMonitorException("Master config instance names is null"))
      masterInstance <- instanceNames.asScala.headOption    .toRight(ClusterMonitorException("Master instance not found"))
    } yield masterInstance

    EitherT(Future.successful(errorOrMasterInstanceName))
  }

  /**
    * Gets the master instance zone (not to be confused with region), with error handling.
    * @param cluster the Google dataproc cluster
    * @return error or the master instance zone
    */
  private def getMasterInstanceZone(cluster: GoogleCluster): EitherT[Future, Throwable, String] = {
    def parseZone(zoneUri: String): String = {
      zoneUri.lastIndexOf('/') match {
        case -1 => zoneUri
        case n => zoneUri.substring(n + 1)
      }
    }

    val errorOrMasterInstanceZone = for {
      config <- Option(cluster.getConfig)            .toRight(ClusterMonitorException("Cluster config is null"))
      gceConfig <- Option(config.getGceClusterConfig).toRight(ClusterMonitorException("Cluster GCE config is null"))
      zoneUri <- Option(gceConfig.getZoneUri)           .toRight(ClusterMonitorException("Cluster zone is null"))
    } yield parseZone(zoneUri)

    EitherT(Future.successful(errorOrMasterInstanceZone))
  }

  /**
    * Gets the public IP from a google Instance, with error handling.
    * @param instance the Google instance
    * @return error or public IP, as a String
    */
  private def getInstanceIP(instance: Instance): EitherT[Future, Throwable, String] = {
    val errorOrIp = for {
      interfaces <- Option(instance.getNetworkInterfaces).toRight(ClusterMonitorException("Network interfaces is null"))
      interface <- interfaces.asScala.headOption         .toRight(ClusterMonitorException("Network interface not found"))
      accessConfigs <- Option(interface.getAccessConfigs).toRight(ClusterMonitorException("Access configs is null"))
      accessConfig <- accessConfigs.asScala.headOption   .toRight(ClusterMonitorException("Access config not found"))
    } yield accessConfig.getNatIP

    EitherT(Future.successful(errorOrIp))
  }

  /**
    * Gets the error code and message from a Google Operation, with error handling.
    * @param operation the Operation to query
    * @return error or (code, message)
    */
  private def getErrorCodeAndMessage(operation: Operation): EitherT[Future, Throwable, (Int, Option[String])] = {
    val errorOrCodeAndMessage = for {
      _ <- Option(operation.getDone).filter(_ == true).toRight(ClusterMonitorException("Operation is not done"))
      error <- Option(operation.getError).toRight(ClusterMonitorException("Operation error not found"))
      code <- Option(error.getCode)      .toRight(ClusterMonitorException("Operation code is null"))
    } yield (code.toInt, Option(error.getMessage))

    EitherT(Future.successful(errorOrCodeAndMessage))
  }
}
