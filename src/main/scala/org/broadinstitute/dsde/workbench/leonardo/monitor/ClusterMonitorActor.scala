package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.{Actor, Props}
import akka.pattern.pipe
import cats.data._
import cats.implicits._
import com.google.api.services.compute.model.Instance
import com.google.api.services.dataproc.model.{Operation, Cluster => GoogleCluster}
import com.typesafe.scalalogging.LazyLogging
import io.grpc.Status.Code
import org.broadinstitute.dsde.workbench.leonardo.dao.DataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterStatus._
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterStatus, LeoException}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorActor._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.ClusterDeleted

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure

object ClusterMonitorActor {
  def props(cluster: Cluster, gdDAO: DataprocDAO, dbRef: DbReference): Props =
    Props(new ClusterMonitorActor(cluster, gdDAO, dbRef))

  sealed trait ClusterMonitorMessage extends Product with Serializable
  case class ScheduleMonitorPass() extends ClusterMonitorMessage
  case class StartMonitorPass() extends ClusterMonitorMessage
  case class ReadyCluster(publicIP: String) extends ClusterMonitorMessage
  case class NotReadyCluster(status: ClusterStatus) extends ClusterMonitorMessage
  case class FailedCluster(code: Int, message: Option[String]) extends ClusterMonitorMessage
  case class DeletedCluster() extends ClusterMonitorMessage
  case class ShutdownActor(notifyParentMsg: Option[Any] = None) extends ClusterMonitorMessage

  private[monitor] case class ClusterMonitorException(errMsg: String) extends LeoException(errMsg)

}

class ClusterMonitorActor(val cluster: Cluster,
                          val gdDAO: DataprocDAO,
                          val dbRef: DbReference) extends Actor with LazyLogging {
  import context._

  override def preStart(): Unit = {
    scheduleMonitorPass
    super.preStart()
  }

  override def receive: Receive = {
    case Right(ScheduleMonitorPass) =>
      scheduleMonitorPass

    case Right(StartMonitorPass) =>
      getGoogleCluster.value pipeTo self

    case Right(NotReadyCluster(status)) =>
      handleNotReadyCluster(status).value pipeTo self

    case Right(ReadyCluster(ip)) =>
      handleReadyCluster(ip).value pipeTo self

    case Right(FailedCluster(code, message)) =>
      handleFailedCluster(code, message).value pipeTo self

    case Right(DeletedCluster) =>
      handleDeletedCluster().value pipeTo self

    case Right(ShutdownActor(notifyParentMsg)) =>
      notifyParentMsg.foreach(msg => parent ! msg)
      stop(self)

    case Left(exception: Throwable) =>
      logger.error("Error occurred in ClusterMonitorActor", exception)
      throw exception

    case Failure(exception) =>
      logger.error("Error occurred in ClusterMonitorActor", exception)
      throw exception
  }

  def scheduleMonitorPass: Unit = {
    context.system.scheduler.scheduleOnce(1 minute, self, Right(StartMonitorPass))
  }

  private def handleNotReadyCluster(status: ClusterStatus): EitherT[Future, Throwable, ScheduleMonitorPass] = {
    for {
      _ <- dbRef.inTransaction(_.clusterQuery.updateClusterStatus(cluster.googleId, status)).attemptT
    } yield ScheduleMonitorPass()
  }

  private def handleReadyCluster(publicIp: String): EitherT[Future, Throwable, ShutdownActor] = {
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.updateClusterStatus(cluster.googleId, ClusterStatus.Running) andThen
        dataAccess.clusterQuery.updateIpByGoogleId(cluster.googleId, publicIp)
    }.map(_ => ShutdownActor()).attemptT
  }

  private def handleFailedCluster(code: Int, message: Option[String]): EitherT[Future, Throwable, ShutdownActor] = {
    dbRef.inTransaction(_.clusterQuery.updateClusterStatus(cluster.googleId, ClusterStatus.Error)).attemptT.flatMap { _ =>
      val future = if (shouldRecreateCluster(code, message)) {
        gdDAO.deleteCluster(cluster.googleProject, cluster.clusterName).map { _ =>
          ShutdownActor(Some(ClusterDeleted(cluster, true)))
        }
      } else {
        Future.successful(ShutdownActor())
      }
      future.attemptT
    }
  }

  private def shouldRecreateCluster(code: Int, message: Option[String]): Boolean = {
    code == Code.UNKNOWN.value
  }

  private def handleDeletedCluster(): EitherT[Future, Throwable, ShutdownActor] = {
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.updateClusterStatus(cluster.googleId, ClusterStatus.Deleted)
    }.map(_ => ShutdownActor()).attemptT
  }

  private def getGoogleCluster: EitherT[Future, Throwable, ClusterMonitorMessage] = {
    for {
      googleCluster <- gdDAO.getCluster(cluster.googleProject, cluster.clusterName).attemptT
      status <- getGoogleClusterStatus(googleCluster)
      result <- status match {
        case Unknown | Creating | Updating => Future.successful(NotReadyCluster(status)).attemptT
        case Running => processRunningCluster(googleCluster)
        case Error => processFailedCluster()
        case Deleted => Future.successful(DeletedCluster()).attemptT
      }
    } yield result
  }

  private def getGoogleClusterStatus(cluster: GoogleCluster): EitherT[Future, Throwable, ClusterStatus] = {
    val errorOrGoogleStatus = for {
      status <- Option(cluster.getStatus)      .toRight(ClusterMonitorException("Cluster status is null"))
      state <-  Option(status.getState)        .toRight(ClusterMonitorException("Cluster state is null"))
      googleStatus <- withNameIgnoreCase(state).toRight(ClusterMonitorException(s"Unknown Google cluster status: $state"))
    } yield googleStatus

    EitherT(Future.successful(errorOrGoogleStatus))
  }

  private def getMasterInstanceName(cluster: GoogleCluster): EitherT[Future, Throwable, String] = {
    val errorOrMasterInstanceName = for {
      config <- Option(cluster.getConfig)                   .toRight(ClusterMonitorException("Cluster config is null"))
      masterConfig <- Option(config.getMasterConfig)        .toRight(ClusterMonitorException("Cluster master config is null"))
      instanceNames <- Option(masterConfig.getInstanceNames).toRight(ClusterMonitorException("Master config instance names is null"))
      masterInstance <- instanceNames.asScala.headOption    .toRight(ClusterMonitorException("Master instance not found"))
    } yield masterInstance

    EitherT(Future.successful(errorOrMasterInstanceName))
  }

  private def getInstanceIP(instance: Instance): EitherT[Future, Throwable, String] = {
    val errorOrIp = for {
      interfaces <- Option(instance.getNetworkInterfaces).toRight(ClusterMonitorException("Network interfaces is null"))
      interface <- interfaces.asScala.headOption         .toRight(ClusterMonitorException("Network interface not found"))
      accessConfigs <- Option(interface.getAccessConfigs).toRight(ClusterMonitorException("Access configs is null"))
      accessConfig <- accessConfigs.asScala.headOption   .toRight(ClusterMonitorException("Access config not found"))
    } yield accessConfig.getNatIP

    EitherT(Future.successful(errorOrIp))
  }

  private def processRunningCluster(cluster: GoogleCluster): EitherT[Future, Throwable, ReadyCluster] = {
    for {
      masterInstanceName <- getMasterInstanceName(cluster)
      instance <- gdDAO.getInstance(cluster.getProjectId, masterInstanceName).attemptT
      ip <- getInstanceIP(instance)
    } yield ReadyCluster(ip)
  }

  private def getErrorCodeAndMessage(operation: Operation): EitherT[Future, Throwable, (Int, String)] = {
    val errorOrCodeAndMessage = for {
      _ <- Option(operation.getDone).filter(_ == true).toRight(ClusterMonitorException("Operation is not done"))
      error <- Option(operation.getError).toRight(ClusterMonitorException("Operation error not found"))
      code <- Option(error.getCode)      .toRight(ClusterMonitorException("Operation code is null"))
    } yield (code.toInt, error.getMessage)

    EitherT(Future.successful(errorOrCodeAndMessage))
  }


  private def processFailedCluster(): EitherT[Future, Throwable, FailedCluster] = {
    for {
      op <- gdDAO.getOperation(cluster.operationName).attemptT
      codeAndMessage <- getErrorCodeAndMessage(op)
    } yield FailedCluster(codeAndMessage._1, Option(codeAndMessage._2))
  }
}
