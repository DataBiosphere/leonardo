package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.{Actor, Props}
import akka.pattern.pipe
import cats._
import cats.implicits._
import cats.data._
import com.google.api.services.compute.model.Instance
import com.google.api.services.dataproc.model.{Cluster => GoogleCluster, Operation}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.dao.DataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterStatus.Value
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorActor._

import scala.concurrent.Future
import scala.collection.JavaConverters._

object ClusterMonitorActor {
  def props(cluster: Cluster, gdDAO: DataprocDAO, dbRef: DbReference): Props =
    Props(new ClusterMonitorActor(cluster, gdDAO, dbRef))

  sealed trait ClusterMonitorMessage extends Product with Serializable
  case object StartMonitorPass extends ClusterMonitorMessage
  case class ReadyCluster(publicIP: String) extends ClusterMonitorMessage
  case class NotReadyCluster(status: GoogleClusterStatusEnum.GoogleClusterStatus) extends ClusterMonitorMessage
  case class FailedCluster(error: Option[GoogleClusterError]) extends ClusterMonitorMessage
  case object ClusterDeleted extends ClusterMonitorMessage


  private object GoogleClusterStatusEnum extends Enumeration {
    type GoogleClusterStatus = Value
    val Unknown, Creating, Running, Error, Deleted, Updating = Value

    def fromStringIgnoreCase(str: String): Option[GoogleClusterStatus] = {
      values.find(_.toString.equalsIgnoreCase(str))
    }
  }

  private case class GoogleClusterError(code: Int, message: String)

}

class ClusterMonitorActor(val cluster: Cluster,
                          val gdDAO: DataprocDAO,
                          val dbRef: DbReference) extends Actor with LazyLogging {

  import context.dispatcher
  import GoogleClusterStatusEnum._

  override def receive: Receive = {
    case StartMonitorPass => getGoogleCluster pipeTo self
  }

  private def getGoogleCluster: EitherT[Future, String, ClusterMonitorMessage] = {
    for {
      googleCluster <- gdDAO.getCluster(cluster.googleProject, cluster.clusterName).attemptT
      status <- getGoogleClusterStatus(googleCluster)
      result <- status match {
        case Unknown | Creating | Updating => EitherT.right(Future.successful(NotReadyCluster(status)))
        case Running => processRunningCluster(googleCluster)
        case Error => processFailedCluster()
        case Deleted => EitherT.right(Future.successful(ClusterDeleted))
      }
    } yield result
  }

  private def getGoogleClusterStatus(cluster: GoogleCluster): EitherT[Future, String, GoogleClusterStatus] = {
    val errorOrGoogleStatus = for {
      status <- Option(cluster.getStatus).toRight("Cluster status is null")
      state <- Option(status.getState).toRight("Cluster state is null")
      googleStatus <- GoogleClusterStatusEnum.fromStringIgnoreCase(state).toRight(s"Unknown Google cluster status: $state")
    } yield googleStatus

    EitherT(Future.successful(errorOrGoogleStatus))
  }

  private def getMasterInstanceName(cluster: GoogleCluster): EitherT[Future, String, String] = {
    val errorOrMasterInstanceName = for {
      config <- Option(cluster.getConfig).toRight("Cluster config is null")
      masterConfig <- Option(config.getMasterConfig).toRight("Cluster master config is null")
      instanceNames <- Option(masterConfig.getInstanceNames).toRight("Master config instance names is null")
      masterInstance <- instanceNames.asScala.headOption.toRight("Master instance not found")
    } yield masterInstance

    EitherT(Future.successful(errorOrMasterInstanceName))
  }

  private def getInstanceIP(instance: Instance): EitherT[Future, String, String] = {
    val errorOrIp = for {
      interfaces <- Option(instance.getNetworkInterfaces).toRight("Network interfaces is null")
      interface <- interfaces.asScala.headOption.toRight("Network interface not found")
      accessConfigs <- Option(interface.getAccessConfigs).toRight("Access configs is null")
      accessConfig <- accessConfigs.asScala.headOption.toRight("Access config not found")
    } yield accessConfig.getNatIP

    EitherT(Future.successful(errorOrIp))
  }



  private def processRunningCluster(cluster: GoogleCluster): EitherT[Future, String, ReadyCluster] = {
    for {
      masterInstanceName <- getMasterInstanceName(cluster)
      instance <- EitherT.right(gdDAO.getInstance(cluster.getProjectId, masterInstanceName))
      ip <- getInstanceIP(instance)
    } yield ReadyCluster(ip)
  }

  private def processFailedCluster(): EitherT[Future, String, FailedCluster] = {
    EitherT.right(gdDAO.getOperation(cluster.operationName).map { op =>
      val googleClusterErrorOpt = for {
        done <- Option(op.getDone) if done == true
        error <- Option(op.getError)
      } yield GoogleClusterError(error.getCode, error.getMessage)

      FailedCluster(googleClusterErrorOpt)
    })
  }


}
