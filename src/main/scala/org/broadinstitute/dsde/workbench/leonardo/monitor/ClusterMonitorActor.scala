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

  private def getGoogleCluster: Future[ClusterMonitorMessage] = {
    gdDAO.getCluster(cluster.googleProject, cluster.clusterName).flatMap { c =>
      val googleClusterStatus = getGoogleClusterStatus(c)
      googleClusterStatus match {
        case Unknown | Creating | Updating => Future.successful(NotReadyCluster(googleClusterStatus))
        case Running => processRunningCluster(c)
        case Error => processFailedCluster()
        case Deleted => Future.successful(ClusterDeleted)
      }
    }
  }

  private def getGoogleClusterStatus(cluster: GoogleCluster): GoogleClusterStatus = {
    val statusOpt = Option(cluster.getStatus).map(_.getState).flatMap(GoogleClusterStatusEnum.fromStringIgnoreCase)

    statusOpt.getOrElse(throw new IllegalStateException(s"Unknown google status: ${cluster.getStatus}"))
  }

  private def getMasterInstanceName(cluster: GoogleCluster): String = {
    val masterInstanceOpt = for {
      config <- Option(cluster.getConfig)
      masterConfig <- Option(config.getMasterConfig)
      instanceNames <- Option(masterConfig.getInstanceNames)
      masterInstance <- instanceNames.asScala.headOption
    } yield masterInstance

    masterInstanceOpt.getOrElse(throw new IllegalStateException(s"Could not get master node from cluster ${cluster.getClusterName}"))
  }

  private def getInstanceIP(instance: Instance): String = {
    val ipOpt = for {
      interfaces <- Option(instance.getNetworkInterfaces)
      interface <- interfaces.asScala.headOption
      accessConfigs <- Option(interface.getAccessConfigs)
      accessConfig <- accessConfigs.asScala.headOption
    } yield accessConfig.getNatIP

    ipOpt.getOrElse(throw new IllegalStateException(s"Could not get IP from instance ${instance.getName}"))
  }



  private def processRunningCluster(cluster: GoogleCluster): Future[ReadyCluster] = {
    gdDAO.getInstance(cluster.getProjectId, getMasterInstanceName(cluster)).map { instance =>
      ReadyCluster(getInstanceIP(instance))
    }
  }

  private def processFailedCluster(): Future[FailedCluster] = {
    gdDAO.getOperation(cluster.operationName).map { op =>
      val googleClusterErrorOpt = for {
        done <- Option(op.getDone) if done == true
        error <- Option(op.getError)
      } yield GoogleClusterError(error.getCode, error.getMessage)

      FailedCluster(googleClusterErrorOpt)
    }
  }


}
