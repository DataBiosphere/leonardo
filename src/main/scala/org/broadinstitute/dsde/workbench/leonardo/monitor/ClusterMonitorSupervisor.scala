package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.leonardo.config.{DataprocConfig, MonitorConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.DataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterRequest, LeoAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor._
import org.broadinstitute.dsde.workbench.leonardo.service.LeonardoService

object ClusterMonitorSupervisor {
  def props(monitorConfig: MonitorConfig, dataprocConfig: DataprocConfig, gdDAO: DataprocDAO, googleIamDAO: GoogleIamDAO, dbRef: DbReference, clusterDnsCache: ActorRef, authProvider: LeoAuthProvider): Props =
    Props(new ClusterMonitorSupervisor(monitorConfig, dataprocConfig, gdDAO, googleIamDAO, dbRef, clusterDnsCache, authProvider))

  sealed trait ClusterSupervisorMessage
  case class RegisterLeoService(service: LeonardoService) extends ClusterSupervisorMessage
  case class ClusterCreated(cluster: Cluster) extends ClusterSupervisorMessage
  case class ClusterDeleted(cluster: Cluster, recreate: Boolean = false) extends ClusterSupervisorMessage
  case class RecreateCluster(cluster: Cluster) extends ClusterSupervisorMessage
}

class ClusterMonitorSupervisor(monitorConfig: MonitorConfig, dataprocConfig: DataprocConfig, gdDAO: DataprocDAO, googleIamDAO: GoogleIamDAO, dbRef: DbReference, clusterDnsCache: ActorRef, authProvider: LeoAuthProvider) extends Actor with LazyLogging {
  import context.dispatcher

  var leoService: LeonardoService = _

  override def receive: Receive = {
    case RegisterLeoService(service) =>
      leoService = service

    case ClusterCreated(cluster) =>
      logger.info(s"Monitoring cluster ${cluster.projectNameString} for initialization.")
      startClusterMonitorActor(cluster)

    case ClusterDeleted(cluster, recreate) =>
      logger.info(s"Monitoring cluster ${cluster.projectNameString} for deletion.")
      startClusterMonitorActor(cluster, recreate)

    case RecreateCluster(cluster) =>
      if (monitorConfig.recreateCluster) {
        logger.info(s"Recreating cluster ${cluster.projectNameString}...")
        val clusterRequest = ClusterRequest(
          cluster.labels,
          cluster.jupyterExtensionUri,
          cluster.jupyterUserScriptUri,
          Some(cluster.machineConfig))
        leoService.internalCreateCluster(cluster.creator, cluster.serviceAccountInfo, cluster.googleProject, cluster.clusterName, clusterRequest).failed.foreach { e =>
          logger.error("Error occurred recreating cluster", e)
        }
      } else {
        logger.warn(s"Received RecreateCluster message for cluster ${cluster.projectNameString} but cluster recreation is disabled.")
      }
  }

  def createChildActor(cluster: Cluster): ActorRef = {
    context.actorOf(ClusterMonitorActor.props(cluster, monitorConfig, dataprocConfig, gdDAO, googleIamDAO, dbRef, clusterDnsCache, authProvider))
  }

  def startClusterMonitorActor(cluster: Cluster, recreate: Boolean = false): Unit = {
    val child = createChildActor(cluster)

    if (recreate && monitorConfig.recreateCluster) {
      context.watchWith(child, RecreateCluster(cluster))
    }
  }

  override val supervisorStrategy = {
    // TODO add threshold monitoring stuff from Rawls
    // for now always restart the child actor in case of failure
    OneForOneStrategy(maxNrOfRetries = monitorConfig.maxRetries) {
      case _ => Restart
    }
  }
}
