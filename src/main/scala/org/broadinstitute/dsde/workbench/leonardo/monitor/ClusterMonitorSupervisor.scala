package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, OneForOneStrategy, Props}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.dao.DataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterRequest}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor._

import scala.util.{Failure, Success}

object ClusterMonitorSupervisor {
  def props(gdDAO: DataprocDAO, dbRef: DbReference): Props =
    Props(new ClusterMonitorSupervisor(gdDAO, dbRef))

  sealed trait ClusterSupervisorMessage
  case class ClusterCreated(cluster: Cluster) extends ClusterSupervisorMessage
  case class ClusterDeleted(cluster: Cluster, recreate: Boolean = false) extends ClusterSupervisorMessage
  case class RecreateCluster(cluster: Cluster) extends ClusterSupervisorMessage
}

class ClusterMonitorSupervisor(gdDAO: DataprocDAO, dbRef: DbReference) extends Actor with LazyLogging {
  import context.dispatcher

  override def receive: Receive = {
    case ClusterCreated(cluster) =>
      startClusterMonitorActor(cluster)

    case ClusterDeleted(cluster, recreate) =>
      startClusterMonitorActor(cluster, recreate)

    case RecreateCluster(cluster) =>
      val clusterRequest = ClusterRequest(cluster.googleBucket, cluster.googleServiceAccount, cluster.labels)
      gdDAO.createCluster(cluster.googleProject, cluster.clusterName, clusterRequest).map { clusterResponse =>
        ClusterCreated(Cluster(clusterRequest, clusterResponse))
      }.onComplete {
        case Success(c) => self ! c
        case Failure(e) =>
          logger.error(s"Error recreating cluster ${cluster.googleProject}/${cluster.clusterName}", e)
      }
  }

  def startClusterMonitorActor(cluster: Cluster, recreate: Boolean = false): Unit = {
    val child = context.actorOf(ClusterMonitorActor.props(cluster, gdDAO, dbRef))

    if (recreate) {
      context.watchWith(child, RecreateCluster(cluster))
    }
  }

  override val supervisorStrategy = {
    // TODO add threshold monitoring stuff
    // for now always restart
    OneForOneStrategy() {
      case _ => Restart
    }
  }
}
