package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.config.MonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.DataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterRequest}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor._

import scala.concurrent.Future
import scala.util.{Failure, Random, Success}

object ClusterMonitorSupervisor {
  def props(monitorConfig: MonitorConfig, gdDAO: DataprocDAO, dbRef: DbReference): Props =
    Props(new ClusterMonitorSupervisor(monitorConfig, gdDAO, dbRef))

  sealed trait ClusterSupervisorMessage
  case class ClusterCreated(cluster: Cluster) extends ClusterSupervisorMessage
  case class ClusterDeleted(cluster: Cluster, recreate: Boolean = false) extends ClusterSupervisorMessage
  case class RecreateCluster(cluster: Cluster) extends ClusterSupervisorMessage
}

class ClusterMonitorSupervisor(monitorConfig: MonitorConfig, gdDAO: DataprocDAO, dbRef: DbReference) extends Actor with LazyLogging {
  import context.dispatcher

  override def receive: Receive = {
    case ClusterCreated(cluster) =>
      logger.info(s"Monitoring cluster ${cluster.googleProject}/${cluster.clusterName} for initialization")
      startClusterMonitorActor(cluster)

    case ClusterDeleted(cluster, recreate) =>
      logger.info(s"Monitoring cluster ${cluster.googleProject}/${cluster.clusterName} for deletion")
      startClusterMonitorActor(cluster, recreate)

    case RecreateCluster(cluster) =>
      if (monitorConfig.canRecreateCluster) {
        logger.info(s"Recreating cluster ${cluster.googleProject}/${cluster.clusterName}...")
        val clusterRequest = ClusterRequest(cluster.googleBucket, cluster.googleServiceAccount, cluster.labels)
        val clusterCreatedFuture = for {
          clusterResponse <- gdDAO.createCluster(cluster.googleProject, cluster.clusterName, clusterRequest)
          newCluster <- Future.successful(Cluster(clusterRequest, clusterResponse))
          _ <- dbRef.inTransaction { _.clusterQuery.save(newCluster) }
        } yield ClusterCreated(newCluster)

        clusterCreatedFuture.onComplete {
          case Success(c) => self ! c
          case Failure(e) =>
            logger.error(s"Error recreating cluster ${cluster.googleProject}/${cluster.clusterName}", e)
        }
      } else {
        logger.warn(s"Received RecreateCluster message for cluster ${cluster} but cluster recreation is disabled.")
      }
  }

  def createChildActor(cluster: Cluster): ActorRef = {
    context.actorOf(ClusterMonitorActor.props(cluster, monitorConfig, gdDAO, dbRef))
  }

  def startClusterMonitorActor(cluster: Cluster, recreate: Boolean = false): Unit = {
    val child = createChildActor(cluster)

    if (recreate && monitorConfig.canRecreateCluster) {
      context.watchWith(child, RecreateCluster(cluster))
    }
  }

  override val supervisorStrategy = {
    // TODO add threshold monitoring stuff
    // for now always restart
    OneForOneStrategy(maxNrOfRetries = monitorConfig.maxRetries) {
      case _ => Restart
    }
  }

  private def appendRandomSuffix(str: String): String = {
    s"${str}_${Random.alphanumeric.take(6).mkString}"
  }
}
