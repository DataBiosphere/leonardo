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
      startClusterMonitorActor(cluster)

    case ClusterDeleted(cluster, recreate) =>
      startClusterMonitorActor(cluster, recreate)

    case RecreateCluster(cluster) =>
      if (monitorConfig.canRecreateCluster) {
        logger.info(s"Recreating cluster ${cluster.googleProject}/${cluster.clusterName}...")
        val clusterRequest = ClusterRequest(cluster.googleBucket, cluster.googleServiceAccount, cluster.labels)
        val clusterCreatedFuture = for {
          clusterResponse <- gdDAO.createCluster(cluster.googleProject, cluster.clusterName, clusterRequest)
          newCluster <- Future.successful(Cluster(clusterRequest, clusterResponse))
          _ <- dbRef.inTransaction { dataAccess =>
            // Append a random suffix to the old cluster name to prevent unique key conflicts in the database.
            // This is a bit ugly; a better solution would be to have a unique yet on (googleId, clusterName, deletedAt)
            dataAccess.clusterQuery.updateClusterName(cluster.googleId, appendRandomSuffix(cluster.clusterName)) andThen
              dataAccess.clusterQuery.save(newCluster)
          }
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
