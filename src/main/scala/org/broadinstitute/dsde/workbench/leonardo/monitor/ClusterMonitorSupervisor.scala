package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.leonardo.config.{AutoFreezeConfig, DataprocConfig, MonitorConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.JupyterDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterRequest, LeoAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor._
import org.broadinstitute.dsde.workbench.leonardo.service.LeonardoService
import org.broadinstitute.dsde.workbench.model.WorkbenchException

import scala.concurrent.Future

object ClusterMonitorSupervisor {
  def props(monitorConfig: MonitorConfig, dataprocConfig: DataprocConfig, gdDAO: GoogleDataprocDAO, googleComputeDAO: GoogleComputeDAO, googleIamDAO: GoogleIamDAO, googleStorageDAO: GoogleStorageDAO, dbRef: DbReference, authProvider: LeoAuthProvider, autoFreezeConfig: AutoFreezeConfig, jupyterProxyDAO: JupyterDAO): Props =
    Props(new ClusterMonitorSupervisor(monitorConfig, dataprocConfig, gdDAO, googleComputeDAO, googleIamDAO, googleStorageDAO, dbRef, authProvider, autoFreezeConfig, jupyterProxyDAO))

  sealed trait ClusterSupervisorMessage
  case class RegisterLeoService(service: LeonardoService) extends ClusterSupervisorMessage

  // sent after a cluster is created by the user
  case class ClusterCreated(cluster: Cluster, stopAfterCreate: Boolean = false) extends ClusterSupervisorMessage
  // sent after a cluster is deleted by the user
  case class ClusterDeleted(cluster: Cluster, recreate: Boolean = false) extends ClusterSupervisorMessage
  // sent after a cluster is stopped by the user
  case class ClusterStopped(cluster: Cluster) extends ClusterSupervisorMessage
  // sent after a cluster is started by the user
  case class ClusterStarted(cluster: Cluster) extends ClusterSupervisorMessage

  // sent after cluster creation fails, and the cluster should be recreated
  case class RecreateCluster(cluster: Cluster) extends ClusterSupervisorMessage
  // sent after cluster creation succeeds, and the cluster should be stopped
  case class StopClusterAfterCreation(cluster: Cluster) extends ClusterSupervisorMessage
  // Auto freeze idle clusters
  case object AutoFreezeClusters extends ClusterSupervisorMessage
}

class ClusterMonitorSupervisor(monitorConfig: MonitorConfig, dataprocConfig: DataprocConfig, gdDAO: GoogleDataprocDAO, googleComputeDAO: GoogleComputeDAO, googleIamDAO: GoogleIamDAO, googleStorageDAO: GoogleStorageDAO, dbRef: DbReference, authProvider: LeoAuthProvider, autoFreezeConfig: AutoFreezeConfig, jupyterProxyDAO: JupyterDAO) extends Actor with LazyLogging {
  import context.dispatcher

  var leoService: LeonardoService = _

  import context._

  override def preStart(): Unit = {
    super.preStart()
    if(autoFreezeConfig.enableAutoFreeze)
      system.scheduler.schedule(autoFreezeConfig.autoFreezeCheckScheduler, autoFreezeConfig.autoFreezeCheckScheduler, self, AutoFreezeClusters)
  }
  override def receive: Receive = {
    case RegisterLeoService(service) =>
      leoService = service

    case ClusterCreated(cluster, stopAfterCreate) =>
      logger.info(s"Monitoring cluster ${cluster.projectNameString} for initialization.")
      startClusterMonitorActor(cluster, if (stopAfterCreate) Some(StopClusterAfterCreation(cluster)) else None)

    case ClusterDeleted(cluster, recreate) =>
      logger.info(s"Monitoring cluster ${cluster.projectNameString} for deletion.")
      startClusterMonitorActor(cluster, if (recreate) Some(RecreateCluster(cluster)) else None)

    case RecreateCluster(cluster) =>
      if (monitorConfig.recreateCluster) {
        logger.info(s"Recreating cluster ${cluster.projectNameString}...")
        val clusterRequest = ClusterRequest(
          cluster.labels,
          cluster.jupyterExtensionUri,
          cluster.jupyterUserScriptUri,
          Some(cluster.machineConfig),
          None,
          cluster.userJupyterExtensionConfig,
          if (cluster.autopauseThreshold == 0) Some(false) else Some(true),
          Some(cluster.autopauseThreshold),
          cluster.defaultClientId)
        leoService.internalCreateCluster(cluster.auditInfo.creator, cluster.serviceAccountInfo, cluster.googleProject, cluster.clusterName, clusterRequest).failed.foreach { e =>
          logger.error(s"Error occurred recreating cluster ${cluster.projectNameString}", e)
        }
      } else {
        logger.warn(s"Received RecreateCluster message for cluster ${cluster.projectNameString} but cluster recreation is disabled.")
      }

    case StopClusterAfterCreation(cluster) =>
      logger.info(s"Stopping cluster ${cluster.projectNameString} after creation...")
      dbRef.inTransaction { dataAccess =>
        dataAccess.clusterQuery.getClusterById(cluster.id)
      }.flatMap {
        case Some(resolvedCluster) if resolvedCluster.status.isStoppable =>
          leoService.internalStopCluster(resolvedCluster)
        case Some(resolvedCluster) =>
          logger.warn(s"Unable to stop cluster ${resolvedCluster.projectNameString} in status ${resolvedCluster.status.toString} after creation.")
          Future.successful(())
        case None =>
          Future.failed(new WorkbenchException(s"Cluster ${cluster.projectNameString} not found in the database"))
      }.failed.foreach { e =>
        logger.error(s"Error occurred stopping cluster ${cluster.projectNameString} after creation", e)
      }

    case ClusterStopped(cluster) =>
      logger.info(s"Monitoring cluster ${cluster.projectNameString} after stopping.")
      startClusterMonitorActor(cluster)

    case ClusterStarted(cluster) =>
      logger.info(s"Monitoring cluster ${cluster.projectNameString} after starting.")
      startClusterMonitorActor(cluster)

    case AutoFreezeClusters =>
      autoFreezeClusters
  }

  def createChildActor(cluster: Cluster): ActorRef = {
    context.actorOf(ClusterMonitorActor.props(cluster, monitorConfig, dataprocConfig, gdDAO, googleComputeDAO, googleIamDAO, googleStorageDAO, dbRef, authProvider, jupyterProxyDAO))
  }

  def startClusterMonitorActor(cluster: Cluster, watchMessageOpt: Option[ClusterSupervisorMessage] = None): Unit = {
    val child = createChildActor(cluster)

    watchMessageOpt.foreach {
      case RecreateCluster(_) if !monitorConfig.recreateCluster =>
        // don't recreate clusters if not configured to do so
      case watchMsg =>
        context.watchWith(child, watchMsg)
    }
  }

  def autoFreezeClusters: Future[Unit] = {
    val clusterList = dbRef.inTransaction {
      _.clusterQuery.getClustersReadyToAutoFreeze()
    }
    clusterList map { cl =>
      cl.foreach { c =>
        logger.info(s"Auto freezing cluster ${c.clusterName} in project ${c.googleProject}")
        leoService.internalStopCluster(c).failed.foreach { e =>
          logger.warn(s"Error occurred auto freezing cluster ${c.projectNameString}", e)
        }
      }
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
