package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.time.{Duration, Instant}

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props, Timers}
import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleProjectDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.leonardo.Metrics
import org.broadinstitute.dsde.workbench.leonardo.config.{AutoFreezeConfig, ClusterBucketConfig, DataprocConfig, MonitorConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.dao.{JupyterDAO, RStudioDAO, WelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterTool.Jupyter
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterRequest, ClusterTool, LeoAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.{ClusterSupervisorMessage, _}
import org.broadinstitute.dsde.workbench.leonardo.service.LeonardoService
import org.broadinstitute.dsde.workbench.model.WorkbenchException

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ClusterMonitorSupervisor {
  def props(monitorConfig: MonitorConfig, dataprocConfig: DataprocConfig, clusterBucketConfig: ClusterBucketConfig, gdDAO: GoogleDataprocDAO, googleProjectDAO: GoogleProjectDAO, googleComputeDAO: GoogleComputeDAO, googleIamDAO: GoogleIamDAO, googleStorageDAO: GoogleStorageDAO, google2StorageDAO: GoogleStorageService[IO], dbRef: DbReference, authProvider: LeoAuthProvider, autoFreezeConfig: AutoFreezeConfig, jupyterProxyDAO: JupyterDAO, rstudioProxyDAO: RStudioDAO, welderDAO: WelderDAO, leonardoService: LeonardoService): Props =
    Props(new ClusterMonitorSupervisor(monitorConfig, dataprocConfig, clusterBucketConfig, gdDAO, googleProjectDAO, googleComputeDAO, googleIamDAO, googleStorageDAO, google2StorageDAO, dbRef, authProvider, autoFreezeConfig, jupyterProxyDAO, rstudioProxyDAO, welderDAO, leonardoService))

  sealed trait ClusterSupervisorMessage

  // sent after a cluster is created by the user
  case class ClusterCreated(cluster: Cluster, stopAfterCreate: Boolean = false) extends ClusterSupervisorMessage
  // sent after a cluster is deleted by the user
  case class ClusterDeleted(cluster: Cluster, recreate: Boolean = false) extends ClusterSupervisorMessage
  // sent after a cluster is stopped by the user
  case class ClusterStopped(cluster: Cluster) extends ClusterSupervisorMessage
  // sent after a cluster is started by the user
  case class ClusterStarted(cluster: Cluster) extends ClusterSupervisorMessage
  // sent after a cluster is updated by the user
  case class ClusterUpdated(cluster: Cluster) extends ClusterSupervisorMessage
  // sent after cluster creation fails, and the cluster should be recreated
  case class RecreateCluster(cluster: Cluster) extends ClusterSupervisorMessage
  // sent after cluster creation succeeds, and the cluster should be stopped
  case class StopClusterAfterCreation(cluster: Cluster) extends ClusterSupervisorMessage
  //Sent when the cluster should be removed from the monitored cluster list
  case class RemoveFromList(cluster: Cluster) extends ClusterSupervisorMessage
  // Auto freeze idle clusters
  case object AutoFreezeClusters extends ClusterSupervisorMessage


  case object CheckClusterTimerKey
  case object AutoFreezeTimerKey
  private case object CheckForClusters extends ClusterSupervisorMessage
}

class ClusterMonitorSupervisor(monitorConfig: MonitorConfig, dataprocConfig: DataprocConfig, clusterBucketConfig: ClusterBucketConfig, gdDAO: GoogleDataprocDAO, googleProjectDAO: GoogleProjectDAO, googleComputeDAO: GoogleComputeDAO, googleIamDAO: GoogleIamDAO, googleStorageDAO: GoogleStorageDAO, google2StorageDAO: GoogleStorageService[IO], dbRef: DbReference, authProvider: LeoAuthProvider, autoFreezeConfig: AutoFreezeConfig, jupyterProxyDAO: JupyterDAO, rstudioProxyDAO: RStudioDAO, welderProxyDAO: WelderDAO, leonardoService: LeonardoService)
  extends Actor with Timers with LazyLogging {
  import context.dispatcher

  var monitoredClusters: Set[Cluster] = Set.empty

  override def preStart(): Unit = {
    super.preStart()

    timers.startPeriodicTimer(CheckClusterTimerKey, CheckForClusters, monitorConfig.pollPeriod)

    if (autoFreezeConfig.enableAutoFreeze)
      timers.startPeriodicTimer(AutoFreezeTimerKey, AutoFreezeClusters, autoFreezeConfig.autoFreezeCheckScheduler)
  }

  override def receive: Receive = {

    case ClusterCreated(cluster, stopAfterCreate) =>
      logger.info(s"Monitoring cluster ${cluster.projectNameString} for initialization.")
      monitoredClusters += cluster
      startClusterMonitorActor(cluster, if (stopAfterCreate) Some(StopClusterAfterCreation(cluster)) else None)

    case ClusterDeleted(cluster, recreate) =>
      logger.info(s"Monitoring cluster ${cluster.projectNameString} for deletion.")
      monitoredClusters += cluster
      startClusterMonitorActor(cluster, if (recreate) Some(RecreateCluster(cluster)) else None)

    case ClusterUpdated(cluster) =>
      logger.info(s"Monitor cluster ${cluster.projectNameString} for updating.")
      monitoredClusters += cluster
      startClusterMonitorActor(cluster, None)

    case RecreateCluster(cluster) =>
      if (monitorConfig.recreateCluster) {
        logger.info(s"Recreating cluster ${cluster.projectNameString}...")
        dbRef.inTransaction { dataAccess =>
          dataAccess.clusterQuery.getClusterById(cluster.id)
        }.flatMap {
          case Some(cluster) =>
            val clusterRequest = ClusterRequest(
              cluster.labels,
              cluster.jupyterExtensionUri,
              cluster.jupyterUserScriptUri,
              Some(cluster.machineConfig),
              cluster.properties,
              None,
              cluster.userJupyterExtensionConfig,
              if (cluster.autopauseThreshold == 0) Some(false) else Some(true),
              Some(cluster.autopauseThreshold),
              cluster.defaultClientId,
              cluster.clusterImages.find(_.tool == Jupyter).map(_.dockerImage))
            val createFuture = leonardoService.internalCreateCluster(cluster.auditInfo.creator, cluster.serviceAccountInfo, cluster.googleProject, cluster.clusterName, clusterRequest)
            createFuture.failed.foreach { e =>
              logger.error(s"Error occurred recreating cluster ${cluster.projectNameString}", e)
            }
            createFuture
          case None => Future.failed(new WorkbenchException(s"Cluster ${cluster.projectNameString} not found in the database"))
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
          leonardoService.internalStopCluster(resolvedCluster)
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
      monitoredClusters += cluster
      startClusterMonitorActor(cluster)

    case ClusterStarted(cluster) =>
      logger.info(s"Monitoring cluster ${cluster.projectNameString} after starting.")
      monitoredClusters += cluster
      startClusterMonitorActor(cluster)

    case AutoFreezeClusters =>
      autoFreezeClusters()

    case CheckForClusters =>
      createClusterMonitors

    case RemoveFromList(cluster) =>
      removeFromMonitoredClusters(cluster)
  }

  def createChildActor(cluster: Cluster): ActorRef = {
    val proxyDAOs: Map[ClusterTool, () => Future[Boolean]] = Map(
      ClusterTool.Jupyter -> (() => jupyterProxyDAO.isProxyAvailable(cluster.googleProject, cluster.clusterName)),
      ClusterTool.RStudio -> (() => rstudioProxyDAO.isProxyAvailable(cluster.googleProject, cluster.clusterName)),
      ClusterTool.Welder -> (() => welderProxyDAO.isProxyAvailable(cluster.googleProject, cluster.clusterName))
    )
    context.actorOf(ClusterMonitorActor.props(cluster, monitorConfig, dataprocConfig, clusterBucketConfig, gdDAO, googleProjectDAO, googleComputeDAO, googleIamDAO, googleStorageDAO, google2StorageDAO, dbRef, authProvider, proxyDAOs))
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

  def autoFreezeClusters(): Future[Unit] = for {
    clusters <- dbRef.inTransaction {
      _.clusterQuery.getClustersReadyToAutoFreeze()
    }
    pauseableClusters <- clusters.toList.filterA {
      cluster =>
        jupyterProxyDAO.isAllKernalsIdle(cluster.googleProject, cluster.clusterName).attempt.map {
          attempted =>
            attempted match {
              case Left(t) =>
                logger.error(s"Fail to get kernel status for ${cluster.googleProject}/${cluster.clusterName} due to $t")
                true
              case Right(isIdle) =>
                if (!isIdle) {
                  val idleLimit = Duration.ofNanos(autoFreezeConfig.maxKernelBusyLimit.toNanos) // convert from FiniteDuration to java Duration
                  val maxKernelActiveTimeExceeded = cluster.auditInfo.kernelFoundBusyDate match {
                    case Some(attemptedDate) => Duration.between(attemptedDate, Instant.now()).compareTo(idleLimit) == 1
                    case None => {
                      dbRef.inTransaction { dataAccess =>
                        dataAccess.clusterQuery.updateKernelFoundBusyDate(cluster.id, Instant.now())
                      }
                      false // max kernel active time has not been exceeded
                    }
                  }
                  if (maxKernelActiveTimeExceeded){
                    Metrics.newRelic.incrementCounterIO("maxKernelExceededClusters").unsafeRunAsync(_ => ())
                    logger.info(s"Auto pausing ${cluster.googleProject}/${cluster.clusterName} due to exceeded max kernel active time")
                  } else {
                    Metrics.newRelic.incrementCounterIO("activeKernelClusters").unsafeRunAsync(_ => ())
                    logger.info(s"Not going to auto pause cluster ${cluster.googleProject}/${cluster.clusterName} due to active kernels")
                  }
                  maxKernelActiveTimeExceeded

                } else isIdle
            }
        }
    }
    _ <- Metrics.newRelic.gauge("autopausedClusters", pauseableClusters.length).unsafeToFuture()
    _ <- pauseableClusters.traverse{
      cl =>
        logger.info(s"Auto freezing cluster ${cl.clusterName} in project ${cl.googleProject}")
        leonardoService.internalStopCluster(cl).attempt.map { e =>
          e.fold(t => logger.warn(s"Error occurred auto freezing cluster ${cl.projectNameString}", e), identity)
        }
    }
  } yield ()

  private def createClusterMonitors(): Unit = {
    val monitoredClusterIds = monitoredClusters.map(_.id)

    dbRef
      .inTransaction { _.clusterQuery.listMonitoredClusterOnly() }
      .onComplete {
        case Success(clusters) =>
          val clustersNotAlreadyBeingMonitored = clusters.filterNot(c => monitoredClusterIds.contains(c.id))

          clustersNotAlreadyBeingMonitored foreach {
            case c if c.status == ClusterStatus.Deleting => self ! ClusterDeleted(c)

            case c if c.status == ClusterStatus.Stopping => self ! ClusterStopped(c)

            case c if c.status == ClusterStatus.Starting => self ! ClusterStarted(c)

            case c if c.status == ClusterStatus.Updating => self ! ClusterUpdated(c)

            case c if c.status == ClusterStatus.Creating => self ! ClusterCreated(c, c.stopAfterCreation)

            case c => logger.warn(s"Unhandled status(${c.status}) in ClusterMonitorSupervisor")
          }
        case Failure(e) =>
          logger.error("Error starting cluster monitor", e)
      }
  }

  private def removeFromMonitoredClusters(cluster: Cluster) = {
    monitoredClusters -= cluster
  }

  override val supervisorStrategy = {
    // TODO add threshold monitoring stuff from Rawls
    // for now always restart the child actor in case of failure
    OneForOneStrategy(maxNrOfRetries = monitorConfig.maxRetries) {
      case _ => Restart
    }
  }
}
