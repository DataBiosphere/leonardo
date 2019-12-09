package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.{Duration, Instant}
import java.util.UUID

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props, Timers}
import cats.effect.{ContextShift, IO}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.leonardo.config.{AutoFreezeConfig, ClusterBucketConfig, DataprocConfig, MonitorConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.dao.{JupyterDAO, RStudioDAO, ToolDAO, WelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterContainerServiceType, LeoAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.{ClusterSupervisorMessage, _}
import org.broadinstitute.dsde.workbench.leonardo.util.ClusterHelper
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchException}
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ClusterMonitorSupervisor {
  def props(
    monitorConfig: MonitorConfig,
    dataprocConfig: DataprocConfig,
    clusterBucketConfig: ClusterBucketConfig,
    gdDAO: GoogleDataprocDAO,
    googleComputeDAO: GoogleComputeDAO,
    googleStorageDAO: GoogleStorageDAO,
    google2StorageDAO: GoogleStorageService[IO],
    dbRef: DbReference,
    authProvider: LeoAuthProvider[IO],
    autoFreezeConfig: AutoFreezeConfig,
    jupyterProxyDAO: JupyterDAO,
    rstudioProxyDAO: RStudioDAO,
    welderDAO: WelderDAO[IO],
    clusterHelper: ClusterHelper
  )(implicit metrics: NewRelicMetrics[IO],
    clusterToolToToolDao: ClusterContainerServiceType => ToolDAO[ClusterContainerServiceType],
    cs: ContextShift[IO]): Props =
    Props(
      new ClusterMonitorSupervisor(monitorConfig,
                                   dataprocConfig,
                                   clusterBucketConfig,
                                   gdDAO,
                                   googleComputeDAO,
                                   googleStorageDAO,
                                   google2StorageDAO,
                                   dbRef,
                                   authProvider,
                                   autoFreezeConfig,
                                   jupyterProxyDAO,
                                   rstudioProxyDAO,
                                   welderDAO,
                                   clusterHelper)
    )

  sealed trait ClusterSupervisorMessage

  // sent after a cluster is created by the user
  case class ClusterCreated(cluster: Cluster, stopAfterCreate: Boolean = false) extends ClusterSupervisorMessage
  // sent after a cluster is deleted by the user
  case class ClusterDeleted(cluster: Cluster, recreate: Boolean = false) extends ClusterSupervisorMessage
  // sent after a cluster is stopped by the user
  case class ClusterStopped(cluster: Cluster, updateAfterStop: Boolean = false) extends ClusterSupervisorMessage
  // sent after a cluster is started by the user
  case class ClusterStarted(cluster: Cluster) extends ClusterSupervisorMessage
  // sent after a cluster is updated by the user
  case class ClusterUpdated(cluster: Cluster) extends ClusterSupervisorMessage
  // sent after cluster creation fails, and the cluster should be recreated
  case class RecreateCluster(cluster: Cluster) extends ClusterSupervisorMessage
  // sent after cluster creation succeeds, and the cluster should be stopped
  case class StopClusterAfterCreation(cluster: Cluster) extends ClusterSupervisorMessage
  // sent after cluster stop succeeds, and the cluster should be updated
  case class ClusterStopAndUpdate(cluster: Cluster) extends ClusterSupervisorMessage
  //sent when the the update endpoint signals a cluster should be stopped
  case class ClusterStopQueued(cluster: Cluster) extends ClusterSupervisorMessage
  //Sent when the cluster should be removed from the monitored cluster list
  case class RemoveFromList(cluster: Cluster) extends ClusterSupervisorMessage
  // Auto freeze idle clusters
  case object AutoFreezeClusters extends ClusterSupervisorMessage

  case object CheckClusterTimerKey
  case object AutoFreezeTimerKey
  private case object CheckForClusters extends ClusterSupervisorMessage
}

class ClusterMonitorSupervisor(
  monitorConfig: MonitorConfig,
  dataprocConfig: DataprocConfig,
  clusterBucketConfig: ClusterBucketConfig,
  gdDAO: GoogleDataprocDAO,
  googleComputeDAO: GoogleComputeDAO,
  googleStorageDAO: GoogleStorageDAO,
  google2StorageDAO: GoogleStorageService[IO],
  dbRef: DbReference,
  authProvider: LeoAuthProvider[IO],
  autoFreezeConfig: AutoFreezeConfig,
  jupyterProxyDAO: JupyterDAO,
  rstudioProxyDAO: RStudioDAO,
  welderProxyDAO: WelderDAO[IO],
  clusterHelper: ClusterHelper
)(implicit metrics: NewRelicMetrics[IO],
  clusterToolToToolDao: ClusterContainerServiceType => ToolDAO[ClusterContainerServiceType],
  cs: ContextShift[IO])
    extends Actor
    with Timers
    with LazyLogging {
  import context.dispatcher

  var monitoredClusterIds: Set[Long] = Set.empty

  override def preStart(): Unit = {
    super.preStart()

    timers.startPeriodicTimer(CheckClusterTimerKey, CheckForClusters, monitorConfig.pollPeriod)

    if (autoFreezeConfig.enableAutoFreeze)
      timers.startPeriodicTimer(AutoFreezeTimerKey, AutoFreezeClusters, autoFreezeConfig.autoFreezeCheckScheduler)
  }

  override def receive: Receive = {

    case ClusterCreated(cluster, stopAfterCreate) =>
      logger.info(s"Monitoring cluster ${cluster.projectNameString} for initialization.")
      addToMonitoredClusters(cluster)
      startClusterMonitorActor(cluster, if (stopAfterCreate) Some(StopClusterAfterCreation(cluster)) else None)

    case ClusterDeleted(cluster, recreate) =>
      logger.info(s"Monitoring cluster ${cluster.projectNameString} for deletion.")
      addToMonitoredClusters(cluster)
      startClusterMonitorActor(cluster, if (recreate) Some(RecreateCluster(cluster)) else None)

    case ClusterUpdated(cluster) =>
      logger.info(s"Monitor cluster ${cluster.projectNameString} for updating.")
      addToMonitoredClusters(cluster)
      startClusterMonitorActor(cluster, None)

    case RecreateCluster(cluster) =>
      val traceId = UUID.randomUUID()
      implicit val traceIdIO = ApplicativeAsk.const[IO, TraceId](TraceId(traceId))

      if (monitorConfig.recreateCluster) {
        logger.info(s"[$traceId] Recreating cluster ${cluster.projectNameString}...")
        removeFromMonitoredClusters(cluster)
        for {
          now <- IO(Instant.now).unsafeToFuture()
          _ <- dbRef
            .inTransaction { dataAccess =>
              dataAccess.clusterQuery.clearAsyncClusterCreationFields(cluster, now) >>
                dataAccess.clusterQuery.updateClusterStatus(cluster.id, ClusterStatus.Creating, now)
            }
        } yield ()
      } else {
        logger.warn(
          s"[$traceId] Received RecreateCluster message for cluster ${cluster.projectNameString} but cluster recreation is disabled."
        )
      }

    case StopClusterAfterCreation(cluster) =>
      logger.info(s"Stopping cluster ${cluster.projectNameString} after creation...")
      dbRef
        .inTransaction { dataAccess =>
          dataAccess.clusterQuery.getClusterById(cluster.id)
        }
        .flatMap {
          case Some(resolvedCluster) if resolvedCluster.status.isStoppable =>
            stopCluster(resolvedCluster)
          case Some(resolvedCluster) =>
            logger.warn(
              s"Unable to stop cluster ${resolvedCluster.projectNameString} in status ${resolvedCluster.status.toString} after creation."
            )
            Future.successful(())
          case None =>
            Future.failed(new WorkbenchException(s"Cluster ${cluster.projectNameString} not found in the database"))
        }
        .failed
        .foreach { e =>
          logger.error(s"Error occurred stopping cluster ${cluster.projectNameString} after creation", e)
        }

//    case ClusterStopQueued(cluster) =>
//      logger.info(s"Monitoring cluster ${cluster.projectNameString} with stop queued")
//      addToMonitoredClusters(cluster)
//      clusterHelper.stopCluster(cluster)
//
//      startClusterMonitorActor(cluster, Some(ClusterStopped(cluster, true)))

    case ClusterStopped(cluster, updateAfterStop) =>
      logger.info(s"Monitoring cluster ${cluster.projectNameString} after stopping.")
      addToMonitoredClusters(cluster)
      startClusterMonitorActor(cluster, if (updateAfterStop) Some(ClusterStopAndUpdate(cluster)) else None)

    case ClusterStopAndUpdate(cluster) =>
      logger.info(s"Updating cluster ${cluster.projectNameString} after stopping...")

      dbRef.inTransaction {
        dataAccess => dataAccess.clusterQuery.getClusterById(cluster.id)
      }.flatMap {
        case Some(resolvedCluster) if resolvedCluster.status == ClusterStatus.Stopped && !resolvedCluster.updatedMachineConfig.masterMachineType.isEmpty => {
          // do update
          logger.info(s"In update of UpdateClusterAfterStop, updating cluster to machine type ${MachineType(resolvedCluster.updatedMachineConfig.masterMachineType.get)}")
          for {
            // perform gddao and db updates for new resources
            _ <- clusterHelper.updateMasterMachineType(resolvedCluster, MachineType(resolvedCluster.updatedMachineConfig.masterMachineType.get)).unsafeToFuture()
            // start cluster
            _ <- clusterHelper.internalStartCluster(resolvedCluster).unsafeToFuture()
            // clean up temporary state used for transition
            //TODO: why doesn't this work
            _ <- dbRef.inTransaction {
              dataAccess => dataAccess.clusterQuery.updateClusterForFinishedTransition(resolvedCluster.id)
            }
          } yield ()
        }
        case Some(resolvedCluster) => {
          logger.warn(s"Unable to update cluster ${resolvedCluster.projectNameString} in status ${resolvedCluster.status.toString} after stopping.")
          //if we fail, we want to unmark the cluster for update in the db
          dbRef.inTransaction {
            dataAccess => dataAccess.clusterQuery.updateClusterForFinishedTransition(resolvedCluster.id)
          }.void
        }
        case None => Future.failed(new WorkbenchException(s"Cluster ${cluster.projectNameString} not found in the database"))
      }.failed
        .foreach { e =>
          logger.error(s"Error occurred updating cluster ${cluster.projectNameString} after stopping", e)
        }

    case ClusterStarted(cluster) =>
      logger.info(s"Monitoring cluster ${cluster.projectNameString} after starting.")
      addToMonitoredClusters(cluster)
      startClusterMonitorActor(cluster)

    case AutoFreezeClusters =>
      autoFreezeClusters()

    case CheckForClusters =>
      createClusterMonitors

    case RemoveFromList(cluster) =>
      removeFromMonitoredClusters(cluster)
  }

  def createChildActor(cluster: Cluster): ActorRef =
    context.actorOf(
      ClusterMonitorActor.props(
        cluster.id,
        monitorConfig,
        dataprocConfig,
        clusterBucketConfig,
        gdDAO,
        googleComputeDAO,
        googleStorageDAO,
        google2StorageDAO,
        dbRef,
        authProvider,
        clusterHelper
      )
    )

  def startClusterMonitorActor(cluster: Cluster, watchMessageOpt: Option[ClusterSupervisorMessage] = None): Unit = {
    val child = createChildActor(cluster)
    watchMessageOpt.foreach {
      case RecreateCluster(_) if !monitorConfig.recreateCluster =>
      // don't recreate clusters if not configured to do so
      case watchMsg =>
        context.watchWith(child, watchMsg)
    }
  }

  def autoFreezeClusters(): Future[Unit] =
    for {
      clusters <- dbRef.inTransaction {
        _.clusterQuery.getClustersReadyToAutoFreeze()
      }
      now <- IO(Instant.now).unsafeToFuture()
      pauseableClusters <- clusters.toList.filterA { cluster =>
        jupyterProxyDAO.isAllKernalsIdle(cluster.googleProject, cluster.clusterName).attempt.map {
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
                    dataAccess.clusterQuery.updateKernelFoundBusyDate(cluster.id, now, now)
                  }
                  false // max kernel active time has not been exceeded
                }
              }
              if (maxKernelActiveTimeExceeded) {
                metrics.incrementCounter("autoPause/maxKernelActiveTimeExceeded").unsafeRunAsync(_ => ())
                logger.info(
                  s"Auto pausing ${cluster.googleProject}/${cluster.clusterName} due to exceeded max kernel active time"
                )
              } else {
                metrics.incrementCounter("autoPause/activeKernelClusters").unsafeRunAsync(_ => ())
                logger.info(
                  s"Not going to auto pause cluster ${cluster.googleProject}/${cluster.clusterName} due to active kernels"
                )
              }
              maxKernelActiveTimeExceeded

            } else isIdle
        }
      }
      _ <- metrics.gauge("autoPause/numOfCusters", pauseableClusters.length).unsafeToFuture()
      _ <- pauseableClusters.traverse { cl =>
        logger.info(s"Auto freezing cluster ${cl.clusterName} in project ${cl.googleProject}")
        stopCluster(cl).attempt.map { e =>
          e.fold(t => logger.warn(s"Error occurred auto freezing cluster ${cl.projectNameString}", e), identity)
        }
      }
    } yield ()

  private def stopCluster(cluster: Cluster): Future[Unit] =
    for {
      // Flush the welder cache to disk
      _ <- if (cluster.welderEnabled) {
        welderProxyDAO
          .flushCache(cluster.googleProject, cluster.clusterName)
          .handleError(e => logger.error(s"Failed to flush welder cache for ${cluster.projectNameString}", e))
          .unsafeToFuture()
      } else Future.unit

      // Stop the cluster in Google
      _ <- clusterHelper.stopCluster(cluster).unsafeToFuture()

      // Update the cluster status to Stopping
      now <- IO(Instant.now).unsafeToFuture()
      _ <- dbRef.inTransaction { _.clusterQuery.setToStopping(cluster.id, now) }
    } yield ()

  private def createClusterMonitors(): Unit =
    dbRef
      .inTransaction { _.clusterQuery.listMonitoredClusterOnly() }
      .onComplete {
        case Success(clusters) =>
          val clustersNotAlreadyBeingMonitored = clusters.filterNot(c => monitoredClusterIds.contains(c.id))

          clustersNotAlreadyBeingMonitored foreach {

            case c if c.status == ClusterStatus.Deleting => self ! ClusterDeleted(c)

            case c if c.status == ClusterStatus.Stopping => self ! ClusterStopped(c, c.stopAndUpdate)

            case c if c.status == ClusterStatus.Starting => self ! ClusterStarted(c)

            case c if c.status == ClusterStatus.Updating => self ! ClusterUpdated(c)

            case c if c.status == ClusterStatus.Creating => self ! ClusterCreated(c, c.stopAfterCreation)

            case c => logger.warn(s"Unhandled status(${c.status}) in ClusterMonitorSupervisor")
          }
        case Failure(e) =>
          logger.error("Error starting cluster monitor", e)
      }

  private def addToMonitoredClusters(cluster: Cluster) =
    monitoredClusterIds += cluster.id
  private def removeFromMonitoredClusters(cluster: Cluster) =
    monitoredClusterIds -= cluster.id

  override val supervisorStrategy = {
    // TODO add threshold monitoring stuff from Rawls
    // for now always restart the child actor in case of failure
    OneForOneStrategy(maxNrOfRetries = monitorConfig.maxRetries) {
      case _ => Restart
    }
  }
}
