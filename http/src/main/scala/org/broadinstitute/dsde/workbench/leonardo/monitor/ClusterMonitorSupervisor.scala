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
import org.broadinstitute.dsde.workbench.google2.{GoogleComputeService, GoogleStorageService}
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.{JupyterDAO, RStudioDAO, ToolDAO, WelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, DbReference, RuntimeConfigQueries}
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.{ClusterSupervisorMessage, _}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.CreateCluster
import org.broadinstitute.dsde.workbench.leonardo.util.ClusterHelper
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchException}
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics

import scala.concurrent.ExecutionContext

object ClusterMonitorSupervisor {
  def props(
    monitorConfig: MonitorConfig,
    dataprocConfig: DataprocConfig,
    imageConfig: ImageConfig,
    clusterBucketConfig: ClusterBucketConfig,
    gdDAO: GoogleDataprocDAO,
    googleComputeService: GoogleComputeService[IO],
    googleStorageDAO: GoogleStorageDAO,
    google2StorageDAO: GoogleStorageService[IO],
    authProvider: LeoAuthProvider[IO],
    autoFreezeConfig: AutoFreezeConfig,
    jupyterProxyDAO: JupyterDAO[IO],
    rstudioProxyDAO: RStudioDAO[IO],
    welderDAO: WelderDAO[IO],
    clusterHelper: ClusterHelper,
    publisherQueue: fs2.concurrent.InspectableQueue[IO, LeoPubsubMessage]
  )(implicit metrics: NewRelicMetrics[IO],
    dbRef: DbReference[IO],
    ec: ExecutionContext,
    clusterToolToToolDao: RuntimeContainerServiceType => ToolDAO[RuntimeContainerServiceType],
    cs: ContextShift[IO]): Props =
    Props(
      new ClusterMonitorSupervisor(monitorConfig,
                                   dataprocConfig,
                                   imageConfig,
                                   clusterBucketConfig,
                                   gdDAO,
                                   googleComputeService,
                                   googleStorageDAO,
                                   google2StorageDAO,
                                   authProvider,
                                   autoFreezeConfig,
                                   jupyterProxyDAO,
                                   rstudioProxyDAO,
                                   welderDAO,
                                   clusterHelper,
                                   publisherQueue)
    )

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

class ClusterMonitorSupervisor(
  monitorConfig: MonitorConfig,
  dataprocConfig: DataprocConfig,
  imageConfig: ImageConfig,
  clusterBucketConfig: ClusterBucketConfig,
  gdDAO: GoogleDataprocDAO,
  googleComputeService: GoogleComputeService[IO],
  googleStorageDAO: GoogleStorageDAO,
  google2StorageDAO: GoogleStorageService[IO],
  authProvider: LeoAuthProvider[IO],
  autoFreezeConfig: AutoFreezeConfig,
  jupyterProxyDAO: JupyterDAO[IO],
  rstudioProxyDAO: RStudioDAO[IO],
  welderProxyDAO: WelderDAO[IO],
  clusterHelper: ClusterHelper,
  publisherQueue: fs2.concurrent.InspectableQueue[IO, LeoPubsubMessage]
)(implicit metrics: NewRelicMetrics[IO],
  ec: ExecutionContext,
  dbRef: DbReference[IO],
  clusterToolToToolDao: RuntimeContainerServiceType => ToolDAO[RuntimeContainerServiceType],
  cs: ContextShift[IO])
    extends Actor
    with Timers
    with LazyLogging {

  var monitoredClusterIds: Set[Long] = Set.empty

  override def preStart(): Unit = {
    super.preStart()

    timers.startTimerWithFixedDelay(CheckClusterTimerKey, CheckForClusters, monitorConfig.pollPeriod)

    if (autoFreezeConfig.enableAutoFreeze)
      timers.startTimerWithFixedDelay(AutoFreezeTimerKey, AutoFreezeClusters, autoFreezeConfig.autoFreezeCheckScheduler)
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
      val traceId = TraceId(UUID.randomUUID())
      implicit val traceIdIO = ApplicativeAsk.const[IO, TraceId](traceId)

      val res = if (monitorConfig.recreateCluster) {
        logger.info(s"[$traceId] Recreating cluster ${cluster.projectNameString}...")
        removeFromMonitoredClusters(cluster)
        for {
          now <- IO(Instant.now)
          _ <- (clusterQuery.clearAsyncClusterCreationFields(cluster, now) >>
            clusterQuery.updateClusterStatus(cluster.id, RuntimeStatus.Creating, now)).transaction
          runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId).transaction
          _ <- publisherQueue.enqueue1(CreateCluster.fromRuntime(cluster, runtimeConfig, Some(traceId)))
        } yield ()
      } else {
        IO(
          logger.warn(
            s"[$traceId] Received RecreateCluster message for cluster ${cluster.projectNameString} but cluster recreation is disabled."
          )
        )
      }

      res.unsafeRunSync()

    case StopClusterAfterCreation(cluster) =>
      val traceId = UUID.randomUUID()
      implicit val traceIdIO = ApplicativeAsk.const[IO, TraceId](TraceId(traceId))

      logger.info(s"Stopping cluster ${cluster.projectNameString} after creation...")
      val res = clusterQuery
        .getClusterById(cluster.id)
        .transaction
        .flatMap {
          case Some(resolvedCluster) if resolvedCluster.status.isStoppable =>
            IO(Instant.now()).flatMap(now => stopCluster(resolvedCluster, now))
          case Some(resolvedCluster) =>
            IO(
              logger.warn(
                s"Unable to stop cluster ${resolvedCluster.projectNameString} in status ${resolvedCluster.status.toString} after creation."
              )
            )
          case None =>
            IO.raiseError(new WorkbenchException(s"Cluster ${cluster.projectNameString} not found in the database"))
        }
        .handleErrorWith(
          e =>
            IO(logger.error(s"Error occurred stopping cluster ${cluster.projectNameString} after creation", e)) >> IO
              .raiseError(e)
        )
      res.unsafeToFuture()

    case ClusterStopped(cluster) =>
      logger.info(s"Monitoring cluster ${cluster.projectNameString} after stopping.")
      addToMonitoredClusters(cluster)
      startClusterMonitorActor(cluster)

    case ClusterStarted(cluster) =>
      logger.info(s"Monitoring cluster ${cluster.projectNameString} after starting.")
      addToMonitoredClusters(cluster)
      startClusterMonitorActor(cluster)

    case AutoFreezeClusters =>
      val traceId = UUID.randomUUID()
      implicit val traceIdIO = ApplicativeAsk.const[IO, TraceId](TraceId(traceId))
      autoFreezeClusters().unsafeToFuture()

    case CheckForClusters =>
      createClusterMonitors.unsafeRunSync()

    case RemoveFromList(cluster) =>
      removeFromMonitoredClusters(cluster)
  }

  def createChildActor(cluster: Cluster): ActorRef =
    context.actorOf(
      ClusterMonitorActor.props(
        cluster.id,
        monitorConfig,
        dataprocConfig,
        imageConfig,
        clusterBucketConfig,
        gdDAO,
        googleComputeService,
        googleStorageDAO,
        google2StorageDAO,
        dbRef,
        authProvider,
        clusterHelper,
        publisherQueue
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

  def autoFreezeClusters()(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    for {
      clusters <- clusterQuery.getClustersReadyToAutoFreeze.transaction
      now <- IO(Instant.now)
      pauseableClusters <- clusters.toList.filterA { cluster =>
        jupyterProxyDAO.isAllKernelsIdle(cluster.googleProject, cluster.runtimeName).attempt.flatMap {
          case Left(t) =>
            IO(logger.error(s"Fail to get kernel status for ${cluster.googleProject}/${cluster.runtimeName} due to $t"))
              .as(true)
          case Right(isIdle) =>
            if (!isIdle) {
              val idleLimit = Duration.ofNanos(autoFreezeConfig.maxKernelBusyLimit.toNanos) // convert from FiniteDuration to java Duration
              val maxKernelActiveTimeExceeded = cluster.auditInfo.kernelFoundBusyDate match {
                case Some(attemptedDate) => IO(Duration.between(attemptedDate, now).compareTo(idleLimit) == 1)
                case None =>
                  clusterQuery
                    .updateKernelFoundBusyDate(cluster.id, now, now)
                    .transaction
                    .as(false) // max kernel active time has not been exceeded
              }

              maxKernelActiveTimeExceeded.ifM(
                metrics.incrementCounter("autoPause/maxKernelActiveTimeExceeded") >>
                  IO(
                    logger.info(
                      s"Auto pausing ${cluster.googleProject}/${cluster.runtimeName} due to exceeded max kernel active time"
                    )
                  ).as(true),
                metrics.incrementCounter("autoPause/activeKernelClusters") >> IO(
                  logger.info(
                    s"Not going to auto pause cluster ${cluster.googleProject}/${cluster.runtimeName} due to active kernels"
                  )
                ).as(false)
              )
            } else IO.pure(isIdle)
        }
      }
      _ <- metrics.gauge("autoPause/numOfCusters", pauseableClusters.length)
      _ <- pauseableClusters.traverse_ { cl =>
        IO(logger.info(s"Auto freezing cluster ${cl.runtimeName} in project ${cl.googleProject}")) >>
          stopCluster(cl, now).attempt.map { e =>
            e.fold(t => logger.warn(s"Error occurred auto freezing cluster ${cl.projectNameString}", e), identity)
          }
      }
    } yield ()

  private def stopCluster(cluster: Cluster, now: Instant)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    for {
      // Flush the welder cache to disk
      _ <- if (cluster.welderEnabled) {
        welderProxyDAO
          .flushCache(cluster.googleProject, cluster.runtimeName)
          .handleError(e => logger.error(s"Failed to flush welder cache for ${cluster.projectNameString}", e))
      } else IO.unit

      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId).transaction
      // Stop the cluster in Google
      _ <- clusterHelper.stopCluster(cluster, runtimeConfig)

      // Update the cluster status to Stopping
      _ <- dbRef.inTransaction { clusterQuery.setToStopping(cluster.id, now) }
    } yield ()

  private def createClusterMonitors(): IO[Unit] =
    dbRef
      .inTransaction { clusterQuery.listMonitoredClusterOnly }
      .attempt
      .flatMap {
        case Right(clusters) =>
          val clustersNotAlreadyBeingMonitored = clusters.filterNot(c => monitoredClusterIds.contains(c.id))

          clustersNotAlreadyBeingMonitored.toList traverse_ {
            case c if c.status == RuntimeStatus.Deleting => IO(self ! ClusterDeleted(c))

            case c if c.status == RuntimeStatus.Stopping => IO(self ! ClusterStopped(c))

            case c if c.status == RuntimeStatus.Starting => IO(self ! ClusterStarted(c))

            case c if c.status == RuntimeStatus.Updating => IO(self ! ClusterUpdated(c))

            case c if c.status == RuntimeStatus.Creating && c.asyncRuntimeFields.isDefined =>
              IO(self ! ClusterCreated(c, c.stopAfterCreation))

            case c => IO(logger.warn(s"Unhandled status(${c.status}) in ClusterMonitorSupervisor"))
          }
        case Left(e) =>
          IO(logger.error("Error starting cluster monitor", e))
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
