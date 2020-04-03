package org.broadinstitute.dsde.workbench.leonardo
package monitor

import akka.actor.{ActorRef, Props}
import akka.testkit.TestKit
import cats.effect.{ContextShift, IO, Timer}
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.google2.{GoogleComputeService, GoogleStorageService}
import org.broadinstitute.dsde.workbench.leonardo.config.{
  AutoFreezeConfig,
  DataprocConfig,
  GceConfig,
  ImageConfig,
  MonitorConfig,
  RuntimeBucketConfig
}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.{JupyterDAO, RStudioDAO, ToolDAO, WelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.util.RuntimeInstances
import org.broadinstitute.dsde.workbench.openTelemetry.FakeOpenTelemetryMetricsInterpreter

import scala.concurrent.ExecutionContext.global

object TestClusterSupervisorActor {
  def props(
    monitorConfig: MonitorConfig,
    dataprocConfig: DataprocConfig,
    gceConfig: GceConfig,
    imageConfig: ImageConfig,
    clusterBucketConfig: RuntimeBucketConfig,
    gdDAO: GoogleDataprocDAO,
    googleComputeService: GoogleComputeService[IO],
    google2StorageDAO: GoogleStorageService[IO],
    dbRef: DbReference[IO],
    testKit: TestKit,
    authProvider: LeoAuthProvider[IO],
    autoFreezeConfig: AutoFreezeConfig,
    jupyterProxyDAO: JupyterDAO[IO],
    rstudioProxyDAO: RStudioDAO[IO],
    welderDAO: WelderDAO[IO],
    publisherQueue: InspectableQueue[IO, LeoPubsubMessage]
  )(implicit cs: ContextShift[IO], runtimeInstances: RuntimeInstances[IO], timer: Timer[IO]): Props =
    Props(
      new TestClusterSupervisorActor(monitorConfig,
                                     dataprocConfig,
                                     gceConfig,
                                     imageConfig,
                                     clusterBucketConfig,
                                     gdDAO,
                                     googleComputeService,
                                     google2StorageDAO,
                                     dbRef,
                                     testKit,
                                     authProvider,
                                     autoFreezeConfig,
                                     jupyterProxyDAO,
                                     rstudioProxyDAO,
                                     welderDAO,
                                     publisherQueue)
    )
}

object TearDown

/**
 * Extends ClusterMonitorSupervisor so the akka TestKit can watch the child ClusterMonitorActors.
 */
class TestClusterSupervisorActor(
  monitorConfig: MonitorConfig,
  dataprocConfig: DataprocConfig,
  gceConfig: GceConfig,
  imageConfig: ImageConfig,
  clusterBucketConfig: RuntimeBucketConfig,
  gdDAO: GoogleDataprocDAO,
  googleComputeService: GoogleComputeService[IO],
  google2StorageDAO: GoogleStorageService[IO],
  dbRef: DbReference[IO],
  testKit: TestKit,
  authProvider: LeoAuthProvider[IO],
  autoFreezeConfig: AutoFreezeConfig,
  jupyterProxyDAO: JupyterDAO[IO],
  rstudioProxyDAO: RStudioDAO[IO],
  welderDAO: WelderDAO[IO],
  publisherQueue: InspectableQueue[IO, LeoPubsubMessage]
)(implicit cs: ContextShift[IO], runtimeInstances: RuntimeInstances[IO], timer: Timer[IO])
    extends ClusterMonitorSupervisor(
      monitorConfig,
      dataprocConfig,
      gceConfig,
      imageConfig,
      clusterBucketConfig,
      gdDAO,
      googleComputeService,
      google2StorageDAO,
      authProvider,
      rstudioProxyDAO,
      welderDAO,
      publisherQueue
    )(FakeOpenTelemetryMetricsInterpreter,
      global,
      dbRef,
      ToolDAO.clusterToolToToolDao(jupyterProxyDAO, welderDAO, rstudioProxyDAO),
      cs,
      timer,
      runtimeInstances) {
  // Keep track of spawned child actors so we can shut them down when this actor is stopped
  var childActors: Seq[ActorRef] = Seq.empty

  override def createChildActor(cluster: Runtime): ActorRef = {
    val child = super.createChildActor(cluster)
    childActors = child +: childActors
    testKit watch child
    child
  }

  def tearDown: PartialFunction[Any, Unit] = {
    case TearDown =>
      childActors.foreach(context.stop)
      context.stop(self)
  }

  override def receive: Receive = tearDown orElse super.receive
}
