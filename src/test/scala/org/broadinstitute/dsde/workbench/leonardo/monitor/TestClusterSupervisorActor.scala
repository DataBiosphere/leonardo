package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.{ActorRef, Props}
import akka.testkit.TestKit
import cats.effect.IO
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.leonardo.config.{AutoFreezeConfig, ClusterBucketConfig, DataprocConfig, MonitorConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.{JupyterDAO, RStudioDAO, WelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, LeoAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.service.LeonardoService
import org.broadinstitute.dsde.workbench.leonardo.util.ClusterHelper

object TestClusterSupervisorActor {
  def props(monitorConfig: MonitorConfig,
            dataprocConfig: DataprocConfig,
            clusterBucketConfig: ClusterBucketConfig,
            gdDAO: GoogleDataprocDAO,
            googleComputeDAO: GoogleComputeDAO,
            googleStorageDAO: GoogleStorageDAO,
            google2StorageDAO: GoogleStorageService[IO],
            dbRef: DbReference,
            testKit: TestKit,
            authProvider: LeoAuthProvider,
            autoFreezeConfig: AutoFreezeConfig,
            jupyterProxyDAO: JupyterDAO,
            rstudioProxyDAO: RStudioDAO,
            welderDAO: WelderDAO,
            leonardoService: LeonardoService,
            clusterHelper: ClusterHelper): Props =
    Props(new TestClusterSupervisorActor(
      monitorConfig, dataprocConfig, clusterBucketConfig, gdDAO, googleComputeDAO, googleStorageDAO,
      google2StorageDAO, dbRef, testKit, authProvider, autoFreezeConfig, jupyterProxyDAO, rstudioProxyDAO, welderDAO,
      leonardoService, clusterHelper))
}

object TearDown

/**
  * Extends ClusterMonitorSupervisor so the akka TestKit can watch the child ClusterMonitorActors.
  */
class TestClusterSupervisorActor(monitorConfig: MonitorConfig,
                                 dataprocConfig: DataprocConfig,
                                 clusterBucketConfig: ClusterBucketConfig,
                                 gdDAO: GoogleDataprocDAO,
                                 googleComputeDAO: GoogleComputeDAO,
                                 googleStorageDAO: GoogleStorageDAO,
                                 google2StorageDAO: GoogleStorageService[IO],
                                 dbRef: DbReference,
                                 testKit: TestKit,
                                 authProvider: LeoAuthProvider,
                                 autoFreezeConfig: AutoFreezeConfig,
                                 jupyterProxyDAO: JupyterDAO,
                                 rstudioProxyDAO: RStudioDAO,
                                 welderDAO: WelderDAO,
                                 leonardoService: LeonardoService,
                                 clusterHelper: ClusterHelper)
  extends ClusterMonitorSupervisor(
    monitorConfig, dataprocConfig, clusterBucketConfig, gdDAO, googleComputeDAO,
    googleStorageDAO, google2StorageDAO, dbRef, authProvider, autoFreezeConfig,
    jupyterProxyDAO, rstudioProxyDAO, welderDAO, leonardoService, clusterHelper) {

  // Keep track of spawned child actors so we can shut them down when this actor is stopped
  var childActors: Seq[ActorRef] = Seq.empty

  override def createChildActor(cluster: Cluster): ActorRef = {
    val child = super.createChildActor(cluster)
    childActors = child +: childActors
    testKit watch child
    child
  }

  def tearDown: PartialFunction[Any, Unit] = {
    case TearDown =>
      childActors.foreach { context.stop }
      context.stop(self)
  }

  override def receive: Receive = tearDown orElse super.receive
}
