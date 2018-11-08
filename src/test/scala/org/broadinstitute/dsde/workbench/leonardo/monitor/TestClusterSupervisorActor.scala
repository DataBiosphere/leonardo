package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.{ActorRef, Props}
import akka.testkit.TestKit
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.leonardo.config.{AutoFreezeConfig, DataprocConfig, MonitorConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.JupyterDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, LeoAuthProvider}

object TestClusterSupervisorActor {
  def props(monitorConfig: MonitorConfig,
            dataprocConfig: DataprocConfig,
            gdDAO: GoogleDataprocDAO,
            googleComputeDAO: GoogleComputeDAO,
            googleIamDAO: GoogleIamDAO,
            googleStorageDAO: GoogleStorageDAO,
            dbRef: DbReference,
            testKit: TestKit,
            authProvider: LeoAuthProvider,
            autoFreezeConfig: AutoFreezeConfig,
            jupyterProxyDAO: JupyterDAO): Props =
    Props(new TestClusterSupervisorActor(
      monitorConfig, dataprocConfig, gdDAO, googleComputeDAO, googleIamDAO, googleStorageDAO,
      dbRef, testKit, authProvider, autoFreezeConfig, jupyterProxyDAO))
}

object TearDown

/**
  * Extends ClusterMonitorSupervisor so the akka TestKit can watch the child ClusterMonitorActors.
  */
class TestClusterSupervisorActor(monitorConfig: MonitorConfig,
                                 dataprocConfig: DataprocConfig,
                                 gdDAO: GoogleDataprocDAO,
                                 googleComputeDAO: GoogleComputeDAO,
                                 googleIamDAO: GoogleIamDAO,
                                 googleStorageDAO: GoogleStorageDAO,
                                 dbRef: DbReference,
                                 testKit: TestKit,
                                 authProvider: LeoAuthProvider,
                                 autoFreezeConfig: AutoFreezeConfig,
                                 jupyterProxyDAO: JupyterDAO)
  extends ClusterMonitorSupervisor(
    monitorConfig, dataprocConfig, gdDAO, googleComputeDAO,
    googleIamDAO, googleStorageDAO, dbRef,
    authProvider, autoFreezeConfig, jupyterProxyDAO) {

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
