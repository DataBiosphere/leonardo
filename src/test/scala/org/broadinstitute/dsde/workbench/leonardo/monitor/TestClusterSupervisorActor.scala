package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.{ActorRef, Props}
import akka.testkit.TestKit
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.leonardo.config.{DataprocConfig, MonitorConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, LeoAuthProvider}

import scala.concurrent.duration._

object TestClusterSupervisorActor {
  def props(dataprocConfig: DataprocConfig, gdDAO: GoogleDataprocDAO, googleComputeDAO: GoogleComputeDAO, googleIamDAO: GoogleIamDAO, googleStorageDAO: GoogleStorageDAO, dbRef: DbReference, clusterDnsCache: ActorRef, testKit: TestKit, authProvider: LeoAuthProvider): Props =
    Props(new TestClusterSupervisorActor(dataprocConfig, gdDAO, googleComputeDAO, googleIamDAO, googleStorageDAO, dbRef, clusterDnsCache, testKit, authProvider))
}

object TearDown

/**
  * Extends ClusterMonitorSupervisor so the akka TestKit can watch the child ClusterMonitorActors.
  */
class TestClusterSupervisorActor(dataprocConfig: DataprocConfig, gdDAO: GoogleDataprocDAO, googleComputeDAO: GoogleComputeDAO, googleIamDAO: GoogleIamDAO, googleStorageDAO: GoogleStorageDAO, dbRef: DbReference, clusterDnsCache: ActorRef, testKit: TestKit, authProvider: LeoAuthProvider) extends ClusterMonitorSupervisor(MonitorConfig(100 millis), dataprocConfig, gdDAO, googleComputeDAO, googleIamDAO, googleStorageDAO, dbRef, clusterDnsCache, authProvider) {
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
