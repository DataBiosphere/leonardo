package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.{ActorRef, Props}
import akka.testkit.TestKit
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.leonardo.config.{DataprocConfig, MonitorConfig}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, LeoAuthProvider}

import scala.concurrent.duration._

object TestClusterSupervisorActor {
  def props(dataprocConfig: DataprocConfig, gdDAO: DataprocDAO, googleIamDAO: GoogleIamDAO, dbRef: DbReference, clusterDnsCache: ActorRef, testKit: TestKit, authProvider: LeoAuthProvider): Props =
    Props(new TestClusterSupervisorActor(dataprocConfig, gdDAO, googleIamDAO, dbRef, clusterDnsCache, testKit, authProvider))
}

/**
  * Extends ClusterMonitorSupervisor so the akka TestKit can watch the child ClusterMonitorActors.
  */
class TestClusterSupervisorActor(dataprocConfig: DataprocConfig, gdDAO: DataprocDAO, googleIamDAO: GoogleIamDAO, dbRef: DbReference, clusterDnsCache: ActorRef, testKit: TestKit, authProvider: LeoAuthProvider) extends ClusterMonitorSupervisor(MonitorConfig(100 millis), dataprocConfig, gdDAO, googleIamDAO, dbRef, clusterDnsCache, authProvider) {
  override def createChildActor(cluster: Cluster): ActorRef = {
    val child = super.createChildActor(cluster)
    testKit watch child
    child
  }
}
