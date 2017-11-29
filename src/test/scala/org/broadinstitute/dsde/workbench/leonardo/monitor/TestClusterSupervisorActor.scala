package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.{ActorRef, Props}
import akka.testkit.TestKit
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.leonardo.config.{DataprocConfig, MonitorConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.DataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster

import scala.concurrent.duration._

object TestClusterSupervisorActor {
  def props(dataprocConfig: DataprocConfig, gdDAO: DataprocDAO, googleIamDAO: GoogleIamDAO, dbRef: DbReference, clusterDnsCache: ActorRef, testKit: TestKit): Props =
    Props(new TestClusterSupervisorActor(dataprocConfig, gdDAO, googleIamDAO, dbRef, clusterDnsCache, testKit))
}

/**
  * Extends ClusterMonitorSupervisor so the akka TestKit can watch the child ClusterMonitorActors.
  */
class TestClusterSupervisorActor(dataprocConfig: DataprocConfig, gdDAO: DataprocDAO, googleIamDAO: GoogleIamDAO, dbRef: DbReference, clusterDnsCache: ActorRef, testKit: TestKit) extends ClusterMonitorSupervisor(MonitorConfig(100 millis), dataprocConfig, gdDAO, googleIamDAO, dbRef, clusterDnsCache) {
  override def createChildActor(cluster: Cluster): ActorRef = {
    val child = super.createChildActor(cluster)
    testKit watch child
    child
  }
}
