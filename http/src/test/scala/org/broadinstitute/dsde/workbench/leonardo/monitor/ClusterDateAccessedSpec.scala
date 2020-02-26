package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterDateAccessedActor.UpdateDateAccessed
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.ExecutionContext.Implicits.global

class ClusterDateAccessedSpec
    extends TestKit(ActorSystem("leonardotest"))
    with FlatSpecLike
    with BeforeAndAfterAll
    with TestComponent
    with GcsPathUtils { testKit =>

  val testCluster1 = makeCluster(1)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "ClusterDateAccessedMonitor" should "update date accessed" in isolatedDbTest {
    import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq

    val savedTestCluster1 = testCluster1.save()
    savedTestCluster1.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual testCluster1

    val currentTime = Instant.now()
    val dateAccessedActor = system.actorOf(ClusterDateAccessedActor.props(autoFreezeConfig, DbSingleton.dbRef))
    dateAccessedActor ! UpdateDateAccessed(testCluster1.runtimeName, testCluster1.googleProject, currentTime)
    eventually(timeout(Span(5, Seconds))) {
      val c1 = dbFutureValue { clusterQuery.getClusterById(savedTestCluster1.id) }

      c1.map(_.auditInfo.dateAccessed).get shouldBe currentTime
    }
  }
}
