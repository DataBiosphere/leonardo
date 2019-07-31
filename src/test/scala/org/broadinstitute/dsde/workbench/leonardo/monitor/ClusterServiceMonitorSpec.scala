package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.{ActorRef, ActorSystem, Terminated}
import akka.testkit.TestKit
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleDataprocDAO, MockGoogleProjectDAO}
import org.broadinstitute.dsde.workbench.leonardo.dao.{JupyterDAO, MockJupyterDAO, MockWelderDAO, WelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import com.typesafe.scalalogging.LazyLogging
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics
import org.mockito.ArgumentMatchers
import org.scalatest.concurrent.Eventually.eventually
import cats.effect.IO

import scala.concurrent.duration._
import scala.util.Try

class ClusterServiceMonitorSpec  extends TestKit(ActorSystem("leonardotest")) with
  FlatSpecLike with LazyLogging with BeforeAndAfterAll with TestComponent with CommonTestData with GcsPathUtils with MockitoSugar { testKit =>

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  val welderEnabledCluster = makeCluster(1).copy(status = ClusterStatus.Running, welderEnabled = true)
  val welderDisabledCluster = makeCluster(2).copy(welderEnabled = false)

  it should "report both services are up normally" in isolatedDbTest {
    welderEnabledCluster.save()

    withServiceActor() { (_, mockNewRelic) =>

      Thread.sleep(clusterServiceConfig.pollPeriod.toMillis * 3)
      verify(mockNewRelic, never()).incrementCounterIO("jupyterDown")
      verify(mockNewRelic, never()).incrementCounterIO("welderDown")
    }
  }

  it should "report both services are down" in isolatedDbTest {
    welderEnabledCluster.save()

    withServiceActor(welderDAO = new MockWelderDAO(false), jupyterDAO = new MockJupyterDAO(false)) { (_, mockNewRelic) =>

      eventually(timeout(clusterServiceConfig.pollPeriod * 4)) {
        //the second parameter is needed because of something scala does under the covers that mockito does not like to handle the fact we omit the predefined param count from our incrementCounterIO call.
        //explicitly specifying the count in the incrementCounterIO in the monitor itself does not fix this
        verify(mockNewRelic, times(3)).incrementCounterIO("jupyterDown", 0)
        verify(mockNewRelic, times(3)).incrementCounterIO("welderDown", 0)
      }
    }
  }

  it should "report welder as OK when it is disabled" in isolatedDbTest {
    welderDisabledCluster.save()

    withServiceActor(welderDAO = new MockWelderDAO(false)) { (_, mockNewRelic) =>

      Thread.sleep(clusterServiceConfig.pollPeriod.toMillis * 3)
      verify(mockNewRelic, never()).incrementCounterIO("welderDown")
    }
  }

  private def makeNewRelicMock(): NewRelicMetrics = {
    val mockNewRelic = mock[NewRelicMetrics]
    when(mockNewRelic.incrementCounterIO(ArgumentMatchers.anyString(), ArgumentMatchers.anyInt())).thenReturn(IO.unit)
    mockNewRelic
  }

  private def withServiceActor[T](newRelic: NewRelicMetrics = makeNewRelicMock(), welderDAO: WelderDAO = MockWelderDAO, jupyterDAO: JupyterDAO = MockJupyterDAO)
                                (testCode: (ActorRef, NewRelicMetrics) => T): T = {
    val actor = system.actorOf(ClusterServiceMonitor.props(clusterServiceConfig, gdDAO = new MockGoogleDataprocDAO, googleProjectDAO = new MockGoogleProjectDAO, DbSingleton.ref, welderDAO, jupyterDAO, newRelic))
    val testResult = Try(testCode(actor, newRelic))

    // shut down the actor and wait for it to terminate
    testKit watch actor
    system.stop(actor)
    expectMsgClass(5 seconds, classOf[Terminated])
    testResult.get
  }
}
