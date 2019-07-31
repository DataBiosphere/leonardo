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
import org.scalatest.concurrent.Eventually.eventually

import scala.concurrent.duration._
import scala.util.Try

class ClusterServiceMonitorSpec  extends TestKit(ActorSystem("leonardotest")) with
  FlatSpecLike with LazyLogging with BeforeAndAfterAll with TestComponent with CommonTestData with GcsPathUtils with MockitoSugar { testKit =>

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  val testCluster1 = makeCluster(1).copy(status = ClusterStatus.Running)
  val mockNewRelic = mock[NewRelicMetrics]


  it should "report both services are up normally" in isolatedDbTest {
    withServiceActor() { _ =>

      Thread.sleep(clusterServiceConfig.pollPeriod.toMillis * 3)
      verify(mockNewRelic, never()).incrementCounterIO("jupyterDown")
      verify(mockNewRelic, never()).incrementCounterIO("welderDown")

    }
  }

  it should "report both services are down" in isolatedDbTest {
    withServiceActor(mockNewRelic, welderDAO = new MockWelderDAO(false), jupyterDAO = new MockJupyterDAO(false)) { _ =>

//      eventually(timeout(clusterServiceConfig.pollPeriod * 4)) {
      eventually(timeout(5 minutes)) {
        //the second parameter is needed because of something scala does under the covers to handle the fact we omit the predefined param count from our incrementCounterIO call.
        //interestingly, explicitly specifying the count in the incrementCounterIO does not fix this
        verify(mockNewRelic, times(3)).incrementCounterIO("jupyterDown", 0)
        verify(mockNewRelic, times(3)).incrementCounterIO("welderDown", 0)
      }

    }
  }

  private def withServiceActor[T](newRelic: NewRelicMetrics = mockNewRelic, welderDAO: WelderDAO = MockWelderDAO, jupyterDAO: JupyterDAO = MockJupyterDAO)
                                (testCode: ActorRef => T): T = {
    val actor = system.actorOf(ClusterServiceMonitor.props(clusterServiceConfig, gdDAO = new MockGoogleDataprocDAO, googleProjectDAO = new MockGoogleProjectDAO, DbSingleton.ref, welderDAO, jupyterDAO, newRelic))
    val testResult = Try(testCode(actor))

    // shut down the actor and wait for it to terminate
    testKit watch actor
    system.stop(actor)
    expectMsgClass(5 seconds, classOf[Terminated])
    testResult.get
  }




}
