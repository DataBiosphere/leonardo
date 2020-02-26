package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.{ActorRef, ActorSystem, Terminated}
import akka.testkit.TestKit
import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleDataprocDAO, MockGoogleProjectDAO}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.{GcsPathUtils, RuntimeContainerServiceType, RuntimeStatus}
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Try

class ClusterToolMonitorSpec
    extends TestKit(ActorSystem("leonardotest"))
    with FlatSpecLike
    with LazyLogging
    with BeforeAndAfterAll
    with TestComponent
    with GcsPathUtils
    with MockitoSugar { testKit =>

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  val welderEnabledCluster = makeCluster(1).copy(status = RuntimeStatus.Running,
                                                 welderEnabled = true,
                                                 runtimeImages = Set(jupyterImage, welderImage))
  val welderDisabledCluster =
    makeCluster(2).copy(status = RuntimeStatus.Running, welderEnabled = false, runtimeImages = Set(jupyterImage))
  val notRunningCluster = makeCluster(3).copy(status = RuntimeStatus.Deleted,
                                              welderEnabled = true,
                                              runtimeImages = Set(jupyterImage, welderImage))
  val rstudioCluster = makeCluster(4).copy(status = RuntimeStatus.Running,
                                           welderEnabled = true,
                                           runtimeImages = Set(rstudioImage, welderImage))

  it should "report all services are up normally" in isolatedDbTest {
    welderEnabledCluster.save()
    welderDisabledCluster.save()
    notRunningCluster.save()
    rstudioCluster.save()

    withServiceActor() { (_, mockNewRelic) =>
      Thread.sleep(clusterToolConfig.pollPeriod.toMillis * 3)
      RuntimeContainerServiceType.values.foreach { service =>
        verify(mockNewRelic, never()).incrementCounter(ArgumentMatchers.startsWith(service.toString + "Down"),
                                                       ArgumentMatchers.anyInt())
      }
    }
  }

  it should "report services are down for a Jupyter image" in isolatedDbTest {
    welderEnabledCluster.save()

    withServiceActor(welderDAO = new MockWelderDAO(false), jupyterDAO = new MockJupyterDAO(false)) {
      (_, mockNewRelic) =>
        eventually(timeout(clusterToolConfig.pollPeriod * 4)) {
          //the second parameter is needed because of something scala does under the covers that mockito does not like to handle the fact we omit the predefined param count from our incrementCounterIO call.
          //explicitly specifying the count in the incrementCounterIO in the monitor itself does not fix this
          verify(mockNewRelic, times(3)).incrementCounter(ArgumentMatchers.startsWith("JupyterServiceDown"),
                                                          ArgumentMatchers.anyInt())
          verify(mockNewRelic, times(3)).incrementCounter(ArgumentMatchers.startsWith("WelderServiceDown"),
                                                          ArgumentMatchers.anyInt())
          verify(mockNewRelic, never()).incrementCounter(ArgumentMatchers.startsWith("RStudioServiceDown"),
                                                         ArgumentMatchers.anyInt())
        }
    }
  }

  it should "report services are down for a RStudio image" in isolatedDbTest {
    rstudioCluster.save()

    withServiceActor(welderDAO = new MockWelderDAO(false), rstudioDAO = new MockRStudioDAO(false)) {
      (_, mockNewRelic) =>
        eventually(timeout(clusterToolConfig.pollPeriod * 4)) {
          //the second parameter is needed because of something scala does under the covers that mockito does not like to handle the fact we omit the predefined param count from our incrementCounterIO call.
          //explicitly specifying the count in the incrementCounterIO in the monitor itself does not fix this
          verify(mockNewRelic, times(3)).incrementCounter(ArgumentMatchers.startsWith("RStudioServiceDown"),
                                                          ArgumentMatchers.anyInt())
          verify(mockNewRelic, times(3)).incrementCounter(ArgumentMatchers.startsWith("WelderServiceDown"),
                                                          ArgumentMatchers.anyInt())
          verify(mockNewRelic, never()).incrementCounter(ArgumentMatchers.startsWith("JupyterServiceDown"),
                                                         ArgumentMatchers.anyInt())
        }
    }
  }

  it should "report welder as OK when it is disabled while jupyter is down" in isolatedDbTest {
    welderDisabledCluster.save()

    withServiceActor(welderDAO = new MockWelderDAO(false), jupyterDAO = new MockJupyterDAO(false)) {
      (_, mockNewRelic) =>
        eventually(timeout(clusterToolConfig.pollPeriod * 4)) {
          verify(mockNewRelic, times(3)).incrementCounter(ArgumentMatchers.startsWith("JupyterServiceDown"),
                                                          ArgumentMatchers.anyInt())
        }

        Thread.sleep(clusterToolConfig.pollPeriod.toMillis * 3)
        verify(mockNewRelic, never()).incrementCounter(ArgumentMatchers.startsWith("WelderServiceDown"),
                                                       ArgumentMatchers.anyInt())
    }
  }

  it should "not check a non-active cluster" in isolatedDbTest {
    notRunningCluster.save()

    withServiceActor(welderDAO = new MockWelderDAO(false), jupyterDAO = new MockJupyterDAO(false)) {
      (_, mockNewRelic) =>
        Thread.sleep(clusterToolConfig.pollPeriod.toMillis * 3)
        verify(mockNewRelic, never()).incrementCounter(ArgumentMatchers.startsWith("WelderServiceDown"),
                                                       ArgumentMatchers.anyInt())
        verify(mockNewRelic, never()).incrementCounter(ArgumentMatchers.startsWith("JupyterServiceDown"),
                                                       ArgumentMatchers.anyInt())
    }
  }

  private def withServiceActor[T](
    newRelic: NewRelicMetrics[IO] = mock[NewRelicMetrics[IO]],
    welderDAO: WelderDAO[IO] = new MockWelderDAO(),
    jupyterDAO: JupyterDAO[IO] = new MockJupyterDAO(),
    rstudioDAO: RStudioDAO[IO] = new MockRStudioDAO()
  )(testCode: (ActorRef, NewRelicMetrics[IO]) => T): T = {
    implicit def clusterToolToToolDao = ToolDAO.clusterToolToToolDao(jupyterDAO, welderDAO, rstudioDAO)

    val actor = system.actorOf(
      ClusterToolMonitor.props(clusterToolConfig,
                               gdDAO = new MockGoogleDataprocDAO,
                               googleProjectDAO = new MockGoogleProjectDAO,
                               DbSingleton.dbRef,
                               newRelic)
    )
    val testResult = Try(testCode(actor, newRelic))

    // shut down the actor and wait for it to terminate
    testKit watch actor
    system.stop(actor)
    expectMsgClass(5 seconds, classOf[Terminated])
    testResult.get
  }
}
