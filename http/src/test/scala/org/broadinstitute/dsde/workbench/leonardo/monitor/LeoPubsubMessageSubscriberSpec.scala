package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.{Blocker, IO, Resource}
import fs2.concurrent.InspectableQueue
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleProjectDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleDataprocDAO, MockGoogleDirectoryDAO, MockGoogleIamDAO, MockGoogleProjectDAO, MockGoogleStorageDAO}
import org.broadinstitute.dsde.workbench.google2.{Event, GoogleSubscriber}
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, MachineConfig, ServiceAccountInfo}
import org.broadinstitute.dsde.workbench.leonardo.config.Config.subscriberConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.WelderDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO, MockGoogleComputeDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.util.{BucketHelper, ClusterHelper, QueueFactory}
import org.scalatest.{FlatSpecLike, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito._
import org.scalatest.concurrent._

class LeoPubsubMessageSubscriberSpec extends TestKit(ActorSystem("leonardotest")) with TestComponent with FlatSpecLike with Matchers with CommonTestData with MockitoSugar with Eventually {
  implicit val cs = IO.contextShift(system.dispatcher)
  implicit val timer = IO.timer(system.dispatcher)
  implicit def unsafeLogger = Slf4jLogger.getLogger[IO]
  val blocker = Blocker.liftExecutionContext(system.dispatcher)

  val mockWelderDAO = mock[WelderDAO[IO]]
  val mockGoogleDirectoryDAO = new MockGoogleDirectoryDAO()
  val gdDAO = new MockGoogleDataprocDAO
  val computeDAO =  new MockGoogleComputeDAO
  val storageDAO =  new MockGoogleStorageDAO
  val iamDAO = new MockGoogleIamDAO
  val projectDAO = new MockGoogleProjectDAO
  val authProvider = mock[LeoAuthProvider[IO]]

  val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
    new MockGoogleStorageDAO
  }

  val bucketHelper = new BucketHelper(computeDAO, storageDAO, FakeGoogleStorageService, serviceAccountProvider)

  val clusterHelper = new ClusterHelper(DbSingleton.ref,
    dataprocConfig,
    imageConfig,
    googleGroupsConfig,
    proxyConfig,
    clusterResourcesConfig,
    clusterFilesConfig,
    monitorConfig,
    bucketHelper,
    gdDAO,
    computeDAO,
    mockGoogleDirectoryDAO,
    iamDAO,
    projectDAO,
    mockWelderDAO,
    blocker)

  val runningCluster = makeCluster(1).copy(
    serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount(project), notebookServiceAccount(project)),
    dataprocInfo = Some(makeDataprocInfo(1).copy(hostIp = None)),
    status = ClusterStatus.Running,
    instances = Set(masterInstance, workerInstance1, workerInstance2)
  )
  "LeoPubsubMessageSubscriber" should "handle StopUpdateMessage and stop cluster id4" in isolatedDbTest {

    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)
    //    leoSubscriber.messageResponder()

    val savedRunningCluster = runningCluster.save()
    println(savedRunningCluster.id)
    println(runningCluster.id)

    val clusterId = savedRunningCluster.id
    val newMachineConfig = defaultMachineConfig.copy(masterMachineType = Some("n1-standard-8"))
    val message = StopUpdateMessage(newMachineConfig, clusterId)

    leoSubscriber.followupMap.size shouldBe 0

    leoSubscriber.messageResponder(message).unsafeRunSync()

    leoSubscriber.followupMap.size shouldBe 1
    leoSubscriber.followupMap.get(ClusterFollowupDetails(clusterId, ClusterStatus.Stopped)) shouldBe Some(newMachineConfig)

    eventually {
      dbFutureValue { _.clusterQuery.getClusterById(clusterId) }.get.status shouldBe ClusterStatus.Stopping
    }

  }

  def makeLeoSubscriber(queue: InspectableQueue[IO, Event[LeoPubsubMessage]]) = {
    val googleSubscriber = mock[GoogleSubscriber[IO, LeoPubsubMessage]]

    new LeoPubsubMessageSubscriber[IO](googleSubscriber, clusterHelper, DbSingleton.ref)
  }



  //gracefully handle a no-op transition finished message

  //gracefully handle transition finished with no follow-up action saved


}
