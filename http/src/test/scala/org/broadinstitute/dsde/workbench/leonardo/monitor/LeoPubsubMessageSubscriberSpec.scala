package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.{Blocker, IO}
import com.typesafe.scalalogging.LazyLogging
import fs2.concurrent.InspectableQueue
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleDataprocDAO, MockGoogleDirectoryDAO, MockGoogleIamDAO, MockGoogleProjectDAO, MockGoogleStorageDAO}
import org.broadinstitute.dsde.workbench.google2.{Event, GoogleSubscriber}
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, ServiceAccountInfo}
import org.broadinstitute.dsde.workbench.leonardo.dao.WelderDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.MockGoogleComputeDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.util.{BucketHelper, ClusterHelper, QueueFactory}
import org.scalatest.{FlatSpecLike, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.concurrent._
import org.broadinstitute.dsde.workbench.model.WorkbenchException
import io.circe.parser.decode
import io.circe.syntax._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubCodec._

class LeoPubsubMessageSubscriberSpec extends TestKit(ActorSystem("leonardotest")) with TestComponent with FlatSpecLike with Matchers with CommonTestData with MockitoSugar with Eventually with LazyLogging {
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

  val stoppedCluster = makeCluster(2).copy(
    serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount(project), notebookServiceAccount(project)),
    dataprocInfo = Some(makeDataprocInfo(1).copy(hostIp = None)),
    status = ClusterStatus.Stopped
  )

  "LeoPubsubMessageSubscriber" should "handle StopUpdateMessage and stop cluster" in isolatedDbTest {

    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)

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

  "LeoPubsubMessageSubscriber" should "throw an exception if it receives an incorrect cluster transition finished message and the database does not reflect the state in message" in isolatedDbTest {

    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)

    val savedRunningCluster = runningCluster.save()

    val clusterId = savedRunningCluster.id
    val newMachineConfig = defaultMachineConfig.copy(masterMachineType = Some("n1-standard-8"))
    val followupKey = ClusterFollowupDetails(clusterId, ClusterStatus.Stopped)
    val message = ClusterTransitionFinishedMessage(followupKey)

    leoSubscriber.followupMap.size shouldBe 0

    leoSubscriber.followupMap.put(followupKey, newMachineConfig)

    val caught = the[WorkbenchException] thrownBy {
      leoSubscriber.messageResponder(message).unsafeRunSync()
    }

    leoSubscriber.followupMap.size shouldBe 1
    leoSubscriber.followupMap.get(followupKey) shouldBe Some(newMachineConfig)
    caught.getMessage should include("it is not stopped")

    dbFutureValue { _.clusterQuery.getClusterById(clusterId) }.get.status shouldBe ClusterStatus.Running

  }

  "LeoPubsubMessageSubscriber" should "throw an exception if it receives an incorrect StopUpdate message and the database does not reflect the state in message" in isolatedDbTest {

    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)

    val savedStoppedCluster = stoppedCluster.save()

    val clusterId = savedStoppedCluster.id
    val newMachineConfig = defaultMachineConfig.copy(masterMachineType = Some("n1-standard-8"))
    val message = StopUpdateMessage(newMachineConfig, clusterId)

    leoSubscriber.followupMap.size shouldBe 0

    val caught = the[WorkbenchException] thrownBy {
      leoSubscriber.messageResponder(message).unsafeRunSync()
    }

    leoSubscriber.followupMap.size shouldBe 0

    caught.getMessage should include("Failed to process StopUpdateMessage")

    dbFutureValue { _.clusterQuery.getClusterById(clusterId) }.get.status shouldBe ClusterStatus.Stopped

  }

  "LeoPubsubMessageSubscriber" should "perform a noop when it recieves an irrelevant transition for a cluster in the follow-up map" in isolatedDbTest {
    val savedRunningCluster = runningCluster.save()
    val clusterId = savedRunningCluster.id
    val newMachineConfig = defaultMachineConfig.copy(masterMachineType = Some("n1-standard-8"))
    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)

    val followupKey = ClusterFollowupDetails(clusterId, ClusterStatus.Stopped)

    leoSubscriber.followupMap.put(followupKey, newMachineConfig)

    val transitionFinishedMessage = ClusterTransitionFinishedMessage(ClusterFollowupDetails(clusterId, ClusterStatus.Creating))

    leoSubscriber.followupMap.size shouldBe 1

    leoSubscriber.messageResponder(transitionFinishedMessage).unsafeRunSync()

    leoSubscriber.followupMap.size shouldBe 1

    val cluster = dbFutureValue { _.clusterQuery.getClusterById(clusterId) }.get
    cluster.status shouldBe ClusterStatus.Running
    cluster.machineConfig shouldBe defaultMachineConfig
  }

  "LeoPubsubMessageSubscriber" should "perform a noop recieves a transition for a cluster in the follow-up map which is not in the db" in isolatedDbTest {
//    val savedRunningCluster = runningCluster.save()
//    val clusterId = savedRunningCluster.id
    val newMachineConfig = defaultMachineConfig.copy(masterMachineType = Some("n1-standard-8"))
    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)

    val fakeClusterId = Integer.MAX_VALUE
    val followupKey = ClusterFollowupDetails(fakeClusterId, ClusterStatus.Stopped)

    leoSubscriber.followupMap.put(followupKey, newMachineConfig)

    val transitionFinishedMessage = ClusterTransitionFinishedMessage(followupKey)

    leoSubscriber.followupMap.size shouldBe 1

    val caught = the[ClusterNotFoundException] thrownBy {
      leoSubscriber.messageResponder(transitionFinishedMessage).unsafeRunSync()
    }

    leoSubscriber.followupMap.size shouldBe 1

    val cluster = dbFutureValue { _.clusterQuery.getClusterById(fakeClusterId) }
    cluster shouldBe None
    caught.clusterId shouldBe fakeClusterId
  }

  //gracefully handle transition finished with no follow-up action saved
  "LeoPubsubMessageSubscriber" should "throw an exception when no follow-up action is present for a transition" in isolatedDbTest {
    val savedStoppedCluster = stoppedCluster.save()
    val clusterId = savedStoppedCluster.id
    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)

    val followupKey = ClusterFollowupDetails(clusterId, ClusterStatus.Stopped)
    val transitionFinishedMessage = ClusterTransitionFinishedMessage(followupKey)

    leoSubscriber.followupMap.size shouldBe 0

    leoSubscriber.messageResponder(transitionFinishedMessage).unsafeRunSync()

    val cluster = dbFutureValue { _.clusterQuery.getClusterById(clusterId) }.get
    cluster.status shouldBe ClusterStatus.Stopped
    cluster.machineConfig shouldBe defaultMachineConfig
  }

  //handle transition finished message with follow-up action saved
  "LeoPubsubMessageSubscriber" should "handle perform an update and stop the cluster when a follow-up action is present" in isolatedDbTest {
    val savedStoppedCluster = stoppedCluster.save()
    val clusterId = savedStoppedCluster.id
    val newMachineConfig = defaultMachineConfig.copy(masterMachineType = Some("n1-standard-8"))
    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)

    val followupKey = ClusterFollowupDetails(clusterId, ClusterStatus.Stopped)

    leoSubscriber.followupMap.put(followupKey, newMachineConfig)

    val transitionFinishedMessage = ClusterTransitionFinishedMessage(followupKey)

    leoSubscriber.followupMap.size shouldBe 1
    println(s"Cluster followup map: ${leoSubscriber.followupMap}")

    leoSubscriber.messageResponder(transitionFinishedMessage).unsafeRunSync()

    leoSubscriber.followupMap.size shouldBe 0

    eventually {
      dbFutureValue { _.clusterQuery.getClusterById(clusterId) }.get.status shouldBe ClusterStatus.Starting
      dbFutureValue { _.clusterQuery.getClusterById(clusterId) }.get.machineConfig shouldBe newMachineConfig
    }
  }

  "LeoPubsubCodec" should "encode/decode a StopUpdate message" in isolatedDbTest {
    val originalMessage = StopUpdateMessage(defaultMachineConfig.copy(masterMachineType = Some("n1-standard-8")), 1)
    val json = originalMessage.asJson
    val actualJson = json.toString

    val expectedJson =
      s"""
        |{
        |  "messageType": "stopUpdate",
        |  "updatedMachineConfig": {
        |    "numberOfWorkers": 0,
        |    "masterMachineType": "n1-standard-8",
        |    "masterDiskSize": 500,
        |    "workerMachineType": null,
        |    "workerDiskSize": null,
        |    "numberOfWorkerLocalSSDs": null,
        |    "numberOfPreemptibleWorkers": null
        |  },
        |  "clusterId": 1
        |}
        |""".stripMargin

    actualJson.filterNot(_.isWhitespace) shouldBe expectedJson.filterNot(_.isWhitespace)

    val decodedMessage = decode[StopUpdateMessage](expectedJson)
    logger.info(s"decodedMessage: ${decodedMessage}")
    decodedMessage.right.get shouldBe originalMessage
  }

  "LeoPubsubCodec" should "encode/decode a ClusterTransitionFinished message" in isolatedDbTest {
    val clusterFollowupDetails = ClusterFollowupDetails(1, ClusterStatus.Stopping)
    val originalMessage = ClusterTransitionFinishedMessage(clusterFollowupDetails)
    val json = originalMessage.asJson
    val actualJson = json.toString

    val expectedJson =
      s"""
         |{
         |  "messageType": "transitionFinished",
         |  "clusterFollowupDetails": {
         |    "clusterId": 1,
         |    "clusterStatus": "Stopping"
         |  }
         |}
         |""".stripMargin

    actualJson.filterNot(_.isWhitespace) shouldBe expectedJson.filterNot(_.isWhitespace)

    val decodedMessage = decode[ClusterTransitionFinishedMessage](expectedJson)
    logger.info(s"decodedMessage: ${decodedMessage}")
    decodedMessage.right.get shouldBe originalMessage
  }

  def makeLeoSubscriber(queue: InspectableQueue[IO, Event[LeoPubsubMessage]]) = {
    val googleSubscriber = mock[GoogleSubscriber[IO, LeoPubsubMessage]]

    new LeoPubsubMessageSubscriber[IO](googleSubscriber, clusterHelper, DbSingleton.ref)
  }

}
