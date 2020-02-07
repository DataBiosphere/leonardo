package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.protobuf.Timestamp
import fs2.concurrent.InspectableQueue
import io.circe.parser.decode
import io.circe.syntax._
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.mock._
import org.broadinstitute.dsde.workbench.google2.{Event, GoogleSubscriber}
import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.dao.WelderDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.MockGoogleComputeDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent, clusterQuery, followupQuery}
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubCodec._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError.{ClusterInvalidState, ClusterNotStopped}
import org.broadinstitute.dsde.workbench.leonardo.util.{BucketHelper, ClusterHelper, QueueFactory}
import org.broadinstitute.dsde.workbench.leonardo.{LeonardoTestSuite, ServiceAccountInfo}
import org.mockito.Mockito
import org.scalatest.concurrent._
import org.scalatest.{FlatSpecLike, Matchers}
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.ExecutionContext.Implicits.global

class LeoPubsubMessageSubscriberSpec
    extends TestKit(ActorSystem("leonardotest"))
    with TestComponent
    with FlatSpecLike
    with Matchers
    with MockitoSugar
    with Eventually
    with LeonardoTestSuite {

  val mockWelderDAO = mock[WelderDAO[IO]]
  val mockGoogleDirectoryDAO = new MockGoogleDirectoryDAO()
  val gdDAO = new MockGoogleDataprocDAO
  val computeDAO = new MockGoogleComputeDAO
  val storageDAO = new MockGoogleStorageDAO
  val iamDAO = new MockGoogleIamDAO
  val projectDAO = new MockGoogleProjectDAO
  val authProvider = mock[LeoAuthProvider[IO]]

  val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
    new MockGoogleStorageDAO
  }

  val bucketHelper = new BucketHelper(computeDAO, storageDAO, FakeGoogleStorageService, serviceAccountProvider)

  val clusterHelper = new ClusterHelper(DbSingleton.dbRef,
                                        dataprocConfig,
                                        imageConfig,
                                        googleGroupsConfig,
                                        proxyConfig,
                                        clusterResourcesConfig,
                                        clusterFilesConfig,
                                        monitorConfig,
                                        welderConfig,
                                        bucketHelper,
                                        gdDAO,
                                        computeDAO,
                                        mockGoogleDirectoryDAO,
                                        iamDAO,
                                        projectDAO,
                                        mockWelderDAO,
                                        blocker)

  val runningCluster = makeCluster(1).copy(
    serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount, notebookServiceAccount),
    dataprocInfo = Some(makeDataprocInfo(1).copy(hostIp = None)),
    status = ClusterStatus.Running,
    instances = Set(masterInstance, workerInstance1, workerInstance2)
  )

  val stoppedCluster = makeCluster(2).copy(
    serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount, notebookServiceAccount),
    dataprocInfo = Some(makeDataprocInfo(1).copy(hostIp = None)),
    status = ClusterStatus.Stopped
  )

  "LeoPubsubMessageSubscriber" should "handle StopUpdateMessage and stop cluster" in isolatedDbTest {
    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)

    val savedRunningCluster = runningCluster.save()
    savedRunningCluster shouldEqual runningCluster

    val clusterId = savedRunningCluster.id
    val newMachineConfig = defaultMachineConfig.copy(masterMachineType = Some("n1-standard-8"))
    val message = StopUpdateMessage(newMachineConfig, clusterId, None)

    val followupDetails = ClusterFollowupDetails(clusterId, ClusterStatus.Stopped)

    dbRef
      .inTransaction(
        followupQuery.getFollowupAction(followupDetails)
      )
      .unsafeRunSync() shouldBe None

    leoSubscriber.messageResponder(message).unsafeRunSync()

    val postStorage = dbRef
      .inTransaction(
        followupQuery.getFollowupAction(followupDetails)
      )
      .unsafeRunSync()

    postStorage shouldBe newMachineConfig.masterMachineType

    eventually {
      dbFutureValue { clusterQuery.getClusterById(clusterId) }.get.status shouldBe ClusterStatus.Stopping
    }
  }

  "LeoPubsubMessageSubscriber messageResponder" should "throw an exception if it receives an incorrect cluster transition finished message and the database does not reflect the state in message" in isolatedDbTest {

    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)

    val savedRunningCluster = runningCluster.save()

    val clusterId = savedRunningCluster.id
    val newMasterMachineType = Some("n1-standard-8")
    val followupKey = ClusterFollowupDetails(clusterId, ClusterStatus.Stopped)
    val message = ClusterTransitionFinishedMessage(followupKey, None)

    val preStorage = dbRef.inTransaction(followupQuery.getFollowupAction(followupKey)).unsafeRunSync()
    preStorage shouldBe None

    //save follow-up details in DB
    dbRef
      .inTransaction(
        followupQuery.save(followupKey, newMasterMachineType)
      )
      .unsafeRunSync()

    the[ClusterNotStopped] thrownBy {
      leoSubscriber.messageResponder(message).unsafeRunSync()
    }

    dbFutureValue { clusterQuery.getClusterById(clusterId) }.get.status shouldBe ClusterStatus.Running
  }

  "LeoPubsubMessageSubscriber messageHandler" should "not throw an exception if it receives an incorrect cluster transition finished message and the database does not reflect the state in message" in isolatedDbTest {
    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)
    val savedRunningCluster = runningCluster.save()
    val followupKey = ClusterFollowupDetails(savedRunningCluster.id, ClusterStatus.Stopped)
    val message = ClusterTransitionFinishedMessage(followupKey, None)
    val newMasterMachineType = Some("n1-standard-8")

    val consumer = mock[AckReplyConsumer]
    Mockito.doNothing().when(consumer).ack()

    dbRef
      .inTransaction(
        followupQuery.save(followupKey, newMasterMachineType)
      )
      .unsafeRunSync()

    val process = fs2.Stream(Event[LeoPubsubMessage](message, None, Timestamp.getDefaultInstance ,consumer)) through leoSubscriber.messageHandler
    val res = process.compile.drain

    res.attempt.unsafeRunSync().isRight shouldBe true //messageHandler will never throw exception
  }

  it should "throw an exception if it receives an incorrect StopUpdate message and the database does not reflect the state in message" in isolatedDbTest {

    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)

    val savedStoppedCluster = stoppedCluster.save()

    val clusterId = savedStoppedCluster.id
    val newMachineConfig = defaultMachineConfig.copy(masterMachineType = Some("n1-standard-8"))
    val message = StopUpdateMessage(newMachineConfig, clusterId, None)
    val followupKey = ClusterFollowupDetails(clusterId, ClusterStatus.Stopping)

    dbRef
      .inTransaction(followupQuery.getFollowupAction(followupKey))
      .unsafeRunSync() shouldBe None

    the[ClusterInvalidState] thrownBy {
      leoSubscriber.messageResponder(message).unsafeRunSync()
    }

    dbRef
      .inTransaction(followupQuery.getFollowupAction(followupKey))
      .unsafeRunSync() shouldBe None

    dbFutureValue { clusterQuery.getClusterById(clusterId) }.get.status shouldBe ClusterStatus.Stopped
  }

  "LeoPubsubMessageSubscriber" should "perform a noop when it receives an irrelevant transition for a cluster which has a saved action for a different transition" in isolatedDbTest {
    val savedRunningCluster = runningCluster.save()
    val clusterId = savedRunningCluster.id
    val newMachineType = Some("n1-standard-8")
    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)

    val followupDetails = ClusterFollowupDetails(clusterId, ClusterStatus.Stopped)

    //subscriber has a follow-up when the cluster is stopped
    dbRef
      .inTransaction(
        followupQuery.save(followupDetails, newMachineType)
      )
      .unsafeRunSync()

    //we are notifying the subscriber the cluster has finished creating (aka, noise as far as its concerned)
    val transitionFinishedMessage =
      ClusterTransitionFinishedMessage(ClusterFollowupDetails(clusterId, ClusterStatus.Creating), None)

    leoSubscriber.messageResponder(transitionFinishedMessage).unsafeRunSync()
    val postStorage = dbRef.inTransaction(followupQuery.getFollowupAction(followupDetails)).unsafeRunSync()
    postStorage shouldBe newMachineType

    val cluster = dbFutureValue { clusterQuery.getClusterById(clusterId) }.get
    cluster.status shouldBe ClusterStatus.Running
    cluster.machineConfig shouldBe defaultMachineConfig
  }

  //gracefully handle transition finished with no follow-up action saved
  "LeoPubsubMessageSubscriber" should "perform a no-op when no follow-up action is present for a transition" in isolatedDbTest {
    val savedStoppedCluster = stoppedCluster.save()
    val clusterId = savedStoppedCluster.id
    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)

    val followupKey = ClusterFollowupDetails(clusterId, ClusterStatus.Stopped)
    val transitionFinishedMessage = ClusterTransitionFinishedMessage(followupKey, None)

    leoSubscriber.messageResponder(transitionFinishedMessage).unsafeRunSync()

    val cluster = dbFutureValue { clusterQuery.getClusterById(clusterId) }.get
    cluster.status shouldBe ClusterStatus.Stopped
    cluster.machineConfig shouldBe defaultMachineConfig
  }

  //handle transition finished message with follow-up action saved
  "LeoPubsubMessageSubscriber" should "perform an update when follow-up action is present and the cluster is stopped" in isolatedDbTest {
    val savedStoppedCluster = stoppedCluster.save()
    val clusterId = savedStoppedCluster.id
    val newMachineType = Some("n1-standard-8")
    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)

    val followupDetails = ClusterFollowupDetails(clusterId, ClusterStatus.Stopped)

    dbRef
      .inTransaction(
        followupQuery.save(followupDetails, newMachineType)
      )
      .unsafeRunSync()

    val transitionFinishedMessage = ClusterTransitionFinishedMessage(followupDetails, None)

    leoSubscriber.messageResponder(transitionFinishedMessage).unsafeRunSync()

    //we should consume the followup data and clean up the db
    val postStorage = dbRef.inTransaction(followupQuery.getFollowupAction(followupDetails)).unsafeRunSync()
    postStorage shouldBe None

    eventually {
      dbFutureValue { clusterQuery.getClusterById(clusterId) }.get.status shouldBe ClusterStatus.Starting

      dbFutureValue { clusterQuery.getClusterById(clusterId) }.get.machineConfig shouldBe defaultMachineConfig.copy(
        masterMachineType = newMachineType
      )
    }
  }

  "LeoPubsubCodec" should "encode/decode a StopUpdate message" in isolatedDbTest {
    val originalMessage = StopUpdateMessage(defaultMachineConfig.copy(masterMachineType = Some("n1-standard-8")), 1, None)
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
    decodedMessage.right.get shouldBe originalMessage
  }

  "LeoPubsubCodec" should "encode/decode a ClusterTransitionFinished message" in isolatedDbTest {
    val clusterFollowupDetails = ClusterFollowupDetails(1, ClusterStatus.Stopping)
    val originalMessage = ClusterTransitionFinishedMessage(clusterFollowupDetails, None)
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
    decodedMessage.right.get shouldBe originalMessage
  }

  def makeLeoSubscriber(queue: InspectableQueue[IO, Event[LeoPubsubMessage]]) = {
    val googleSubscriber = mock[GoogleSubscriber[IO, LeoPubsubMessage]]

    new LeoPubsubMessageSubscriber[IO](googleSubscriber, clusterHelper, DbSingleton.dbRef)
  }

}
