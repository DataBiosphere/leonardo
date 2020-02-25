package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant

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
import org.broadinstitute.dsde.workbench.leonardo.db.{
  clusterQuery,
  followupQuery,
  RuntimeConfigId,
  RuntimeConfigQueries,
  TestComponent
}
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubCodec._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError.{
  ClusterInvalidState,
  ClusterNotStopped
}
import org.broadinstitute.dsde.workbench.leonardo.util.{BucketHelper, ClusterHelper, QueueFactory}
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
  val currentTime = Instant.now

  val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
    new MockGoogleStorageDAO
  }

  val bucketHelper = new BucketHelper(computeDAO, storageDAO, FakeGoogleStorageService, serviceAccountProvider)

  val clusterHelper = new ClusterHelper(dataprocConfig,
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
    savedRunningCluster.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual runningCluster

    val clusterId = savedRunningCluster.id
    val newMachineConfig = defaultRuntimeConfig.copy(masterMachineType = "n1-standard-8")
    val message = StopUpdate(newMachineConfig, clusterId, None)

    val followupDetails = ClusterFollowupDetails(clusterId, ClusterStatus.Stopped)

    dbRef
      .inTransaction(
        followupQuery.getFollowupAction(followupDetails)
      )
      .unsafeRunSync() shouldBe None

    leoSubscriber.messageResponder(message, currentTime).unsafeRunSync()

    val postStorage = dbRef
      .inTransaction(
        followupQuery.getFollowupAction(followupDetails)
      )
      .unsafeRunSync()

    postStorage.get.value shouldBe newMachineConfig.masterMachineType

    eventually {
      dbFutureValue { clusterQuery.getClusterById(clusterId) }.get.status shouldBe ClusterStatus.Stopping
    }
  }

  "LeoPubsubMessageSubscriber messageResponder" should "throw an exception if it receives an incorrect cluster transition finished message and the database does not reflect the state in message" in isolatedDbTest {

    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)

    val savedRunningCluster = runningCluster.save()

    val clusterId = savedRunningCluster.id
    val newMasterMachineType = MachineType("n1-standard-8")
    val followupKey = ClusterFollowupDetails(clusterId, ClusterStatus.Stopped)
    val message = ClusterTransition(followupKey, None)

    val preStorage = dbRef.inTransaction(followupQuery.getFollowupAction(followupKey)).unsafeRunSync()
    preStorage shouldBe None

    //save follow-up details in DB
    dbRef
      .inTransaction(
        followupQuery.save(followupKey, Some(newMasterMachineType))
      )
      .unsafeRunSync()

    the[ClusterNotStopped] thrownBy {
      leoSubscriber.messageResponder(message, currentTime).unsafeRunSync()
    }

    dbFutureValue { clusterQuery.getClusterById(clusterId) }.get.status shouldBe ClusterStatus.Running
  }

  "LeoPubsubMessageSubscriber messageHandler" should "not throw an exception if it receives an incorrect cluster transition finished message and the database does not reflect the state in message" in isolatedDbTest {
    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)
    val savedRunningCluster = runningCluster.save()
    val followupKey = ClusterFollowupDetails(savedRunningCluster.id, ClusterStatus.Stopped)
    val message = ClusterTransition(followupKey, None)
    val newMasterMachineType = MachineType("n1-standard-8")

    val consumer = mock[AckReplyConsumer]
    Mockito.doNothing().when(consumer).ack()

    dbRef
      .inTransaction(
        followupQuery.save(followupKey, Some(newMasterMachineType))
      )
      .unsafeRunSync()

    val process = fs2.Stream(Event[LeoPubsubMessage](message, None, Timestamp.getDefaultInstance, consumer)) through leoSubscriber.messageHandler
    val res = process.compile.drain

    res.attempt.unsafeRunSync().isRight shouldBe true //messageHandler will never throw exception
  }

  it should "throw an exception if it receives an incorrect StopUpdate message and the database does not reflect the state in message" in isolatedDbTest {

    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)

    val savedStoppedCluster = stoppedCluster.save()

    val clusterId = savedStoppedCluster.id
    val newMachineConfig = defaultRuntimeConfig.copy(masterMachineType = "n1-standard-8")
    val message = StopUpdate(newMachineConfig, clusterId, None)
    val followupKey = ClusterFollowupDetails(clusterId, ClusterStatus.Stopping)

    dbRef
      .inTransaction(followupQuery.getFollowupAction(followupKey))
      .unsafeRunSync() shouldBe None

    the[ClusterInvalidState] thrownBy {
      leoSubscriber.messageResponder(message, currentTime).unsafeRunSync()
    }

    dbRef
      .inTransaction(followupQuery.getFollowupAction(followupKey))
      .unsafeRunSync() shouldBe None

    dbFutureValue { clusterQuery.getClusterById(clusterId) }.get.status shouldBe ClusterStatus.Stopped
  }

  "LeoPubsubMessageSubscriber" should "perform a noop when it receives an irrelevant transition for a cluster which has a saved action for a different transition" in isolatedDbTest {
    val savedRunningCluster = runningCluster.save()
    val clusterId = savedRunningCluster.id
    val newMachineType = MachineType("n1-standard-8")
    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)

    val followupDetails = ClusterFollowupDetails(clusterId, ClusterStatus.Stopped)

    //subscriber has a follow-up when the cluster is stopped
    dbRef
      .inTransaction(
        followupQuery.save(followupDetails, Some(newMachineType))
      )
      .unsafeRunSync()

    //we are notifying the subscriber the cluster has finished creating (aka, noise as far as its concerned)
    val transitionFinishedMessage =
      ClusterTransition(ClusterFollowupDetails(clusterId, ClusterStatus.Creating), None)

    leoSubscriber.messageResponder(transitionFinishedMessage, currentTime).unsafeRunSync()
    val postStorage = dbRef.inTransaction(followupQuery.getFollowupAction(followupDetails)).unsafeRunSync()
    postStorage shouldBe Some(newMachineType)

    val cluster = dbFutureValue { clusterQuery.getClusterById(clusterId) }.get
    cluster.status shouldBe ClusterStatus.Running
    dbFutureValue(RuntimeConfigQueries.getRuntime(cluster.runtimeConfigId)) shouldBe defaultRuntimeConfig
  }

  //gracefully handle transition finished with no follow-up action saved
  "LeoPubsubMessageSubscriber" should "perform a no-op when no follow-up action is present for a transition" in isolatedDbTest {
    val savedStoppedCluster = stoppedCluster.save()
    val clusterId = savedStoppedCluster.id
    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)

    val followupKey = ClusterFollowupDetails(clusterId, ClusterStatus.Stopped)
    val transitionFinishedMessage = ClusterTransition(followupKey, None)

    leoSubscriber.messageResponder(transitionFinishedMessage, currentTime).unsafeRunSync()

    val cluster = dbFutureValue { clusterQuery.getClusterById(clusterId) }.get
    cluster.status shouldBe ClusterStatus.Stopped
    dbFutureValue { RuntimeConfigQueries.getRuntime(cluster.runtimeConfigId) } shouldBe defaultRuntimeConfig
  }

  //handle transition finished message with follow-up action saved
  "LeoPubsubMessageSubscriber" should "perform an update when follow-up action is present and the cluster is stopped" in isolatedDbTest {
    val savedStoppedCluster = stoppedCluster.save()
    val cluster = savedStoppedCluster
    val newMachineType = MachineType("n1-standard-8")
    val queue = QueueFactory.makeSubscriberQueue()
    val leoSubscriber = makeLeoSubscriber(queue)

    val followupDetails = ClusterFollowupDetails(cluster.id, ClusterStatus.Stopped)

    dbRef
      .inTransaction(followupQuery.save(followupDetails, Some(newMachineType)))
      .unsafeRunSync()

    val transitionFinishedMessage = ClusterTransition(followupDetails, None)

    leoSubscriber.messageResponder(transitionFinishedMessage, currentTime).unsafeRunSync()

    //we should consume the followup data and clean up the db
    val postStorage = dbRef.inTransaction(followupQuery.getFollowupAction(followupDetails)).unsafeRunSync()
    postStorage shouldBe None

    eventually {
      dbFutureValue { clusterQuery.getClusterById(cluster.id) }.get.status shouldBe ClusterStatus.Starting

      dbFutureValue { RuntimeConfigQueries.getRuntime(cluster.runtimeConfigId) } shouldBe defaultRuntimeConfig.copy(
        masterMachineType = newMachineType.value
      )
    }
  }

  "LeoPubsubCodec" should "encode/decode a StopUpdate message" in isolatedDbTest {
    val originalMessage = StopUpdate(defaultRuntimeConfig.copy(masterMachineType = "n1-standard-8"), 1, None)
    val json = originalMessage.asJson
    val actualJsonString = json.noSpaces

    val expectedJsonString =
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
         |    "numberOfPreemptibleWorkers": null,
         |     "cloudService": "DATAPROC"
         |  },
         |  "clusterId": 1
         |}
         |""".stripMargin

    actualJsonString shouldBe expectedJsonString.filterNot(_.isWhitespace)

    val decodedMessage = decode[StopUpdate](expectedJsonString)
    decodedMessage.right.get shouldBe originalMessage
  }

  "LeoPubsubCodec" should "encode/decode a ClusterTransitionFinished message" in isolatedDbTest {
    val clusterFollowupDetails = ClusterFollowupDetails(1, ClusterStatus.Stopping)
    val originalMessage = ClusterTransition(clusterFollowupDetails, None)
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

    val decodedMessage = decode[ClusterTransition](expectedJson)
    decodedMessage.right.get shouldBe originalMessage
  }

  def makeLeoSubscriber(queue: InspectableQueue[IO, Event[LeoPubsubMessage]]) = {
    val googleSubscriber = mock[GoogleSubscriber[IO, LeoPubsubMessage]]

    new LeoPubsubMessageSubscriber[IO](googleSubscriber, clusterHelper)
  }

}
