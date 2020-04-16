package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.protobuf.Timestamp
import io.circe.parser.decode
import io.circe.syntax._
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.mock._
import org.broadinstitute.dsde.workbench.google2.{Event, GoogleSubscriber, MachineTypeName}
import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.VM
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.WelderDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.MockGoogleComputeService
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, patchQuery, RuntimeConfigQueries, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubCodec._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError.{
  ClusterInvalidState,
  ClusterNotStopped
}
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.mockito.Mockito
import org.scalatest.concurrent._
import org.scalatest.{FlatSpecLike, Matchers}
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Left

class LeoPubsubMessageSubscriberSpec
    extends TestKit(ActorSystem("leonardotest"))
    with FlatSpecLike
    with TestComponent
    with Matchers
    with MockitoSugar
    with Eventually
    with LeonardoTestSuite {

  val mockWelderDAO = mock[WelderDAO[IO]]
  val mockGoogleDirectoryDAO = new MockGoogleDirectoryDAO()
  val gdDAO = new MockGoogleDataprocDAO
  val storageDAO = new MockGoogleStorageDAO
  val iamDAO = new MockGoogleIamDAO
  val projectDAO = new MockGoogleProjectDAO
  val authProvider = mock[LeoAuthProvider[IO]]
  val currentTime = Instant.now
  implicit val appContext: ApplicativeAsk[IO, AppContext] =
    ApplicativeAsk.const(AppContext.generate[IO].unsafeRunSync())

  val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
    new MockGoogleStorageDAO
  }

  val bucketHelperConfig =
    BucketHelperConfig(imageConfig, welderConfig, proxyConfig, clusterFilesConfig, clusterResourcesConfig)
  val bucketHelper =
    new BucketHelper[IO](bucketHelperConfig,
                         MockGoogleComputeService,
                         FakeGoogleStorageService,
                         projectDAO,
                         serviceAccountProvider,
                         blocker)

  val vpcInterp = new VPCInterpreter[IO](Config.vpcInterpreterConfig, projectDAO, MockGoogleComputeService)

  val dataprocInterp = new DataprocInterpreter[IO](Config.dataprocInterpreterConfig,
                                                   bucketHelper,
                                                   vpcInterp,
                                                   gdDAO,
                                                   MockGoogleComputeService,
                                                   mockGoogleDirectoryDAO,
                                                   iamDAO,
                                                   projectDAO,
                                                   mockWelderDAO,
                                                   blocker)
  val gceInterp = new GceInterpreter[IO](Config.gceInterpreterConfig,
                                         bucketHelper,
                                         vpcInterp,
                                         MockGoogleComputeService,
                                         mockWelderDAO,
                                         blocker)
  implicit val runtimeInstances = new RuntimeInstances[IO](dataprocInterp, gceInterp)

  val runningCluster = makeCluster(1).copy(
    serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount, notebookServiceAccount),
    asyncRuntimeFields = Some(makeAsyncRuntimeFields(1).copy(hostIp = None)),
    status = RuntimeStatus.Running,
    dataprocInstances = Set(masterInstance, workerInstance1, workerInstance2)
  )

  val stoppedCluster = makeCluster(2).copy(
    serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount, notebookServiceAccount),
    asyncRuntimeFields = Some(makeAsyncRuntimeFields(1).copy(hostIp = None)),
    status = RuntimeStatus.Stopped
  )

  it should "handle StopUpdateMessage and stop cluster" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val savedRunningCluster = runningCluster.save()
    savedRunningCluster.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual runningCluster

    val clusterId = savedRunningCluster.id
    val newMachineConfig = defaultDataprocRuntimeConfig.copy(masterMachineType = MachineTypeName("n1-standard-8"))
    val message = StopUpdateMessage(newMachineConfig, clusterId, None)

    val patchDetails = RuntimePatchDetails(clusterId, RuntimeStatus.Stopped)

    dbRef
      .inTransaction(
        patchQuery.getPatchAction(patchDetails.runtimeId)
      )
      .unsafeRunSync() shouldBe None

    leoSubscriber.messageResponder(message, currentTime).unsafeRunSync()

    eventually {
      dbFutureValue(clusterQuery.getClusterById(clusterId)).get.status shouldBe RuntimeStatus.Stopping
    }
  }

  it should "handle CreateRuntimeMessage and create cluster" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      runtime <- IO(
        makeCluster(1)
          .copy(asyncRuntimeFields = None,
                status = RuntimeStatus.Creating,
                serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount, None))
          .save()
      )
      tr <- traceId.ask
      now <- IO(Instant.now)
      _ <- leoSubscriber.messageResponder(CreateRuntimeMessage.fromRuntime(runtime, gceRuntimeConfig, Some(tr)), now)
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
    } yield {
      updatedRuntime shouldBe 'defined
      updatedRuntime.get.asyncRuntimeFields shouldBe 'defined
      updatedRuntime.get.asyncRuntimeFields.get.stagingBucket.value should startWith("leostaging")
      updatedRuntime.get.asyncRuntimeFields.get.hostIp shouldBe None
      updatedRuntime.get.asyncRuntimeFields.get.operationName.value shouldBe "opName"
      updatedRuntime.get.asyncRuntimeFields.get.googleId.value shouldBe "target"
      updatedRuntime.get.runtimeImages.map(_.imageType) should contain(VM)
    }

    res.unsafeRunSync()
  }

  it should "handle DeleteRuntimeMessage and delete cluster" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      now <- IO(Instant.now)
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Deleting).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask

      _ <- leoSubscriber.messageResponder(DeleteRuntimeMessage(runtime.id, Some(tr)), now)
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
    } yield {
      updatedRuntime shouldBe 'defined
      updatedRuntime.get.status shouldBe RuntimeStatus.Deleting
    }

    res.unsafeRunSync()
  }

  it should "not handle DeleteRuntimeMessage when cluster is not in Deleting status" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      now <- IO(Instant.now)
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask
      message = DeleteRuntimeMessage(runtime.id, Some(tr))
      attempt <- leoSubscriber.messageResponder(message, now).attempt
    } yield {
      attempt shouldBe Left(ClusterInvalidState(runtime.id, runtime.projectNameString, runtime, message))
    }

    res.unsafeRunSync()
  }

  it should "handle StopRuntimeMessage and stop cluster" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      now <- IO(Instant.now)
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Stopping).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask

      _ <- leoSubscriber.messageResponder(StopRuntimeMessage(runtime.id, Some(tr)), now)
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
    } yield {
      updatedRuntime shouldBe 'defined
      updatedRuntime.get.status shouldBe RuntimeStatus.Stopping
    }

    res.unsafeRunSync()
  }

  it should "not handle StopRuntimeMessage when cluster is not in Stopping status" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      now <- IO(Instant.now)
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask
      message = StopRuntimeMessage(runtime.id, Some(tr))
      attempt <- leoSubscriber.messageResponder(message, now).attempt
    } yield {
      attempt shouldBe Left(ClusterInvalidState(runtime.id, runtime.projectNameString, runtime, message))
    }

    res.unsafeRunSync()
  }

  it should "handle StartRuntimeMessage and start cluster" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      now <- IO(Instant.now)
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Starting).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask

      _ <- leoSubscriber.messageResponder(StartRuntimeMessage(runtime.id, Some(tr)), now)
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
    } yield {
      updatedRuntime shouldBe 'defined
      updatedRuntime.get.status shouldBe RuntimeStatus.Starting
    }

    res.unsafeRunSync()
  }

  it should "not handle StartRuntimeMessage when cluster is not in Starting status" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      now <- IO(Instant.now)
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask
      message = StartRuntimeMessage(runtime.id, Some(tr))
      attempt <- leoSubscriber.messageResponder(message, now).attempt
    } yield {
      attempt shouldBe Left(ClusterInvalidState(runtime.id, runtime.projectNameString, runtime, message))
    }

    res.unsafeRunSync()
  }

  it should "handle UpdateRuntimeMessage and stop the cluster when there's a machine type change" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      now <- IO(Instant.now)
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask

      _ <- leoSubscriber.messageResponder(
        UpdateRuntimeMessage(runtime.id, Some(MachineTypeName("n1-highmem-64")), true, None, None, None, Some(tr)),
        now
      )
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
    } yield {
      updatedRuntime shouldBe 'defined
      updatedRuntime.get.status shouldBe RuntimeStatus.Stopping
    }

    res.unsafeRunSync()
  }

  it should "handle UpdateRuntimeMessage and update the runtime config in the database" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      now <- IO(Instant.now)
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Stopped).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask

      _ <- leoSubscriber.messageResponder(UpdateRuntimeMessage(runtime.id,
                                                               Some(MachineTypeName("n1-highmem-64")),
                                                               false,
                                                               Some(DiskSize(1024)),
                                                               None,
                                                               None,
                                                               Some(tr)),
                                          now)
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
      updatedRuntimeConfig <- updatedRuntime.traverse(r =>
        RuntimeConfigQueries.getRuntimeConfig(r.runtimeConfigId).transaction
      )
    } yield {
      updatedRuntime shouldBe 'defined
      updatedRuntime.get.status shouldBe RuntimeStatus.Stopped
      updatedRuntimeConfig shouldBe 'defined
      updatedRuntimeConfig.get.machineType shouldBe MachineTypeName("n1-highmem-64")
      updatedRuntimeConfig.get.diskSize shouldBe DiskSize(1024)
    }

    res.unsafeRunSync()
  }

  "LeoPubsubMessageSubscriber messageResponder" should "throw an exception if it receives an incorrect cluster transition finished message and the database does not reflect the state in message" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val savedRunningCluster = runningCluster.save()

    val clusterId = savedRunningCluster.id
    val newMasterMachineType = MachineTypeName("n1-standard-8")
    val patchKey = RuntimePatchDetails(clusterId, RuntimeStatus.Stopped)
    val message = RuntimeTransitionMessage(patchKey, None)

    val preStorage = dbRef.inTransaction(patchQuery.getPatchAction(patchKey.runtimeId)).unsafeRunSync()
    preStorage shouldBe None

    //save follow-up details in DB
    dbRef
      .inTransaction(
        patchQuery.save(patchKey, Some(newMasterMachineType))
      )
      .unsafeRunSync()

    the[ClusterNotStopped] thrownBy {
      leoSubscriber.messageResponder(message, currentTime).unsafeRunSync()
    }

    dbFutureValue(clusterQuery.getClusterById(clusterId)).get.status shouldBe RuntimeStatus.Running
  }

  "LeoPubsubMessageSubscriber messageHandler" should "not throw an exception if it receives an incorrect cluster transition finished message and the database does not reflect the state in message" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()
    val savedRunningCluster = runningCluster.save()
    val patchKey = RuntimePatchDetails(savedRunningCluster.id, RuntimeStatus.Stopped)
    val message = RuntimeTransitionMessage(patchKey, None)
    val newMasterMachineType = MachineTypeName("n1-standard-8")

    val consumer = mock[AckReplyConsumer]
    Mockito.doNothing().when(consumer).ack()

    dbRef
      .inTransaction(
        patchQuery.save(patchKey, Some(newMasterMachineType))
      )
      .unsafeRunSync()

    val process =
      fs2.Stream(Event[LeoPubsubMessage](message, None, Timestamp.getDefaultInstance, consumer)) through leoSubscriber.messageHandler
    val res = process.compile.drain

    res.attempt.unsafeRunSync().isRight shouldBe true //messageHandler will never throw exception
  }

  it should "throw an exception if it receives an incorrect StopUpdate message and the database does not reflect the state in message" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val savedStoppedCluster = stoppedCluster.save()

    val clusterId = savedStoppedCluster.id
    val newMachineConfig = defaultDataprocRuntimeConfig.copy(masterMachineType = MachineTypeName("n1-standard-8"))
    val message = StopUpdateMessage(newMachineConfig, clusterId, None)
    val patchKey = RuntimePatchDetails(clusterId, RuntimeStatus.Stopping)

    dbRef
      .inTransaction(patchQuery.getPatchAction(patchKey.runtimeId))
      .unsafeRunSync() shouldBe None

    the[ClusterInvalidState] thrownBy {
      leoSubscriber.messageResponder(message, currentTime).unsafeRunSync()
    }

    dbRef
      .inTransaction(patchQuery.getPatchAction(patchKey.runtimeId))
      .unsafeRunSync() shouldBe None

    dbFutureValue(clusterQuery.getClusterById(clusterId)).get.status shouldBe RuntimeStatus.Stopped
  }

  "LeoPubsubMessageSubscriber" should "perform a noop when it receives an irrelevant transition for a cluster which has a saved action for a different transition" in isolatedDbTest {
    val savedRunningCluster = runningCluster.save()
    val clusterId = savedRunningCluster.id
    val newMachineType = MachineTypeName("n1-standard-8")
    val leoSubscriber = makeLeoSubscriber()

    val patchDetails = RuntimePatchDetails(clusterId, RuntimeStatus.Stopped)

    //subscriber has a follow-up when the cluster is stopped
    dbRef
      .inTransaction(
        patchQuery.save(patchDetails, Some(newMachineType))
      )
      .unsafeRunSync()

    //we are notifying the subscriber the cluster has finished creating (aka, noise as far as its concerned)
    val transitionFinishedMessage =
      RuntimeTransitionMessage(RuntimePatchDetails(clusterId, RuntimeStatus.Creating), None)

    leoSubscriber.messageResponder(transitionFinishedMessage, currentTime).unsafeRunSync()
    val postStorage = dbRef.inTransaction(patchQuery.getPatchAction(patchDetails.runtimeId)).unsafeRunSync()
    postStorage shouldBe Some(newMachineType)

    val cluster = dbFutureValue(clusterQuery.getClusterById(clusterId)).get
    cluster.status shouldBe RuntimeStatus.Running
    dbFutureValue(RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId)) shouldBe defaultDataprocRuntimeConfig
  }

  //gracefully handle transition finished with no follow-up action saved
  "LeoPubsubMessageSubscriber" should "perform a no-op when no follow-up action is present for a transition" in isolatedDbTest {
    val savedStoppedCluster = stoppedCluster.save()
    val clusterId = savedStoppedCluster.id
    val leoSubscriber = makeLeoSubscriber()

    val patchKey = RuntimePatchDetails(clusterId, RuntimeStatus.Stopped)
    val transitionFinishedMessage = RuntimeTransitionMessage(patchKey, None)

    leoSubscriber.messageResponder(transitionFinishedMessage, currentTime).unsafeRunSync()

    val cluster = dbFutureValue(clusterQuery.getClusterById(clusterId)).get
    cluster.status shouldBe RuntimeStatus.Stopped
    dbFutureValue(RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId)) shouldBe defaultDataprocRuntimeConfig
  }

  //handle transition finished message with follow-up action saved
  "LeoPubsubMessageSubscriber" should "perform an update when follow-up action is present and the cluster is stopped" in isolatedDbTest {
    val savedStoppedCluster = stoppedCluster.save()
    val cluster = savedStoppedCluster
    val newMachineType = MachineTypeName("n1-standard-8")
    val leoSubscriber = makeLeoSubscriber()

    val patchDetails = RuntimePatchDetails(cluster.id, RuntimeStatus.Stopped)

    dbRef
      .inTransaction(patchQuery.save(patchDetails, Some(newMachineType)))
      .unsafeRunSync()

    val transitionFinishedMessage = RuntimeTransitionMessage(patchDetails, None)

    leoSubscriber.messageResponder(transitionFinishedMessage, currentTime).unsafeRunSync()

    //we should consume the followup data

    eventually {
      dbFutureValue(clusterQuery.getClusterById(cluster.id)).get.status shouldBe RuntimeStatus.Starting

      dbFutureValue(RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId)) shouldBe defaultDataprocRuntimeConfig
        .copy(
          masterMachineType = newMachineType
        )
    }
  }

  "LeoPubsubCodec" should "encode/decode a StopUpdate message" in isolatedDbTest {
    val originalMessage =
      StopUpdateMessage(defaultDataprocRuntimeConfig.copy(masterMachineType = MachineTypeName("n1-standard-8")),
                        1,
                        None)
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

    val decodedMessage = decode[StopUpdateMessage](expectedJsonString)
    decodedMessage.right.get shouldBe originalMessage
  }

  "LeoPubsubCodec" should "encode/decode a ClusterTransitionFinished message" in isolatedDbTest {
    val clusterPatchDetails = RuntimePatchDetails(1, RuntimeStatus.Stopping)
    val originalMessage = RuntimeTransitionMessage(clusterPatchDetails, None)
    val json = originalMessage.asJson
    val actualJson = json.toString

    val expectedJson =
      s"""
         |{
         |  "messageType": "transitionFinished",
         |  "clusterPatchDetails": {
         |    "clusterId": 1,
         |    "clusterStatus": "Stopping"
         |  }
         |}
         |""".stripMargin

    actualJson.filterNot(_.isWhitespace) shouldBe expectedJson.filterNot(_.isWhitespace)

    val decodedMessage = decode[RuntimeTransitionMessage](expectedJson)
    decodedMessage.right.get shouldBe originalMessage
  }

  def makeLeoSubscriber() = {
    val googleSubscriber = mock[GoogleSubscriber[IO, LeoPubsubMessage]]

    new LeoPubsubMessageSubscriber[IO](googleSubscriber, MockGceRuntimeMonitor)
  }

}
