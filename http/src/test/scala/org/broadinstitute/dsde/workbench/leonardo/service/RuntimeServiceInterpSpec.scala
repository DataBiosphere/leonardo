package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.IO
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{gceRuntimeConfig, testCluster, _}
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.MockDockerDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, labelQuery, RuntimeConfigQueries, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.http.api.{
  CreateRuntime2Request,
  UpdateRuntimeConfigRequest,
  UpdateRuntimeRequest
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.util.QueueFactory
import org.broadinstitute.dsde.workbench.model
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.scalatest.FlatSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class RuntimeServiceInterpSpec extends FlatSpec with LeonardoTestSuite with TestComponent {
  val publisherQueue = QueueFactory.makePublisherQueue()
  val runtimeService = new RuntimeServiceInterp(
    RuntimeServiceConfig(Config.proxyConfig.proxyUrlBase,
                         imageConfig,
                         autoFreezeConfig,
                         dataprocConfig,
                         Config.gceConfig),
    whitelistAuthProvider,
    serviceAccountProvider,
    new MockDockerDAO,
    FakeGoogleStorageInterpreter,
    publisherQueue
  )
  val emptyCreateRuntimeReq = CreateRuntime2Request(
    Map.empty,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    Set.empty,
    Map.empty
  )

  implicit val ctx: ApplicativeAsk[IO, AppContext] = ApplicativeAsk.const[IO, AppContext](
    AppContext(model.TraceId("traceId"), Instant.now())
  )

  "RuntimeService" should "fail with AuthorizationError if user doesn't have project level permission" in {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("email"), 0)
    val googleProject = GoogleProject("googleProject")

    val res = for {
      r <- runtimeService
        .createRuntime(
          userInfo,
          googleProject,
          RuntimeName("clusterName1"),
          emptyCreateRuntimeReq
        )
        .attempt
    } yield {
      r shouldBe (Left(AuthorizationError(Some(userInfo.userEmail))))
    }
    res.unsafeRunSync()
  }

  it should "successfully create a GCE runtime when no runtime is specified" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed
    val googleProject = GoogleProject("googleProject")
    val runtimeName = RuntimeName("clusterName1")

    val res = for {
      context <- ctx.ask
      r <- runtimeService
        .createRuntime(
          userInfo,
          googleProject,
          runtimeName,
          emptyCreateRuntimeReq
        )
        .attempt
      clusterOpt <- clusterQuery.getActiveClusterByNameMinimal(googleProject, runtimeName).transaction
      cluster = clusterOpt.get
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId).transaction
      message <- publisherQueue.dequeue1
    } yield {
      r shouldBe Right(())
      runtimeConfig shouldBe (Config.gceConfig.runtimeConfigDefaults)
      cluster.googleProject shouldBe (googleProject)
      cluster.runtimeName shouldBe (runtimeName)
      val expectedMessage = CreateRuntimeMessage
        .fromRuntime(cluster, runtimeConfig, Some(context.traceId))
        .copy(
          runtimeImages = Set(
            RuntimeImage(RuntimeImageType.Jupyter, Config.imageConfig.jupyterImage.imageUrl, context.now),
            RuntimeImage(RuntimeImageType.Welder, Config.imageConfig.welderImage.imageUrl, context.now),
            RuntimeImage(RuntimeImageType.Proxy, Config.imageConfig.proxyImage.imageUrl, context.now)
          ),
          scopes = Config.gceConfig.defaultScopes
        )
      message shouldBe expectedMessage
    }
    res.unsafeRunSync()
  }

  it should "successfully create a dataproc runtime when explicitly told so when numberOfWorkers is 0" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed
    val googleProject = GoogleProject("googleProject")
    val runtimeName = RuntimeName("clusterName1")
    val req = emptyCreateRuntimeReq.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.DataprocConfig(
          None,
          None,
          None,
          None,
          None,
          None,
          None,
          Map.empty
        )
      )
    )

    val res = for {
      context <- ctx.ask
      _ <- runtimeService
        .createRuntime(
          userInfo,
          googleProject,
          runtimeName,
          req
        )
        .attempt
      clusterOpt <- clusterQuery.getActiveClusterByNameMinimal(googleProject, runtimeName).transaction
      cluster = clusterOpt.get
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId).transaction
      message <- publisherQueue.dequeue1
    } yield {
      // Default worker is 0, hence all worker configs are None
      val expectedRuntimeConfig = Config.dataprocConfig.runtimeConfigDefaults.copy(
        workerMachineType = None,
        workerDiskSize = None,
        numberOfWorkerLocalSSDs = None,
        numberOfPreemptibleWorkers = None
      )
      runtimeConfig shouldBe expectedRuntimeConfig
      val expectedMessage = CreateRuntimeMessage
        .fromRuntime(cluster, runtimeConfig, Some(context.traceId))
        .copy(
          runtimeImages = Set(
            RuntimeImage(RuntimeImageType.Jupyter, Config.imageConfig.jupyterImage.imageUrl, context.now),
            RuntimeImage(RuntimeImageType.Welder, Config.imageConfig.welderImage.imageUrl, context.now),
            RuntimeImage(RuntimeImageType.Proxy, Config.imageConfig.proxyImage.imageUrl, context.now)
          ),
          scopes = Config.dataprocConfig.defaultScopes
        )
      message shouldBe expectedMessage
    }
    res.unsafeRunSync()
  }

  it should "successfully create a dataproc runtime when explicitly told so when numberOfWorkers is more than 0" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed
    val googleProject = GoogleProject("googleProject")
    val runtimeName = RuntimeName("clusterName1")
    val req = emptyCreateRuntimeReq.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.DataprocConfig(
          Some(2),
          None,
          None,
          None,
          None,
          None,
          None,
          Map.empty
        )
      )
    )

    val res = for {
      context <- ctx.ask
      _ <- runtimeService
        .createRuntime(
          userInfo,
          googleProject,
          runtimeName,
          req
        )
        .attempt
      clusterOpt <- clusterQuery.getActiveClusterByNameMinimal(googleProject, runtimeName).transaction
      cluster = clusterOpt.get
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId).transaction
      message <- publisherQueue.dequeue1
    } yield {
      runtimeConfig shouldBe Config.dataprocConfig.runtimeConfigDefaults.copy(numberOfWorkers = 2)
      val expectedMessage = CreateRuntimeMessage
        .fromRuntime(cluster, runtimeConfig, Some(context.traceId))
        .copy(
          runtimeImages = Set(
            RuntimeImage(RuntimeImageType.Jupyter, Config.imageConfig.jupyterImage.imageUrl, context.now),
            RuntimeImage(RuntimeImageType.Welder, Config.imageConfig.welderImage.imageUrl, context.now),
            RuntimeImage(RuntimeImageType.Proxy, Config.imageConfig.proxyImage.imageUrl, context.now)
          ),
          scopes = Config.dataprocConfig.defaultScopes
        )
      message shouldBe expectedMessage
    }
    res.unsafeRunSync()
  }

  it should "get a runtime" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      internalId <- IO(RuntimeInternalId(UUID.randomUUID.toString))
      testRuntime <- IO(makeCluster(1).copy(internalId = internalId).save())
      getResponse <- runtimeService.getRuntime(userInfo, testRuntime.googleProject, testRuntime.runtimeName)
    } yield {
      getResponse.internalId shouldBe testRuntime.internalId
    }
    res.unsafeRunSync()
  }

  it should "list runtimes" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      internalId1 <- IO(RuntimeInternalId(UUID.randomUUID.toString))
      internalId2 <- IO(RuntimeInternalId(UUID.randomUUID.toString))
      _ <- IO(makeCluster(1).copy(internalId = internalId1).save())
      _ <- IO(makeCluster(2).copy(internalId = internalId2).save())
      listResponse <- runtimeService.listRuntimes(userInfo, None, Map.empty)
    } yield {
      listResponse.map(_.internalId).toSet shouldBe Set(internalId1, internalId2)
    }

    res.unsafeRunSync()
  }

  it should "list runtimes with a project" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      internalId1 <- IO(RuntimeInternalId(UUID.randomUUID.toString))
      internalId2 <- IO(RuntimeInternalId(UUID.randomUUID.toString))
      _ <- IO(makeCluster(1).copy(internalId = internalId1).save())
      _ <- IO(makeCluster(2).copy(internalId = internalId2).save())
      listResponse <- runtimeService.listRuntimes(userInfo, Some(project), Map.empty)
    } yield {
      listResponse.map(_.internalId).toSet shouldBe Set(internalId1, internalId2)
    }

    res.unsafeRunSync()
  }

  it should "list runtimes with parameters" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      internalId1 <- IO(RuntimeInternalId(UUID.randomUUID.toString))
      internalId2 <- IO(RuntimeInternalId(UUID.randomUUID.toString))
      runtime1 <- IO(makeCluster(1).copy(internalId = internalId1).save())
      _ <- IO(makeCluster(2).copy(internalId = internalId2).save())
      _ <- labelQuery.save(runtime1.id, "foo", "bar").transaction
      listResponse <- runtimeService.listRuntimes(userInfo, None, Map("foo" -> "bar"))
    } yield {
      listResponse.map(_.internalId).toSet shouldBe Set(internalId1)
    }

    res.unsafeRunSync()
  }

  it should "delete a runtime" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      context <- ctx.ask
      internalId <- IO(RuntimeInternalId(UUID.randomUUID.toString))
      testRuntime <- IO(makeCluster(1).copy(internalId = internalId).save())

      _ <- runtimeService.deleteRuntime(userInfo, testRuntime.googleProject, testRuntime.runtimeName)
      dbRuntimeOpt <- clusterQuery
        .getActiveClusterByNameMinimal(testRuntime.googleProject, testRuntime.runtimeName)
        .transaction
      dbRuntime = dbRuntimeOpt.get
      message <- publisherQueue.dequeue1
    } yield {
      dbRuntime.status shouldBe RuntimeStatus.Deleting
      val expectedMessage = DeleteRuntimeMessage(testRuntime.id, Some(context.traceId))
      message shouldBe expectedMessage
    }

    res.unsafeRunSync()
  }

  it should "stop a runtime" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      context <- ctx.ask
      internalId <- IO(RuntimeInternalId(UUID.randomUUID.toString))
      testRuntime <- IO(makeCluster(1).copy(internalId = internalId).save())

      _ <- runtimeService.stopRuntime(userInfo, testRuntime.googleProject, testRuntime.runtimeName)
      dbRuntimeOpt <- clusterQuery
        .getActiveClusterByNameMinimal(testRuntime.googleProject, testRuntime.runtimeName)
        .transaction
      dbRuntime = dbRuntimeOpt.get
      message <- publisherQueue.dequeue1
    } yield {
      dbRuntime.status shouldBe RuntimeStatus.Stopping
      val expectedMessage = StopRuntimeMessage(testRuntime.id, Some(context.traceId))
      message shouldBe expectedMessage
    }

    res.unsafeRunSync()
  }

  it should "start a runtime" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      context <- ctx.ask
      internalId <- IO(RuntimeInternalId(UUID.randomUUID.toString))
      testRuntime <- IO(makeCluster(1).copy(internalId = internalId, status = RuntimeStatus.Stopped).save())

      _ <- runtimeService.startRuntime(userInfo, testRuntime.googleProject, testRuntime.runtimeName)
      dbRuntimeOpt <- clusterQuery
        .getActiveClusterByNameMinimal(testRuntime.googleProject, testRuntime.runtimeName)
        .transaction
      dbRuntime = dbRuntimeOpt.get
      message <- publisherQueue.dequeue1
    } yield {
      dbRuntime.status shouldBe RuntimeStatus.Starting
      val expectedMessage = StartRuntimeMessage(testRuntime.id, Some(context.traceId))
      message shouldBe expectedMessage
    }

    res.unsafeRunSync()
  }

  it should "update autopause" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      internalId <- IO(RuntimeInternalId(UUID.randomUUID.toString))
      testRuntime <- IO(makeCluster(1).copy(internalId = internalId, status = RuntimeStatus.Running).save())
      req = UpdateRuntimeRequest(None, false, Some(true), Some(120.minutes))

      _ <- runtimeService.updateRuntime(userInfo, testRuntime.googleProject, testRuntime.runtimeName, req)
      dbRuntimeOpt <- clusterQuery
        .getActiveClusterByNameMinimal(testRuntime.googleProject, testRuntime.runtimeName)
        .transaction
      dbRuntime = dbRuntimeOpt.get
      messageOpt <- publisherQueue.tryDequeue1
    } yield {
      dbRuntime.autopauseThreshold shouldBe 120
      messageOpt shouldBe None
    }

    res.unsafeRunSync()
  }

  List(RuntimeStatus.Creating, RuntimeStatus.Stopping, RuntimeStatus.Deleting, RuntimeStatus.Starting).foreach {
    status =>
      val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed
      it should s"fail to update a runtime in $status status" in isolatedDbTest {
        val res = for {
          internalId <- IO(RuntimeInternalId(UUID.randomUUID.toString))
          testRuntime <- IO(makeCluster(1).copy(internalId = internalId, status = status).save())
          req = UpdateRuntimeRequest(None, false, Some(true), Some(120.minutes))
          fail <- runtimeService
            .updateRuntime(userInfo, testRuntime.googleProject, testRuntime.runtimeName, req)
            .attempt
        } yield {
          fail shouldBe Left(RuntimeCannotBeUpdatedException(testRuntime.projectNameString, testRuntime.status))
        }
        res.unsafeRunSync()
      }
  }

  "RuntimeServiceInterp.processUpdateRuntimeConfigRequest" should "fail to update the wrong cloud service type" in {
    val req = UpdateRuntimeConfigRequest.DataprocConfig(None, Some(DiskSize(100)), None, None)
    val res = for {
      traceId <- traceId.ask
      _ <- runtimeService.processUpdateRuntimeConfigRequest(req, false, testCluster, gceRuntimeConfig, traceId)
    } yield ()
    res.attempt.unsafeRunSync() shouldBe Left(WrongCloudServiceException(CloudService.GCE, CloudService.Dataproc))
  }

  "RuntimeServiceInterp.processUpdateGceConfigRequest" should "not update a GCE runtime when there are no changes" in {
    val req = UpdateRuntimeConfigRequest.GceConfig(Some(gceRuntimeConfig.machineType), Some(gceRuntimeConfig.diskSize))
    val res = for {
      traceId <- traceId.ask
      _ <- runtimeService.processUpdateGceConfigRequest(req, false, testCluster, gceRuntimeConfig, traceId)
      messageOpt <- publisherQueue.tryDequeue1
    } yield {
      messageOpt shouldBe None
    }
    res.unsafeRunSync()
  }

  it should "update a GCE machine type in Stopped state" in {
    val req = UpdateRuntimeConfigRequest.GceConfig(Some(MachineTypeName("n1-micro-2")), None)
    val res = for {
      traceId <- traceId.ask
      _ <- runtimeService.processUpdateGceConfigRequest(req,
                                                        false,
                                                        testCluster.copy(status = RuntimeStatus.Stopped),
                                                        gceRuntimeConfig,
                                                        traceId)
      message <- publisherQueue.dequeue1
    } yield {
      message shouldBe UpdateRuntimeMessage(testCluster.id,
                                            Some(MachineTypeName("n1-micro-2")),
                                            false,
                                            None,
                                            None,
                                            None,
                                            Some(traceId))
    }
    res.unsafeRunSync()
  }

  it should "update a GCE machine type in Running state" in {
    val req = UpdateRuntimeConfigRequest.GceConfig(Some(MachineTypeName("n1-micro-2")), None)
    val res = for {
      traceId <- traceId.ask
      _ <- runtimeService.processUpdateGceConfigRequest(req,
                                                        true,
                                                        testCluster.copy(status = RuntimeStatus.Running),
                                                        gceRuntimeConfig,
                                                        traceId)
      message <- publisherQueue.dequeue1
    } yield {
      message shouldBe UpdateRuntimeMessage(testCluster.id,
                                            Some(MachineTypeName("n1-micro-2")),
                                            true,
                                            None,
                                            None,
                                            None,
                                            Some(traceId))
    }
    res.unsafeRunSync()
  }

  it should "fail to update a GCE machine type in Running state with allowStop set to false" in {
    val req = UpdateRuntimeConfigRequest.GceConfig(Some(MachineTypeName("n1-micro-2")), None)
    val res = for {
      traceId <- traceId.ask
      _ <- runtimeService.processUpdateGceConfigRequest(req,
                                                        false,
                                                        testCluster.copy(status = RuntimeStatus.Running),
                                                        gceRuntimeConfig,
                                                        traceId)
    } yield ()
    res.attempt.unsafeRunSync() shouldBe Left(
      RuntimeMachineTypeCannotBeChangedException(testCluster.copy(status = RuntimeStatus.Running))
    )
  }

  it should "increase the disk on a GCE runtime" in {
    val req = UpdateRuntimeConfigRequest.GceConfig(None, Some(DiskSize(1024)))
    val res = for {
      traceId <- traceId.ask
      _ <- runtimeService.processUpdateGceConfigRequest(req, false, testCluster, gceRuntimeConfig, traceId)
      message <- publisherQueue.dequeue1
    } yield {
      message shouldBe UpdateRuntimeMessage(testCluster.id,
                                            None,
                                            false,
                                            Some(DiskSize(1024)),
                                            None,
                                            None,
                                            Some(traceId))
    }
    res.unsafeRunSync()
  }

  it should "fail to decrease the disk on a GCE runtime" in {
    val req = UpdateRuntimeConfigRequest.GceConfig(None, Some(DiskSize(50)))
    val res = for {
      traceId <- traceId.ask
      _ <- runtimeService.processUpdateGceConfigRequest(req, false, testCluster, gceRuntimeConfig, traceId)
    } yield ()
    res.attempt.unsafeRunSync() shouldBe Left(RuntimeDiskSizeCannotBeDecreasedException(testCluster))
  }

  "RuntimeServiceInterp.processUpdateDataprocConfigRequest" should "not update a Dataproc runtime when there are no changes" in {
    val req = UpdateRuntimeConfigRequest.DataprocConfig(
      Some(defaultDataprocRuntimeConfig.masterMachineType),
      Some(defaultDataprocRuntimeConfig.masterDiskSize),
      Some(defaultDataprocRuntimeConfig.numberOfWorkers),
      defaultDataprocRuntimeConfig.numberOfPreemptibleWorkers
    )
    val res = for {
      traceId <- traceId.ask
      _ <- runtimeService.processUpdateDataprocConfigRequest(req,
                                                             false,
                                                             testCluster,
                                                             defaultDataprocRuntimeConfig,
                                                             traceId)
      messageOpt <- publisherQueue.tryDequeue1
    } yield {
      messageOpt shouldBe None
    }
    res.unsafeRunSync()
  }

  it should "update Dataproc workers and preemptibles" in {
    val req = UpdateRuntimeConfigRequest.DataprocConfig(None, None, Some(50), Some(1000))
    val res = for {
      traceId <- traceId.ask
      _ <- runtimeService.processUpdateDataprocConfigRequest(req,
                                                             false,
                                                             testCluster,
                                                             defaultDataprocRuntimeConfig,
                                                             traceId)
      message <- publisherQueue.dequeue1
    } yield {
      message shouldBe UpdateRuntimeMessage(testCluster.id, None, false, None, Some(50), Some(1000), Some(traceId))
    }
    res.unsafeRunSync()
  }

  it should "update a Dataproc master machine type in Stopped state" in {
    val req = UpdateRuntimeConfigRequest.DataprocConfig(Some(MachineTypeName("n1-micro-2")), None, None, None)
    val res = for {
      traceId <- traceId.ask
      _ <- runtimeService.processUpdateDataprocConfigRequest(req,
                                                             false,
                                                             testCluster.copy(status = RuntimeStatus.Stopped),
                                                             defaultDataprocRuntimeConfig,
                                                             traceId)
      message <- publisherQueue.dequeue1
    } yield {
      message shouldBe UpdateRuntimeMessage(testCluster.id,
                                            Some(MachineTypeName("n1-micro-2")),
                                            false,
                                            None,
                                            None,
                                            None,
                                            Some(traceId))
    }
    res.unsafeRunSync()
  }

  it should "update a Dataproc machine type in Running state" in {
    val req = UpdateRuntimeConfigRequest.DataprocConfig(Some(MachineTypeName("n1-micro-2")), None, None, None)
    val res = for {
      traceId <- traceId.ask
      _ <- runtimeService.processUpdateDataprocConfigRequest(req,
                                                             true,
                                                             testCluster.copy(status = RuntimeStatus.Running),
                                                             defaultDataprocRuntimeConfig,
                                                             traceId)
      message <- publisherQueue.dequeue1
    } yield {
      message shouldBe UpdateRuntimeMessage(testCluster.id,
                                            Some(MachineTypeName("n1-micro-2")),
                                            true,
                                            None,
                                            None,
                                            None,
                                            Some(traceId))
    }
    res.unsafeRunSync()
  }

  it should "fail to update a Dataproc machine type in Running state with allowStop set to false" in {
    val req = UpdateRuntimeConfigRequest.DataprocConfig(Some(MachineTypeName("n1-micro-2")), None, None, None)
    val res = for {
      traceId <- traceId.ask
      _ <- runtimeService.processUpdateDataprocConfigRequest(req,
                                                             false,
                                                             testCluster.copy(status = RuntimeStatus.Running),
                                                             defaultDataprocRuntimeConfig,
                                                             traceId)
    } yield ()
    res.attempt.unsafeRunSync() shouldBe Left(
      RuntimeMachineTypeCannotBeChangedException(testCluster.copy(status = RuntimeStatus.Running))
    )
  }

  it should "fail to update a Dataproc machine type when workers are also updated" in {
    val req = UpdateRuntimeConfigRequest.DataprocConfig(Some(MachineTypeName("n1-micro-2")), None, Some(50), None)
    val res = for {
      traceId <- traceId.ask
      _ <- runtimeService.processUpdateDataprocConfigRequest(req,
                                                             false,
                                                             testCluster.copy(status = RuntimeStatus.Running),
                                                             defaultDataprocRuntimeConfig,
                                                             traceId)
    } yield ()
    res.attempt.unsafeRunSync().isLeft shouldBe true
  }

  it should "increase the disk on a Dataproc runtime" in {
    val req = UpdateRuntimeConfigRequest.DataprocConfig(None, Some(DiskSize(1024)), None, None)
    val res = for {
      traceId <- traceId.ask
      _ <- runtimeService.processUpdateDataprocConfigRequest(req,
                                                             false,
                                                             testCluster,
                                                             defaultDataprocRuntimeConfig,
                                                             traceId)
      message <- publisherQueue.dequeue1
    } yield {
      message shouldBe UpdateRuntimeMessage(testCluster.id,
                                            None,
                                            false,
                                            Some(DiskSize(1024)),
                                            None,
                                            None,
                                            Some(traceId))
    }
    res.unsafeRunSync()
  }

  it should "fail to decrease the disk on a Dataproc runtime" in {
    val req = UpdateRuntimeConfigRequest.DataprocConfig(None, Some(DiskSize(50)), None, None)
    val res = for {
      traceId <- traceId.ask
      _ <- runtimeService.processUpdateDataprocConfigRequest(req,
                                                             false,
                                                             testCluster,
                                                             defaultDataprocRuntimeConfig,
                                                             traceId)
    } yield ()
    res.attempt.unsafeRunSync() shouldBe Left(RuntimeDiskSizeCannotBeDecreasedException(testCluster))
  }

}
