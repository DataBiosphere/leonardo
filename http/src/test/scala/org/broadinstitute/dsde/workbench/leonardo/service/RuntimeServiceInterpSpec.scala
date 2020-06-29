package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.IO
import cats.mtl.ApplicativeAsk
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{gceRuntimeConfig, testCluster, userInfo, _}
import org.broadinstitute.dsde.workbench.leonardo.SamResource.RuntimeSamResource
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.MockDockerDAO
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.api.{UpdateRuntimeConfigRequest, UpdateRuntimeRequest}
import org.broadinstitute.dsde.workbench.leonardo.http.service.RuntimeServiceInterp.PersistentDiskRequestResult
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.util.QueueFactory
import org.broadinstitute.dsde.workbench.model
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.scalatest.Assertion

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

class RuntimeServiceInterpSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent with MockitoSugar {
  val publisherQueue = QueueFactory.makePublisherQueue()
  def makeRuntimeService(publisherQueue: InspectableQueue[IO, LeoPubsubMessage]) =
    new RuntimeServiceInterp(
      RuntimeServiceConfig(Config.proxyConfig.proxyUrlBase,
                           imageConfig,
                           autoFreezeConfig,
                           Config.zombieRuntimeMonitorConfig,
                           dataprocConfig,
                           Config.gceConfig),
      Config.persistentDiskConfig,
      whitelistAuthProvider,
      serviceAccountProvider,
      new MockDockerDAO,
      FakeGoogleStorageInterpreter,
      publisherQueue
    )
  val runtimeService = makeRuntimeService(publisherQueue)
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

  it should "create a runtime with a disk config" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed
    val persistentDisk = PersistentDiskRequest(
      diskName,
      Some(DiskSize(500)),
      None,
      Map.empty
    )
    val req = emptyCreateRuntimeReq.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.GceWithPdConfig(machineType = Some(MachineTypeName("n1-standard-4")), persistentDisk)
      )
    )

    val res = for {
      context <- ctx.ask
      r <- runtimeService
        .createRuntime(
          userInfo,
          project,
          name0,
          req
        )
        .attempt
      runtimeOpt <- clusterQuery.getActiveClusterByNameMinimal(project, name0).transaction
      runtime = runtimeOpt.get
      diskOpt <- persistentDiskQuery.getActiveByName(project, diskName).transaction
      disk = diskOpt.get
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      message <- publisherQueue.dequeue1
    } yield {
      r shouldBe Right(())
      runtime.googleProject shouldBe project
      runtime.runtimeName shouldBe name0
      runtimeConfig.asInstanceOf[RuntimeConfig.GceWithPdConfig].persistentDiskId shouldBe Some(disk.id)
      disk.googleProject shouldBe project
      disk.name shouldBe diskName
      disk.size shouldBe DiskSize(500)
      runtimeConfig shouldBe RuntimeConfig.GceWithPdConfig(
        MachineTypeName("n1-standard-4"),
        Some(disk.id),
        bootDiskSize = DiskSize(50)
      ) //TODO: this is a problem in terms of inconsistency
      val expectedMessage = CreateRuntimeMessage
        .fromRuntime(runtime, runtimeConfig, Some(context.traceId))
        .copy(
          runtimeImages = Set(
            RuntimeImage(RuntimeImageType.Jupyter, Config.imageConfig.jupyterImage.imageUrl, context.now),
            RuntimeImage(RuntimeImageType.Welder, Config.imageConfig.welderImage.imageUrl, context.now),
            RuntimeImage(RuntimeImageType.Proxy, Config.imageConfig.proxyImage.imageUrl, context.now)
          ),
          scopes = Config.gceConfig.defaultScopes,
          runtimeConfig =
            RuntimeConfig.GceWithPdConfig(runtimeConfig.machineType, Some(disk.id), bootDiskSize = DiskSize(50))
        )
      message shouldBe expectedMessage
    }

    res.unsafeRunSync()
  }

  it should "get a runtime" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      samResource <- IO(RuntimeSamResource(UUID.randomUUID.toString))
      testRuntime <- IO(makeCluster(1).copy(samResource = samResource).save())
      getResponse <- runtimeService.getRuntime(userInfo, testRuntime.googleProject, testRuntime.runtimeName)
    } yield {
      getResponse.samResource shouldBe testRuntime.samResource
    }
    res.unsafeRunSync()
  }

  it should "list runtimes" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      samResource1 <- IO(RuntimeSamResource(UUID.randomUUID.toString))
      samResource2 <- IO(RuntimeSamResource(UUID.randomUUID.toString))
      _ <- IO(makeCluster(1).copy(samResource = samResource1).save())
      _ <- IO(makeCluster(2).copy(samResource = samResource2).save())
      listResponse <- runtimeService.listRuntimes(userInfo, None, Map.empty)
    } yield {
      listResponse.map(_.samResource).toSet shouldBe Set(samResource1, samResource2)
    }

    res.unsafeRunSync()
  }

  it should "list runtimes with a project" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      samResource1 <- IO(RuntimeSamResource(UUID.randomUUID.toString))
      samResource2 <- IO(RuntimeSamResource(UUID.randomUUID.toString))
      _ <- IO(makeCluster(1).copy(samResource = samResource1).save())
      _ <- IO(makeCluster(2).copy(samResource = samResource2).save())
      listResponse <- runtimeService.listRuntimes(userInfo, Some(project), Map.empty)
    } yield {
      listResponse.map(_.samResource).toSet shouldBe Set(samResource1, samResource2)
    }

    res.unsafeRunSync()
  }

  it should "list runtimes with parameters" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      samResource1 <- IO(RuntimeSamResource(UUID.randomUUID.toString))
      samResource2 <- IO(RuntimeSamResource(UUID.randomUUID.toString))
      runtime1 <- IO(makeCluster(1).copy(samResource = samResource1).save())
      _ <- IO(makeCluster(2).copy(samResource = samResource2).save())
      _ <- labelQuery.save(runtime1.id, LabelResourceType.Runtime, "foo", "bar").transaction
      listResponse <- runtimeService.listRuntimes(userInfo, None, Map("foo" -> "bar"))
    } yield {
      listResponse.map(_.samResource).toSet shouldBe Set(samResource1)
    }

    res.unsafeRunSync()
  }

  it should "delete a runtime" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      publisherQueue <- InspectableQueue.bounded[IO, LeoPubsubMessage](10)
      service = makeRuntimeService(publisherQueue)
      samResource <- IO(RuntimeSamResource(UUID.randomUUID.toString))
      testRuntime <- IO(makeCluster(1).copy(samResource = samResource).save())

      _ <- service.deleteRuntime(
        DeleteRuntimeRequest(userInfo, testRuntime.googleProject, testRuntime.runtimeName, false)
      )
      res <- withLeoPublisher(publisherQueue) {
        for {
          dbRuntimeOpt <- clusterQuery
            .getActiveClusterByNameMinimal(testRuntime.googleProject, testRuntime.runtimeName)
            .transaction
          message <- publisherQueue.tryDequeue1

        } yield {
          dbRuntimeOpt.get.status shouldBe RuntimeStatus.Deleting
          message shouldBe (None)
        }
      }
    } yield res

    res.unsafeRunSync()
  }

  it should "stop a runtime" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      publisherQueue <- InspectableQueue.bounded[IO, LeoPubsubMessage](10)
      service = makeRuntimeService(publisherQueue)
      samResource <- IO(RuntimeSamResource(UUID.randomUUID.toString))
      testRuntime <- IO(makeCluster(1).copy(samResource = samResource).save())

      _ <- service.stopRuntime(userInfo, testRuntime.googleProject, testRuntime.runtimeName)
      res <- withLeoPublisher(publisherQueue) {
        for {
          dbRuntimeOpt <- clusterQuery
            .getActiveClusterByNameMinimal(testRuntime.googleProject, testRuntime.runtimeName)
            .transaction
          message <- publisherQueue.tryDequeue1
        } yield {
          dbRuntimeOpt.get.status shouldBe RuntimeStatus.Stopping
          message shouldBe (None)
        }
      }
    } yield res

    res.unsafeRunSync()
  }

  it should "start a runtime" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      publisherQueue <- InspectableQueue.bounded[IO, LeoPubsubMessage](10)
      service = makeRuntimeService(publisherQueue)
      samResource <- IO(RuntimeSamResource(UUID.randomUUID.toString))
      testRuntime <- IO(makeCluster(1).copy(samResource = samResource, status = RuntimeStatus.Stopped).save())

      _ <- service.startRuntime(userInfo, testRuntime.googleProject, testRuntime.runtimeName)
      res <- withLeoPublisher(publisherQueue) {
        for {
          dbRuntimeOpt <- clusterQuery
            .getActiveClusterByNameMinimal(testRuntime.googleProject, testRuntime.runtimeName)
            .transaction
          message <- publisherQueue.tryDequeue1
        } yield {
          dbRuntimeOpt.get.status shouldBe RuntimeStatus.Starting
          message shouldBe (None)
        }
      }
    } yield res

    res.unsafeRunSync()
  }

  it should "update autopause" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      samResource <- IO(RuntimeSamResource(UUID.randomUUID.toString))
      testRuntime <- IO(makeCluster(1).copy(samResource = samResource, status = RuntimeStatus.Running).save())
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
          samResource <- IO(RuntimeSamResource(UUID.randomUUID.toString))
          testRuntime <- IO(makeCluster(1).copy(samResource = samResource, status = status).save())
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
      t <- traceId.ask
      fail <- runtimeService.processUpdateRuntimeConfigRequest(req, false, testCluster, gceRuntimeConfig, t).attempt
    } yield {
      fail shouldBe Left(
        WrongCloudServiceException(CloudService.GCE, CloudService.Dataproc, t)
      )
    }
    res.unsafeRunSync()
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

  it should "update patchInProgress flag if stopToUpdateMachineType is true" in isolatedDbTest {
    val req =
      UpdateRuntimeConfigRequest.GceConfig(Some(MachineTypeName("n1-standard-8")), Some(gceRuntimeConfig.diskSize))
    val runtime = testCluster.copy(status = RuntimeStatus.Running)
    val res = for {
      traceId <- traceId.ask
      savedRuntime <- IO(runtime.save())
      _ <- runtimeService.processUpdateRuntimeConfigRequest(req, true, savedRuntime, gceRuntimeConfig, traceId)
      patchInProgress <- patchQuery.isInprogress(savedRuntime.id).transaction
      message <- publisherQueue.dequeue1
    } yield {
      patchInProgress shouldBe (true)
      message shouldBe UpdateRuntimeMessage(savedRuntime.id,
                                            Some(MachineTypeName("n1-standard-8")),
                                            true,
                                            None,
                                            None,
                                            None,
                                            Some(traceId))
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

  "RuntimeServiceInterp.processDiskConfigRequest" should "process a create disk request" in isolatedDbTest {
    val req = PersistentDiskRequest(diskName, Some(DiskSize(500)), None, Map("foo" -> "bar"))
    val res = for {
      context <- ctx.ask
      diskResult <- RuntimeServiceInterp.processPersistentDiskRequest(req,
                                                                      project,
                                                                      userInfo,
                                                                      serviceAccount,
                                                                      FormattedBy.GCE,
                                                                      whitelistAuthProvider,
                                                                      Config.persistentDiskConfig)
      disk = diskResult.disk
      persistedDisk <- persistentDiskQuery.getById(disk.id).transaction
    } yield {
      diskResult.doesExist shouldBe false
      disk.googleProject shouldBe project
      disk.zone shouldBe Config.persistentDiskConfig.zone
      disk.name shouldBe diskName
      disk.googleId shouldBe None
      disk.status shouldBe DiskStatus.Creating
      disk.auditInfo.creator shouldBe userInfo.userEmail
      disk.auditInfo.createdDate shouldBe context.now
      disk.auditInfo.destroyedDate shouldBe None
      disk.auditInfo.dateAccessed shouldBe context.now
      disk.size shouldBe DiskSize(500)
      disk.diskType shouldBe Config.persistentDiskConfig.defaultDiskType
      disk.blockSize shouldBe Config.persistentDiskConfig.defaultBlockSizeBytes
      disk.labels shouldBe DefaultDiskLabels(diskName, project, userInfo.userEmail, serviceAccount).toMap ++ Map(
        "foo" -> "bar"
      )

      persistedDisk shouldBe 'defined
      persistedDisk.get shouldEqual disk
    }

    res.unsafeRunSync()
  }

  it should "return existing disk if a disk with the same name already exists" in isolatedDbTest {
    val res = for {
      t <- ctx.ask
      disk <- makePersistentDisk(DiskId(1)).save()
      req = PersistentDiskRequest(disk.name, Some(DiskSize(50)), None, Map("foo" -> "bar"))
      returnedDisk <- RuntimeServiceInterp
        .processPersistentDiskRequest(req,
                                      project,
                                      userInfo,
                                      serviceAccount,
                                      FormattedBy.GCE,
                                      whitelistAuthProvider,
                                      Config.persistentDiskConfig)
        .attempt
    } yield {
      returnedDisk shouldBe Right(PersistentDiskRequestResult(disk, true))
    }

    res.unsafeRunSync()
  }

  it should "fail to create a disk when caller has no permission" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("badUser"), WorkbenchEmail("badEmail"), 0)
    val req = PersistentDiskRequest(diskName, Some(DiskSize(500)), None, Map("foo" -> "bar"))

    val thrown = the[AuthorizationError] thrownBy {
      RuntimeServiceInterp
        .processPersistentDiskRequest(req,
                                      project,
                                      userInfo,
                                      serviceAccount,
                                      FormattedBy.GCE,
                                      whitelistAuthProvider,
                                      Config.persistentDiskConfig)
        .unsafeRunSync()
    }

    thrown shouldBe AuthorizationError(Some(userInfo.userEmail))
  }

  it should "fail to process a disk reference when the disk is already attached" in isolatedDbTest {
    val res = for {
      t <- ctx.ask
      savedDisk <- makePersistentDisk(DiskId(1)).save()
      _ <- IO(
        makeCluster(1).saveWithRuntimeConfig(
          RuntimeConfig.GceWithPdConfig(defaultMachineType, Some(savedDisk.id), bootDiskSize = DiskSize(50))
        )
      )
      req = PersistentDiskRequest(savedDisk.name, Some(savedDisk.size), Some(savedDisk.diskType), savedDisk.labels)
      err <- RuntimeServiceInterp
        .processPersistentDiskRequest(req,
                                      project,
                                      userInfo,
                                      serviceAccount,
                                      FormattedBy.GCE,
                                      whitelistAuthProvider,
                                      Config.persistentDiskConfig)
        .attempt
    } yield {
      err shouldBe Left(DiskAlreadyAttachedException(project, savedDisk.name, t.traceId))
    }

    res.unsafeRunSync()
  }

  it should "fail to process a disk reference when the disk is already formatted by another app" in isolatedDbTest {
    val res = for {
      t <- ctx.ask
      gceDisk <- makePersistentDisk(DiskId(1), Some(FormattedBy.GCE)).save()
      req = PersistentDiskRequest(gceDisk.name, Some(gceDisk.size), Some(gceDisk.diskType), gceDisk.labels)
      formatGceDiskError <- RuntimeServiceInterp
        .processPersistentDiskRequest(req,
                                      project,
                                      userInfo,
                                      serviceAccount,
                                      FormattedBy.Galaxy,
                                      whitelistAuthProvider,
                                      Config.persistentDiskConfig)
        .attempt
      galaxyDisk <- makePersistentDisk(DiskId(2), Some(FormattedBy.Galaxy)).save()
      req = PersistentDiskRequest(galaxyDisk.name, Some(galaxyDisk.size), Some(galaxyDisk.diskType), galaxyDisk.labels)
      formatGalaxyDiskError <- RuntimeServiceInterp
        .processPersistentDiskRequest(req,
                                      project,
                                      userInfo,
                                      serviceAccount,
                                      FormattedBy.GCE,
                                      whitelistAuthProvider,
                                      Config.persistentDiskConfig)
        .attempt
    } yield {
      formatGceDiskError shouldBe Left(
        DiskAlreadyFormattedByOtherApp(project, gceDisk.name, t.traceId, FormattedBy.GCE)
      )
      formatGalaxyDiskError shouldBe Left(
        DiskAlreadyFormattedByOtherApp(project, galaxyDisk.name, t.traceId, FormattedBy.Galaxy)
      )
    }

    res.unsafeRunSync()
  }

  it should "fail to attach a disk when caller has no attach permission" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("badUser"), WorkbenchEmail("badEmail"), 0)
    val res = for {
      savedDisk <- makePersistentDisk(DiskId(1)).save()
      req = PersistentDiskRequest(savedDisk.name, Some(savedDisk.size), Some(savedDisk.diskType), savedDisk.labels)
      _ <- RuntimeServiceInterp.processPersistentDiskRequest(req,
                                                             project,
                                                             userInfo,
                                                             serviceAccount,
                                                             FormattedBy.GCE,
                                                             whitelistAuthProvider,
                                                             Config.persistentDiskConfig)
    } yield ()

    val thrown = the[AuthorizationError] thrownBy {
      res.unsafeRunSync()
    }

    thrown shouldBe AuthorizationError(Some(userInfo.userEmail))
  }

  private def withLeoPublisher(
    publisherQueue: InspectableQueue[IO, LeoPubsubMessage]
  )(validations: IO[Assertion]): IO[Assertion] = {
    val leoPublisher = new LeoPublisher[IO](publisherQueue, FakeGooglePublisher)
    withInfiniteStream(leoPublisher.process, validations)
  }
}
