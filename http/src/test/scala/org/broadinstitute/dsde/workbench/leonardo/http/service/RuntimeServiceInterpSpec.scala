package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.IO
import cats.effect.std.Queue
import cats.mtl.Ask
import com.google.api.gax.longrunning.OperationFuture
import com.google.cloud.compute.v1.Operation
import fs2.Pipe
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.google2.mock.{
  FakeComputeOperationFuture,
  FakeGoogleComputeService,
  FakeGoogleStorageInterpreter
}
import org.broadinstitute.dsde.workbench.google2.{
  DataprocRole,
  DeviceName,
  DiskName,
  GoogleComputeService,
  MachineTypeName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec.{
  projectSamResourceDecoder,
  runtimeSamResourceDecoder,
  workspaceSamResourceIdDecoder,
  wsmResourceSamResourceIdDecoder
}
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{CryptoDetector, Jupyter, Welder}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.{appContext, defaultMockitoAnswer, leonardoExceptionEq}
import org.broadinstitute.dsde.workbench.leonardo.auth.AllowlistAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.MockDockerDAO
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.service.RuntimeServiceInterp.{
  calculateAutopauseThreshold,
  getToolFromImages
}
import org.broadinstitute.dsde.workbench.leonardo.model.SamResourceAction.{
  projectSamResourceAction,
  runtimeSamResourceAction,
  workspaceSamResourceAction,
  wsmResourceSamResourceAction,
  AppSamResourceAction
}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.{
  DiskUpdate,
  LeoPubsubMessage,
  LeoPubsubMessageType,
  RuntimeConfigInCreateRuntimeMessage
}
import org.broadinstitute.dsde.workbench.leonardo.util.QueueFactory
import org.broadinstitute.dsde.workbench.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util2.InstanceName
import org.broadinstitute.dsde.workbench.util2.messaging.CloudPublisher
import org.mockito.ArgumentMatchers.{any, eq => isEq}
import org.mockito.Mockito.when
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

import java.net.URL
import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

trait RuntimeServiceInterpSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent with MockitoSugar {
  val publisherQueue = QueueFactory.makePublisherQueue()

  def makeRuntimeService(
    publisherQueue: Queue[IO, LeoPubsubMessage] = publisherQueue,
    computeService: GoogleComputeService[IO] = FakeGoogleComputeService,
    authProvider: AllowlistAuthProvider = allowListAuthProvider
  ) =
    new RuntimeServiceInterp(
      RuntimeServiceConfig(
        Config.proxyConfig.proxyUrlBase,
        imageConfig,
        autoFreezeConfig,
        dataprocConfig,
        Config.gceConfig,
        azureServiceConfig
      ),
      ConfigReader.appConfig.persistentDisk,
      authProvider,
      new MockDockerDAO,
      Some(FakeGoogleStorageInterpreter),
      Some(computeService),
      publisherQueue,
      MockSamService
    )

  val runtimeService = makeRuntimeService()

  val emptyCreateRuntimeReq = CreateRuntimeRequest(
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
    Map.empty,
    None
  )

  /**
   * Generate a mocked AuthProvider which will permit action on the given resource IDs by the given user.
   * TODO: cover actions beside `checkUserEnabled` and `listResourceIds`
   *
   * @param userInfo
   * @param readerRuntimeSamIds
   * @param readerWorkspaceSamIds
   * @param readerProjectSamIds
   * @param ownerWorkspaceSamIds
   * @param ownerProjectSamIds
   * @return
   */
  def mockAuthorize(
    userInfo: UserInfo,
    readerRuntimeSamIds: Set[RuntimeSamResourceId] = Set.empty,
    readerWsmSamIds: Set[WsmResourceSamResourceId] = Set.empty,
    readerWorkspaceSamIds: Set[WorkspaceResourceSamResourceId] = Set.empty,
    readerProjectSamIds: Set[ProjectSamResourceId] = Set.empty,
    ownerWorkspaceSamIds: Set[WorkspaceResourceSamResourceId] = Set.empty,
    ownerProjectSamIds: Set[ProjectSamResourceId] = Set.empty
  ): AllowlistAuthProvider = {
    val mockAuthProvider: AllowlistAuthProvider = mock[AllowlistAuthProvider](defaultMockitoAnswer[IO])

    when(mockAuthProvider.checkUserEnabled(isEq(userInfo))(any(Ask[IO, TraceId].getClass))).thenReturn(IO.unit)
    when(
      mockAuthProvider.listResourceIds[RuntimeSamResourceId](isEq(true), isEq(userInfo))(
        any(runtimeSamResourceAction.getClass),
        any(AppSamResourceAction.getClass),
        any(Decoder[RuntimeSamResourceId].getClass),
        any(Ask[IO, TraceId].getClass)
      )
    ).thenReturn(IO.pure(readerRuntimeSamIds))
    when(
      mockAuthProvider.listResourceIds[WsmResourceSamResourceId](isEq(false), isEq(userInfo))(
        any(wsmResourceSamResourceAction.getClass),
        any(AppSamResourceAction.getClass),
        any(Decoder[WsmResourceSamResourceId].getClass),
        any(Ask[IO, TraceId].getClass)
      )
    ).thenReturn(IO.pure(readerWsmSamIds))
    when(
      mockAuthProvider.listResourceIds[WorkspaceResourceSamResourceId](isEq(false), isEq(userInfo))(
        any(workspaceSamResourceAction.getClass),
        any(AppSamResourceAction.getClass),
        any(Decoder[WorkspaceResourceSamResourceId].getClass),
        any(Ask[IO, TraceId].getClass)
      )
    ).thenReturn(IO.pure(readerWorkspaceSamIds))
    when(
      mockAuthProvider.listResourceIds[ProjectSamResourceId](isEq(false), isEq(userInfo))(
        any(projectSamResourceAction.getClass),
        any(AppSamResourceAction.getClass),
        any(Decoder[ProjectSamResourceId].getClass),
        any(Ask[IO, TraceId].getClass)
      )
    )
      .thenReturn(IO.pure(readerProjectSamIds))
    when(
      mockAuthProvider.listResourceIds[WorkspaceResourceSamResourceId](isEq(true), isEq(userInfo))(
        any(workspaceSamResourceAction.getClass),
        any(AppSamResourceAction.getClass),
        any(Decoder[WorkspaceResourceSamResourceId].getClass),
        any(Ask[IO, TraceId].getClass)
      )
    )
      .thenReturn(IO.pure(ownerWorkspaceSamIds))
    when(
      mockAuthProvider.listResourceIds[ProjectSamResourceId](isEq(true), isEq(userInfo))(
        any(projectSamResourceAction.getClass),
        any(AppSamResourceAction.getClass),
        any(Decoder[ProjectSamResourceId].getClass),
        any(Ask[IO, TraceId].getClass)
      )
    )
      .thenReturn(IO.pure(ownerProjectSamIds))

    mockAuthProvider
  }

  def mockUserInfo(email: String = userEmail.toString()): UserInfo =
    UserInfo(OAuth2BearerToken(""), WorkbenchUserId(s"userId-${email}"), WorkbenchEmail(email), 0)
}

class RuntimeServiceInterpTest
    extends AnyFlatSpec
    with RuntimeServiceInterpSpec
    with LeonardoTestSuite
    with TestComponent
    with MockitoSugar {

  it should "fail if user doesn't have project level permission" in {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("email"), 0)

    val res = for {
      r <- runtimeService
        .createRuntime(
          userInfo,
          cloudContextGcp,
          RuntimeName("clusterName1"),
          emptyCreateRuntimeReq
        )
        .attempt
    } yield r shouldBe Left(ForbiddenError(userInfo.userEmail))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "calculate autopause threshold properly" in {
    calculateAutopauseThreshold(None, None, autoFreezeConfig) shouldBe autoFreezeConfig.autoFreezeAfter.toMinutes.toInt
    calculateAutopauseThreshold(Some(false), None, autoFreezeConfig) shouldBe autoPauseOffValue
    calculateAutopauseThreshold(
      Some(true),
      None,
      autoFreezeConfig
    ) shouldBe autoFreezeConfig.autoFreezeAfter.toMinutes.toInt
    calculateAutopauseThreshold(None, Some(40), autoFreezeConfig) shouldBe 40
    calculateAutopauseThreshold(Some(true), Some(35), autoFreezeConfig) shouldBe 35
  }

  it should "throw ClusterAlreadyExistsException when creating a cluster with same name and project as an existing cluster" in isolatedDbTest {
    runtimeService
      .createRuntime(userInfo, cloudContextGcp, name0, emptyCreateRuntimeReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val exc = runtimeService
      .createRuntime(userInfo, cloudContextGcp, name0, emptyCreateRuntimeReq)
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
    exc shouldBe a[RuntimeAlreadyExistsException]
  }

  it should "throw a JupyterExtensionException when the extensionUri is too long" in isolatedDbTest {
    // create the cluster
    val clusterRequest = emptyCreateRuntimeReq.copy(userJupyterExtensionConfig =
      Some(
        UserJupyterExtensionConfig(nbExtensions =
          Map("notebookExtension" -> s"gs://bucket/${LazyList.continually('a').take(1025).mkString}")
        )
      )
    )
    val response =
      runtimeService
        .createRuntime(userInfo, cloudContextGcp, name0, clusterRequest)
        .attempt
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
        .swap
        .toOption
        .get

    response shouldBe a[BucketObjectException]
  }

  it should "throw a JupyterExtensionException when the jupyterExtensionUri does not point to a GCS object" in isolatedDbTest {
    // create the cluster
    val response =
      runtimeService
        .createRuntime(
          userInfo,
          cloudContextGcp,
          name0,
          emptyCreateRuntimeReq.copy(userJupyterExtensionConfig =
            Some(UserJupyterExtensionConfig(nbExtensions = Map("notebookExtension" -> "gs://bogus/object.tar.gz")))
          )
        )
        .attempt
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
        .swap
        .toOption
        .get

    response shouldBe a[BucketObjectException]
  }

  it should "successfully create a GCE runtime when no runtime is specified" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted
    val cloudContext = CloudContext.Gcp(GoogleProject("googleProject"))
    val runtimeName = RuntimeName("clusterName1")

    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with
      context <- appContext.ask[AppContext]
      r <- runtimeService
        .createRuntime(
          userInfo,
          cloudContext,
          runtimeName,
          emptyCreateRuntimeReq
        )
        .attempt
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(cloudContext, runtimeName)(scala.concurrent.ExecutionContext.global)
        .transaction
      cluster = clusterOpt.get
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId).transaction
      message <- publisherQueue.take
      gceRuntimeConfig = runtimeConfig.asInstanceOf[RuntimeConfig.GceConfig]
      gceRuntimeConfigRequest = LeoLenses.runtimeConfigPrism.getOption(gceRuntimeConfig).get
    } yield {
      r shouldBe Right(CreateRuntimeResponse(context.traceId))
      runtimeConfig shouldBe (Config.gceConfig.runtimeConfigDefaults)
      cluster.cloudContext shouldBe cloudContext
      cluster.runtimeName shouldBe runtimeName
      val expectedMessage = CreateRuntimeMessage
        .fromRuntime(cluster, gceRuntimeConfigRequest, Some(context.traceId), None)
        .copy(
          runtimeImages = Set(
            RuntimeImage(
              RuntimeImageType.Jupyter,
              Config.imageConfig.jupyterImage.imageUrl,
              Some(Paths.get("/home/jupyter")),
              context.now
            ),
            RuntimeImage(RuntimeImageType.Welder, Config.imageConfig.welderGcrImage.imageUrl, None, context.now),
            RuntimeImage(RuntimeImageType.Proxy, Config.imageConfig.proxyImage.imageUrl, None, context.now),
            RuntimeImage(
              RuntimeImageType.CryptoDetector,
              Config.imageConfig.cryptoDetectorImage.imageUrl,
              None,
              context.now
            )
          ),
          scopes = Config.gceConfig.defaultScopes
        )
      message shouldBe expectedMessage
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "successfully accept https as user script and user startup script" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted
    val cloudContext = CloudContext.Gcp(GoogleProject("project1"))
    val runtimeName = RuntimeName("clusterName2")
    val request = emptyCreateRuntimeReq.copy(
      userScriptUri = Some(
        UserScriptPath.Http(
          new URL("https://api-dot-all-of-us-workbench-test.appspot.com/static/start_notebook_cluster.sh")
        )
      ),
      startUserScriptUri = Some(
        UserScriptPath.Http(
          new URL("https://api-dot-all-of-us-workbench-test.appspot.com/static/start_notebook_cluster.sh")
        )
      )
    )

    val res = for {
      r <- runtimeService
        .createRuntime(
          userInfo,
          cloudContext,
          runtimeName,
          request
        )
        .attempt
      _ <- publisherQueue.take // dequeue the message so that it doesn't affect other tests
    } yield r.isRight shouldBe true
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "successfully create a cluster with an rstudio image" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted
    val cloudContext = CloudContext.Gcp(GoogleProject("googleProject"))
    val runtimeName = RuntimeName("clusterName2")
    val rstudioImage = ContainerImage("some-rstudio-image", ContainerRegistry.GCR)
    val request = emptyCreateRuntimeReq.copy(
      toolDockerImage = Some(rstudioImage)
    )

    val res = for {
      r <- runtimeService
        .createRuntime(
          userInfo,
          cloudContext,
          runtimeName,
          request
        )
        .attempt
      runtime <- clusterQuery.getActiveClusterByNameMinimal(cloudContext, runtimeName).transaction
      _ <- publisherQueue.take // dequeue the message so that it doesn't affect other tests
    } yield {
      r.isRight shouldBe true
      runtime.get.runtimeImages.map(_.imageType) contains (RuntimeImageType.RStudio)
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "successfully create a dataproc runtime when explicitly told so when numberOfWorkers is 0" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted
    val cloudContext = CloudContext.Gcp(GoogleProject("googleProject"))
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
          Map.empty,
          None,
          false,
          false
        )
      )
    )

    val res = for {
      _ <- publisherQueue.tryTake
      context <- appContext.ask[AppContext]
      _ <- runtimeService
        .createRuntime(
          userInfo,
          cloudContext,
          runtimeName,
          req
        )
        .attempt
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(cloudContext, runtimeName)(scala.concurrent.ExecutionContext.global)
        .transaction
      cluster = clusterOpt.get
      runtimeConfig <- RuntimeConfigQueries
        .getRuntimeConfig(cluster.runtimeConfigId)(scala.concurrent.ExecutionContext.global)
        .transaction
      runtimeConfigRequest = LeoLenses.runtimeConfigPrism.getOption(runtimeConfig).get
      message <- publisherQueue.take
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
        .fromRuntime(cluster, runtimeConfigRequest, Some(context.traceId), None)
        .copy(
          runtimeImages = Set(
            RuntimeImage(
              RuntimeImageType.Jupyter,
              Config.imageConfig.jupyterImage.imageUrl,
              Some(Paths.get("/home/jupyter")),
              context.now
            ),
            RuntimeImage(RuntimeImageType.Welder, Config.imageConfig.welderGcrImage.imageUrl, None, context.now),
            RuntimeImage(RuntimeImageType.Proxy, Config.imageConfig.proxyImage.imageUrl, None, context.now),
            RuntimeImage(
              RuntimeImageType.CryptoDetector,
              Config.imageConfig.cryptoDetectorImage.imageUrl,
              None,
              context.now
            )
          ),
          scopes = Config.dataprocConfig.defaultScopes
        )
      message shouldBe expectedMessage
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "successfully create a dataproc runtime when explicitly told so when numberOfWorkers is more than 0" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted
    val cloudContext = CloudContext.Gcp(GoogleProject("googleProject"))
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
          Map.empty,
          None,
          false,
          false
        )
      )
    )

    val res = for {
      _ <- publisherQueue.tryTake
      context <- appContext.ask[AppContext]
      _ <- runtimeService
        .createRuntime(
          userInfo,
          cloudContext,
          runtimeName,
          req
        )
        .attempt
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(cloudContext, runtimeName)(scala.concurrent.ExecutionContext.global)
        .transaction
      cluster = clusterOpt.get
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId).transaction
      runtimeConfigRequest = LeoLenses.runtimeConfigPrism.getOption(runtimeConfig).get
      message <- publisherQueue.take
    } yield {
      runtimeConfig shouldBe Config.dataprocConfig.runtimeConfigDefaults.copy(numberOfWorkers = 2)
      val expectedMessage = CreateRuntimeMessage
        .fromRuntime(cluster, runtimeConfigRequest, Some(context.traceId), None)
        .copy(
          runtimeImages = Set(
            RuntimeImage(
              RuntimeImageType.Jupyter,
              Config.imageConfig.jupyterImage.imageUrl,
              Some(Paths.get("/home/jupyter")),
              context.now
            ),
            RuntimeImage(RuntimeImageType.Welder, Config.imageConfig.welderGcrImage.imageUrl, None, context.now),
            RuntimeImage(RuntimeImageType.Proxy, Config.imageConfig.proxyImage.imageUrl, None, context.now),
            RuntimeImage(
              RuntimeImageType.CryptoDetector,
              Config.imageConfig.cryptoDetectorImage.imageUrl,
              None,
              context.now
            )
          ),
          scopes = Config.dataprocConfig.defaultScopes
        )
      message shouldBe expectedMessage
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "create a runtime with the latest welder from welderRegistry" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted
    val cloudContext = CloudContext.Gcp(GoogleProject("cloudContext"))
    val runtimeName1 = RuntimeName("runtimeName1")
    val runtimeName2 = RuntimeName("runtimeName2")
    val runtimeName3 = RuntimeName("runtimeName3")

    val res = for {
      r1 <- runtimeService
        .createRuntime(
          userInfo,
          cloudContext,
          runtimeName1,
          emptyCreateRuntimeReq.copy(welderRegistry = Some(ContainerRegistry.DockerHub))
        )
        .attempt
      r2 <- runtimeService
        .createRuntime(
          userInfo,
          cloudContext,
          runtimeName2,
          emptyCreateRuntimeReq.copy(welderRegistry = Some(ContainerRegistry.GCR))
        )
        .attempt
      r3 <- runtimeService
        .createRuntime(
          userInfo,
          cloudContext,
          runtimeName3,
          emptyCreateRuntimeReq
        )
        .attempt

      runtimeOpt1 <- clusterQuery
        .getActiveClusterByNameMinimal(cloudContext, runtimeName1)(scala.concurrent.ExecutionContext.global)
        .transaction
      runtime1 = runtimeOpt1.get
      runtime1Images <- clusterImageQuery.getAllImagesForCluster(runtime1.id).transaction
      welder1 = runtime1Images.filter(_.imageType == RuntimeImageType.Welder).headOption
      _ <- publisherQueue.take

      runtimeOpt2 <- clusterQuery.getActiveClusterByNameMinimal(cloudContext, runtimeName2).transaction
      runtime2 = runtimeOpt2.get
      runtime2Images <- clusterImageQuery.getAllImagesForCluster(runtime2.id).transaction
      welder2 = runtime2Images.filter(_.imageType == RuntimeImageType.Welder).headOption
      _ <- publisherQueue.take

      runtimeOpt3 <- clusterQuery.getActiveClusterByNameMinimal(cloudContext, runtimeName3).transaction
      runtime3 = runtimeOpt3.get
      runtime2Images <- clusterImageQuery.getAllImagesForCluster(runtime3.id).transaction
      welder3 = runtime2Images.filter(_.imageType == RuntimeImageType.Welder).headOption
      _ <- publisherQueue.take
    } yield {
      r1.isRight shouldBe true
      runtime1.runtimeName shouldBe runtimeName1
      welder1 shouldBe defined
      welder1.get.imageUrl shouldBe Config.imageConfig.welderDockerHubImage.imageUrl

      r2.isRight shouldBe true
      runtime2.runtimeName shouldBe runtimeName2
      welder2 shouldBe defined
      welder2.get.imageUrl shouldBe Config.imageConfig.welderGcrImage.imageUrl

      r3.isRight shouldBe true
      runtime3.runtimeName shouldBe runtimeName3
      welder3 shouldBe defined
      welder3.get.imageUrl shouldBe Config.imageConfig.welderGcrImage.imageUrl
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "create a runtime with the crypto-detector image" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted
    val cloudContext = CloudContext.Gcp(GoogleProject("googleProject"))
    val runtimeName1 = RuntimeName("runtimeName1")
    val runtimeName2 = RuntimeName("runtimeName2")

    val res = for {
      r1 <- runtimeService
        .createRuntime(
          userInfo,
          cloudContext,
          runtimeName1,
          emptyCreateRuntimeReq.copy(welderRegistry = Some(ContainerRegistry.DockerHub))
        )
        .attempt

      r2 <- runtimeService
        .createRuntime(
          userInfo,
          cloudContext,
          runtimeName2,
          emptyCreateRuntimeReq.copy(welderRegistry = Some(ContainerRegistry.GCR))
        )
        .attempt

      runtimeOpt1 <- clusterQuery
        .getActiveClusterByNameMinimal(cloudContext, runtimeName1)(scala.concurrent.ExecutionContext.global)
        .transaction
      runtime1 = runtimeOpt1.get
      runtime1Images <- clusterImageQuery.getAllImagesForCluster(runtime1.id).transaction
      _ <- publisherQueue.take

      runtimeOpt2 <- clusterQuery
        .getActiveClusterByNameMinimal(cloudContext, runtimeName2)(scala.concurrent.ExecutionContext.global)
        .transaction
      runtime2 = runtimeOpt2.get
      runtime2Images <- clusterImageQuery.getAllImagesForCluster(runtime2.id).transaction
      _ <- publisherQueue.take
    } yield {
      // Crypto detector not supported on DockerHub
      r1.isRight shouldBe true
      runtime1.runtimeName shouldBe runtimeName1
      runtime1Images.map(_.imageType) should contain theSameElementsAs Set(Jupyter, Welder, RuntimeImageType.Proxy)

      r2.isRight shouldBe true
      runtime2.runtimeName shouldBe runtimeName2
      runtime2Images
        .map(_.imageType) should contain theSameElementsAs Set(Jupyter, Welder, RuntimeImageType.Proxy, CryptoDetector)
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "create a runtime with a disk config" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted
    val persistentDisk = PersistentDiskRequest(
      diskName,
      Some(DiskSize(500)),
      None,
      Map.empty
    )
    val req = emptyCreateRuntimeReq.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.GceWithPdConfig(
          machineType = Some(MachineTypeName("n1-standard-4")),
          persistentDisk,
          None,
          None
        )
      )
    )

    val res = for {
      context <- appContext.ask[AppContext]
      r <- runtimeService
        .createRuntime(
          userInfo,
          cloudContextGcp,
          name0,
          req
        )
        .attempt
      runtimeOpt <- clusterQuery
        .getActiveClusterByNameMinimal(cloudContextGcp, name0)(scala.concurrent.ExecutionContext.global)
        .transaction
      runtime = runtimeOpt.get
      diskOpt <- persistentDiskQuery
        .getActiveByName(cloudContextGcp, diskName)(scala.concurrent.ExecutionContext.global)
        .transaction
      disk = diskOpt.get
      runtimeConfig <- RuntimeConfigQueries
        .getRuntimeConfig(runtime.runtimeConfigId)(scala.concurrent.ExecutionContext.global)
        .transaction
      runtimeConfigRequest = LeoLenses.runtimeConfigPrism.getOption(runtimeConfig).get
      message <- publisherQueue.take
    } yield {
      r shouldBe Right(CreateRuntimeResponse(context.traceId))
      runtime.cloudContext shouldBe cloudContextGcp
      runtime.runtimeName shouldBe name0
      runtimeConfig.asInstanceOf[RuntimeConfig.GceWithPdConfig].persistentDiskId shouldBe Some(disk.id)
      disk.cloudContext shouldBe cloudContextGcp
      disk.name shouldBe diskName
      disk.size shouldBe DiskSize(500)
      runtimeConfig shouldBe RuntimeConfig.GceWithPdConfig(
        MachineTypeName("n1-standard-4"),
        Some(disk.id),
        bootDiskSize = DiskSize(250),
        zone = ZoneName("us-central1-a"),
        None
      ) // TODO: this is a problem in terms of inconsistency
      val expectedMessage = CreateRuntimeMessage
        .fromRuntime(runtime, runtimeConfigRequest, Some(context.traceId), None)
        .copy(
          runtimeImages = Set(
            RuntimeImage(
              RuntimeImageType.Jupyter,
              Config.imageConfig.jupyterImage.imageUrl,
              Some(Paths.get("/home/jupyter")),
              context.now
            ),
            RuntimeImage(RuntimeImageType.Welder, Config.imageConfig.welderGcrImage.imageUrl, None, context.now),
            RuntimeImage(RuntimeImageType.Proxy, Config.imageConfig.proxyImage.imageUrl, None, context.now),
            RuntimeImage(
              RuntimeImageType.CryptoDetector,
              Config.imageConfig.cryptoDetectorImage.imageUrl,
              None,
              context.now
            )
          ),
          scopes = Config.gceConfig.defaultScopes,
          runtimeConfig = RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig(
            runtimeConfig.machineType,
            disk.id,
            bootDiskSize = DiskSize(250),
            zone = ZoneName("us-central1-a"),
            None
          )
        )
      message shouldBe expectedMessage
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to delete a runtime while it's still creating" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted

    val res = for {
      _ <- runtimeService
        .createRuntime(
          userInfo,
          cloudContextGcp,
          name0,
          emptyCreateRuntimeReq
        )
      r <- runtimeService
        .deleteRuntime(DeleteRuntimeRequest(userInfo, GoogleProject(cloudContextGcp.asString), name0, false))
        .attempt
    } yield r.swap.toOption.get.isInstanceOf[RuntimeCannotBeDeletedException] shouldBe true

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "create a runtime with a gpu config" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted

    val gpuConfig = Some(GpuConfig(GpuType.NvidiaTeslaT4, 2))
    val req = emptyCreateRuntimeReq.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.GceConfig(
          machineType = Some(MachineTypeName("n1-standard-4")),
          diskSize = Some(DiskSize(500)),
          None,
          gpuConfig = gpuConfig
        )
      )
    )

    val res = for {
      _ <- publisherQueue.tryTake
      r <- runtimeService
        .createRuntime(
          userInfo,
          cloudContextGcp,
          name0,
          req
        )
        .attempt
      runtimeOpt <- clusterQuery
        .getActiveClusterByNameMinimal(cloudContextGcp, name0)(scala.concurrent.ExecutionContext.global)
        .transaction
      runtime = runtimeOpt.get
      runtimeConfig <- RuntimeConfigQueries
        .getRuntimeConfig(runtime.runtimeConfigId)(scala.concurrent.ExecutionContext.global)
        .transaction
      message <- publisherQueue.take
    } yield {
      r.isRight shouldBe true
      runtime.cloudContext shouldBe cloudContextGcp
      runtime.runtimeName shouldBe name0
      runtimeConfig.asInstanceOf[RuntimeConfig.GceConfig].gpuConfig shouldBe gpuConfig
      message
        .asInstanceOf[CreateRuntimeMessage]
        .runtimeConfig
        .asInstanceOf[RuntimeConfigInCreateRuntimeMessage.GceConfig]
        .gpuConfig shouldBe gpuConfig
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "get a runtime" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted

    val res = for {
      samResource <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      testRuntime <- IO(makeCluster(1).copy(samResource = samResource).save())
      getResponse <- runtimeService.getRuntime(userInfo, testRuntime.cloudContext, testRuntime.runtimeName)
    } yield getResponse.samResource shouldBe testRuntime.samResource
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "throw ClusterNotFoundException for nonexistent clusters" in isolatedDbTest {
    val exc = runtimeService
      .getRuntime(userInfo, CloudContext.Gcp(GoogleProject("nonexistent")), RuntimeName("cluster"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
    exc shouldBe a[RuntimeNotFoundException]
  }

  it should "fail to get a runtime when users don't have access to the project" in isolatedDbTest {
    val exc = runtimeService
      .getRuntime(userInfo4, cloudContextGcp, RuntimeName("cluster"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
    exc shouldBe a[ForbiddenError]
  }

  it should "list runtimes" in isolatedDbTest {
    val userInfo = mockUserInfo("grendel@mom.mere")
    val runtimeIds =
      Vector(RuntimeSamResourceId(UUID.randomUUID.toString), RuntimeSamResourceId(UUID.randomUUID.toString))
    val mockAuthProvider = mockAuthorize(
      userInfo,
      readerRuntimeSamIds = Set(runtimeIds(0), runtimeIds(1)),
      readerProjectSamIds = Set(ProjectSamResourceId(project))
    )
    when(mockAuthProvider.isUserProjectReader(any, isEq(userInfo))(any)).thenReturn(IO.pure(true))
    val service = makeRuntimeService(authProvider = mockAuthProvider)

    val res = for {
      _ <- IO(makeCluster(1, samResource = runtimeIds(0)).save())
      _ <- IO(makeCluster(2, samResource = runtimeIds(1)).save())
      listResponse <- service.listRuntimes(userInfo, None, Map.empty)
    } yield listResponse.map(_.samResource) should contain theSameElementsAs runtimeIds

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes filtered by project" in isolatedDbTest {
    val userInfo = mockUserInfo("grendel@mom.mere")
    val runtimeIds = Vector(RuntimeSamResourceId(UUID.randomUUID.toString),
                            RuntimeSamResourceId(UUID.randomUUID.toString),
                            RuntimeSamResourceId(UUID.randomUUID.toString)
    )
    val mockAuthProvider = mockAuthorize(
      userInfo,
      readerRuntimeSamIds = Set(runtimeIds(0), runtimeIds(1)),
      readerProjectSamIds = Set(ProjectSamResourceId(project), ProjectSamResourceId(project2))
    )
    when(mockAuthProvider.isUserProjectReader(any, isEq(userInfo))(any)).thenReturn(IO.pure(true))
    val service = makeRuntimeService(authProvider = mockAuthProvider)

    val res = for {
      _ <- IO(makeCluster(1).copy(samResource = runtimeIds(0)).save())
      _ <- IO(makeCluster(2).copy(samResource = runtimeIds(1)).save())
      _ <- IO(makeCluster(3, cloudContext = cloudContext2Gcp).copy(samResource = runtimeIds(2)).save())
      listResponse <- service.listRuntimes(userInfo, Some(cloudContextGcp), Map.empty)
    } yield listResponse.map(_.samResource) should contain theSameElementsAs runtimeIds.slice(0, 2)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes filtered by label" in isolatedDbTest {
    val userInfo = mockUserInfo("grendel@mom.mere")
    val runtimeIds =
      Vector(RuntimeSamResourceId(UUID.randomUUID.toString), RuntimeSamResourceId(UUID.randomUUID.toString))
    val mockAuthProvider = mockAuthorize(
      userInfo,
      readerRuntimeSamIds = Set(runtimeIds(0), runtimeIds(1)),
      readerProjectSamIds = Set(ProjectSamResourceId(project))
    )
    when(mockAuthProvider.isUserProjectReader(any, isEq(userInfo))(any)).thenReturn(IO.pure(true))
    val service = makeRuntimeService(authProvider = mockAuthProvider)

    val res = for {
      runtime1 <- IO(makeCluster(1).copy(samResource = runtimeIds(0)).save())
      _ <- IO(makeCluster(2).copy(samResource = runtimeIds(1)).save())
      _ <- labelQuery.save(runtime1.id, LabelResourceType.Runtime, "foo", "bar").transaction
      listResponse <- service.listRuntimes(userInfo, None, Map("foo" -> "bar"))
      _ = println(listResponse)
    } yield listResponse.map(_.samResource).toSet shouldBe Set(runtimeIds(0))

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  // See https://broadworkbench.atlassian.net/browse/PROD-440
  // AoU relies on the ability for project owners to list other users' runtimes.
  it should "list runtimes belonging to other users" in isolatedDbTest {
    val userInfo = mockUserInfo("grendel@mere.mom")
    val runtimeIds =
      Vector(RuntimeSamResourceId(UUID.randomUUID.toString), RuntimeSamResourceId(UUID.randomUUID.toString))
    val mockAuthProvider = mockAuthorize(
      userInfo,
      ownerProjectSamIds = Set(ProjectSamResourceId(project))
    )
    when(mockAuthProvider.isUserProjectReader(any, isEq(userInfo))(any)).thenReturn(IO.pure(true))
    val service = makeRuntimeService(authProvider = mockAuthProvider)

    // Make runtimes belonging to different users than the calling user
    val res = for {
      samResource1 <- IO(runtimeIds(0))
      samResource2 <- IO(runtimeIds(1))
      runtime1 = LeoLenses.runtimeToCreator.replace(WorkbenchEmail("beowulf@heorot.hall"))(
        makeCluster(1).copy(samResource = samResource1)
      )
      runtime2 = LeoLenses.runtimeToCreator.replace(WorkbenchEmail("beowulf@heorot.hall"))(
        makeCluster(2).copy(samResource = samResource2)
      )
      _ <- IO(runtime1.save())
      _ <- IO(runtime2.save())
      listResponse <- service.listRuntimes(userInfo, None, Map.empty)
    } yield listResponse.map(_.samResource).toSet shouldBe Set(samResource1, samResource2)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes with labels" in isolatedDbTest {
    // create a couple of clusters
    val clusterName1 = RuntimeName(s"cluster-${UUID.randomUUID.toString}")
    val req = emptyCreateRuntimeReq.copy(
      labels = Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"),
      userJupyterExtensionConfig =
        Some(UserJupyterExtensionConfig(Map("abc" -> "def"), Map("pqr" -> "pqr"), Map("xyz" -> "xyz"))),
      defaultClientId = Some("ThisIsADefaultClientID"),
      autopause = Some(true),
      autopauseThreshold = Some(30 minutes)
    )
    runtimeService
      .createRuntime(userInfo, cloudContextGcp, clusterName1, req)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val runtime1 = runtimeService
      .getRuntime(userInfo, cloudContextGcp, clusterName1)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val listRuntimeResponse1 = ListRuntimeResponse2(
      runtime1.id,
      Some(workspaceId),
      runtime1.samResource,
      runtime1.clusterName,
      runtime1.cloudContext,
      runtime1.auditInfo,
      runtime1.runtimeConfig,
      runtime1.clusterUrl,
      runtime1.status,
      runtime1.labels,
      runtime1.patchInProgress
    )

    val clusterName2 = RuntimeName(s"cluster-${UUID.randomUUID.toString}")
    runtimeService
      .createRuntime(userInfo, cloudContextGcp, clusterName2, req.copy(labels = Map("a" -> "b", "foo" -> "bar")))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val runtime2 = runtimeService
      .getRuntime(userInfo, cloudContextGcp, clusterName2)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val listRuntimeResponse2 = ListRuntimeResponse2(
      runtime2.id,
      Some(workspaceId),
      runtime2.samResource,
      runtime2.clusterName,
      runtime2.cloudContext,
      runtime2.auditInfo,
      runtime2.runtimeConfig,
      runtime2.clusterUrl,
      runtime2.status,
      runtime2.labels,
      runtime2.patchInProgress
    )

    val mockAuthProvider = mockAuthorize(
      userInfo,
      readerRuntimeSamIds = Set(runtime1.samResource, runtime2.samResource),
      readerProjectSamIds = Set(ProjectSamResourceId(project))
    )
    when(mockAuthProvider.isUserProjectReader(any, isEq(userInfo))(any)).thenReturn(IO.pure(true))
    val service = makeRuntimeService(authProvider = mockAuthProvider)

    service
      .listRuntimes(userInfo, None, Map("_labels" -> "foo=bar"))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .toSet shouldBe Set(
      listRuntimeResponse1,
      listRuntimeResponse2
    )
    service
      .listRuntimes(userInfo, None, Map("_labels" -> "foo=bar,bam=yes"))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .toSet shouldBe Set(
      listRuntimeResponse1
    )
    service
      .listRuntimes(userInfo, None, Map("_labels" -> "foo=bar,bam=yes,vcf=no"))
      .unsafeToFuture()(cats.effect.unsafe.IORuntime.global)
      .futureValue
      .toSet shouldBe Set(listRuntimeResponse1)
    service
      .listRuntimes(userInfo, None, Map("_labels" -> "a=b"))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .toSet shouldBe Set(
      listRuntimeResponse2
    )
    service
      .listRuntimes(userInfo, None, Map("_labels" -> "baz=biz"))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .toSet shouldBe Set.empty
    service
      .listRuntimes(userInfo, None, Map("_labels" -> "A=B"))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .toSet shouldBe Set(
      listRuntimeResponse2
    ) // labels are not case sensitive because MySQL
    service
      .listRuntimes(userInfo, None, Map("_labels" -> "foo%3Dbar"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ParseLabelsException] shouldBe true
    service
      .listRuntimes(userInfo, None, Map("_labels" -> "foo=bar;bam=yes"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ParseLabelsException] shouldBe true
    service
      .listRuntimes(userInfo, None, Map("_labels" -> "foo=bar,bam"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ParseLabelsException] shouldBe true

    service
      .listRuntimes(userInfo, None, Map("_labels" -> "bogus"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ParseLabelsException] shouldBe true

    service
      .listRuntimes(userInfo, None, Map("_labels" -> "a,b"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ParseLabelsException] shouldBe true
  }

  it should "delete a runtime" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted

    val res = for {
      publisherQueue <- Queue.bounded[IO, LeoPubsubMessage](10)
      service = makeRuntimeService(publisherQueue)
      samResource <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      testRuntime <- IO(makeCluster(1).copy(samResource = samResource).save())

      _ <- service.deleteRuntime(
        DeleteRuntimeRequest(userInfo, GoogleProject(testRuntime.cloudContext.asString), testRuntime.runtimeName, false)
      )
      res <- withLeoPublisher(publisherQueue) {
        for {
          dbRuntimeOpt <- clusterQuery
            .getActiveClusterByNameMinimal(testRuntime.cloudContext, testRuntime.runtimeName)
            .transaction
          message <- publisherQueue.tryTake
        } yield {
          dbRuntimeOpt.get.status shouldBe RuntimeStatus.Deleting
          message shouldBe None
        }
      }
    } yield res

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete a runtime with disk properly" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted

    val res = for {
      publisherQueue <- Queue.bounded[IO, LeoPubsubMessage](10)
      service = makeRuntimeService(publisherQueue)
      pd <- makePersistentDisk().save()
      testRuntime <- IO(
        makeCluster(1).saveWithRuntimeConfig(
          RuntimeConfig
            .GceWithPdConfig(
              MachineTypeName("n1-standard-4"),
              Some(pd.id),
              bootDiskSize = DiskSize(50),
              zone = ZoneName("us-central1-a"),
              None
            )
        )
      )

      _ <- service.deleteRuntime(
        DeleteRuntimeRequest(userInfo, GoogleProject(testRuntime.cloudContext.asString), testRuntime.runtimeName, true)
      )
      diskStatus <- persistentDiskQuery.getStatus(pd.id)(scala.concurrent.ExecutionContext.global).transaction
      _ = diskStatus shouldBe Some(DiskStatus.Deleting)
      res <- withLeoPublisher(publisherQueue) {
        for {
          dbRuntimeOpt <- clusterQuery
            .getActiveClusterByNameMinimal(testRuntime.cloudContext, testRuntime.runtimeName)
            .transaction
          message <- publisherQueue.tryTake
        } yield {
          dbRuntimeOpt.get.status shouldBe RuntimeStatus.Deleting
          message shouldBe None
        }
      }
    } yield res

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to delete a runtime if detaching disk fails" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted
    val operationFuture = new FakeComputeOperationFuture {
      override def get(): Operation = {
        val exception = new java.util.concurrent.ExecutionException(
          "com.google.api.gax.rpc.NotFoundException: Not Found",
          new Exception("bad")
        )
        throw exception
      }
    }
    val computeService = new FakeGoogleComputeService {
      override def detachDisk(
        project: GoogleProject,
        zone: ZoneName,
        instanceName: InstanceName,
        deviceName: DeviceName
      )(implicit ev: Ask[IO, TraceId]): IO[Option[OperationFuture[Operation, Operation]]] =
        IO.pure(Some(operationFuture))
    }
    val runtimeService = makeRuntimeService(computeService = computeService)
    val res = for {
      pd <- makePersistentDisk().save()
      testRuntime <- IO(
        makeCluster(1).saveWithRuntimeConfig(
          RuntimeConfig
            .GceWithPdConfig(
              MachineTypeName("n1-standard-4"),
              Some(pd.id),
              bootDiskSize = DiskSize(50),
              zone = ZoneName("us-central1-a"),
              None
            )
        )
      )
      r <- runtimeService
        .deleteRuntime(
          DeleteRuntimeRequest(userInfo, GoogleProject(cloudContextGcp.asString), testRuntime.runtimeName, false)
        )
        .attempt
    } yield r.isRight shouldBe true

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to delete a runtime if user loses project access" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted
    val runtimeService = makeRuntimeService(authProvider = allowListAuthProvider2)
    val res = for {
      context <- appContext.ask[AppContext]
      pd <- makePersistentDisk().save()
      testRuntime <- IO(
        makeCluster(1).saveWithRuntimeConfig(
          RuntimeConfig
            .GceWithPdConfig(
              MachineTypeName("n1-standard-4"),
              Some(pd.id),
              bootDiskSize = DiskSize(50),
              zone = ZoneName("us-central1-a"),
              None
            )
        )
      )
      r <- runtimeService
        .deleteRuntime(
          DeleteRuntimeRequest(userInfo, GoogleProject(cloudContextGcp.asString), testRuntime.runtimeName, false)
        )
        .attempt
    } yield r shouldBe Left(ForbiddenError(userInfo.userEmail, Some(context.traceId)))

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete runtime records, update all status appropriately, and not queue messages" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted
    val publisherQueue = QueueFactory.makePublisherQueue()
    val runtimeService = makeRuntimeService(authProvider = allowListAuthProvider, publisherQueue = publisherQueue)
    val res = for {
      pd <- makePersistentDisk().save()
      testRuntime <- IO(
        makeCluster(1).saveWithRuntimeConfig(
          RuntimeConfig
            .GceWithPdConfig(
              MachineTypeName("n1-standard-4"),
              Some(pd.id),
              bootDiskSize = DiskSize(50),
              zone = ZoneName("us-central1-a"),
              None
            )
        )
      )

      listRuntimeResponse2 = ListRuntimeResponse2(
        testRuntime.id,
        None,
        testRuntime.samResource,
        testRuntime.runtimeName,
        testRuntime.cloudContext,
        testRuntime.auditInfo,
        gceRuntimeConfig,
        testRuntime.proxyUrl,
        testRuntime.status,
        testRuntime.labels,
        testRuntime.patchInProgress
      )

      _ <- runtimeService
        .deleteRuntimeRecords(userInfo, cloudContextGcp, listRuntimeResponse2)
        .attempt

      runtimeStatus <- clusterQuery
        .getClusterStatus(testRuntime.id)
        .transaction
      message <- publisherQueue.tryTake
    } yield {
      runtimeStatus shouldBe Some(RuntimeStatus.Deleted)
      message shouldBe None
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to delete runtime records if user loses project access" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted
    val runtimeService = makeRuntimeService(authProvider = allowListAuthProvider2)
    val res = for {
      context <- appContext.ask[AppContext]
      pd <- makePersistentDisk().save()
      testRuntime <- IO(
        makeCluster(1).saveWithRuntimeConfig(
          RuntimeConfig
            .GceWithPdConfig(
              MachineTypeName("n1-standard-4"),
              Some(pd.id),
              bootDiskSize = DiskSize(50),
              zone = ZoneName("us-central1-a"),
              None
            )
        )
      )

      listRuntimeResponse2 = ListRuntimeResponse2(
        testRuntime.id,
        None,
        testRuntime.samResource,
        testRuntime.runtimeName,
        testRuntime.cloudContext,
        testRuntime.auditInfo,
        gceRuntimeConfig,
        testRuntime.proxyUrl,
        testRuntime.status,
        testRuntime.labels,
        testRuntime.patchInProgress
      )

      r <- runtimeService
        .deleteRuntimeRecords(userInfo, cloudContextGcp, listRuntimeResponse2)
        .attempt
    } yield r shouldBe Left(
      RuntimeNotFoundException(cloudContextGcp, testRuntime.runtimeName, "Permission Denied", Some(context.traceId))
    )

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "deleteAllRuntimeRecords, update all status appropriately, and not queue messages" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted
    val runtimeIds =
      Vector(RuntimeSamResourceId(UUID.randomUUID.toString), RuntimeSamResourceId(UUID.randomUUID.toString))
    val mockAuthProvider = mockAuthorize(
      userInfo,
      readerRuntimeSamIds = Set(runtimeIds(0), runtimeIds(1)),
      readerProjectSamIds = Set(ProjectSamResourceId(project))
    )
    when(mockAuthProvider.isUserProjectReader(any, isEq(userInfo))(any)).thenReturn(IO.pure(true))
    when(
      mockAuthProvider.getActionsWithProjectFallback[RuntimeSamResourceId, RuntimeAction](any, any, isEq(userInfo))(any,
                                                                                                                    any
      )
    )
      .thenReturn(
        IO.pure(
          (List(RuntimeAction.GetRuntimeStatus, RuntimeAction.DeleteRuntime),
           List(ProjectAction.GetRuntimeStatus, ProjectAction.DeleteRuntime)
          )
        )
      )

    val publisherQueue = QueueFactory.makePublisherQueue()
    val service = makeRuntimeService(authProvider = mockAuthProvider, publisherQueue = publisherQueue)

    val res = for {
      pd1 <- makePersistentDisk().save()
      _ <- IO(
        makeCluster(0)
          .copy(samResource = runtimeIds(0), status = RuntimeStatus.Running)
          .saveWithRuntimeConfig(
            RuntimeConfig
              .GceWithPdConfig(
                MachineTypeName("n1-standard-4"),
                Some(pd1.id),
                bootDiskSize = DiskSize(50),
                zone = ZoneName("us-central1-a"),
                None
              )
          )
      )
      pd2 <- makePersistentDisk(Some(DiskName("disk2"))).save()
      _ <- IO(
        makeCluster(1)
          .copy(samResource = runtimeIds(1), status = RuntimeStatus.Running)
          .saveWithRuntimeConfig(
            RuntimeConfig
              .GceWithPdConfig(
                MachineTypeName("n1-standard-4"),
                Some(pd2.id),
                bootDiskSize = DiskSize(50),
                zone = ZoneName("us-central1-a"),
                None
              )
          )
      )

      _ <- service.deleteAllRuntimesRecords(userInfo, cloudContextGcp)

      runtimes <- service.listRuntimes(userInfo, Some(cloudContextGcp), Map("includeDeleted" -> "true"))
      messages <- publisherQueue.tryTakeN(Some(2))

    } yield {
      runtimes.map(_.status) shouldEqual List(RuntimeStatus.Deleted, RuntimeStatus.Deleted)
      messages shouldBe List.empty
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "deleteAll runtimes" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted
    val runtimeIds =
      Vector(RuntimeSamResourceId(UUID.randomUUID.toString), RuntimeSamResourceId(UUID.randomUUID.toString))
    val mockAuthProvider = mockAuthorize(
      userInfo,
      readerRuntimeSamIds = Set(runtimeIds(0), runtimeIds(1)),
      readerProjectSamIds = Set(ProjectSamResourceId(project))
    )
    when(mockAuthProvider.isUserProjectReader(any, isEq(userInfo))(any)).thenReturn(IO.pure(true))
    when(
      mockAuthProvider.getActionsWithProjectFallback[RuntimeSamResourceId, RuntimeAction](any, any, isEq(userInfo))(any,
                                                                                                                    any
      )
    )
      .thenReturn(
        IO.pure(
          (List(RuntimeAction.GetRuntimeStatus, RuntimeAction.DeleteRuntime),
           List(ProjectAction.GetRuntimeStatus, ProjectAction.DeleteRuntime)
          )
        )
      )

    val publisherQueue = QueueFactory.makePublisherQueue()
    val service = makeRuntimeService(authProvider = mockAuthProvider, publisherQueue = publisherQueue)

    val res = for {
      pd1 <- makePersistentDisk().save()
      _ <- IO(
        makeCluster(0)
          .copy(samResource = runtimeIds(0), status = RuntimeStatus.Running)
          .saveWithRuntimeConfig(
            RuntimeConfig
              .GceWithPdConfig(
                MachineTypeName("n1-standard-4"),
                Some(pd1.id),
                bootDiskSize = DiskSize(50),
                zone = ZoneName("us-central1-a"),
                None
              )
          )
      )
      pd2 <- makePersistentDisk(Some(DiskName("disk2"))).save()
      _ <- IO(
        makeCluster(1)
          .copy(samResource = runtimeIds(1), status = RuntimeStatus.Running)
          .saveWithRuntimeConfig(
            RuntimeConfig
              .GceWithPdConfig(
                MachineTypeName("n1-standard-4"),
                Some(pd2.id),
                bootDiskSize = DiskSize(50),
                zone = ZoneName("us-central1-a"),
                None
              )
          )
      )

      attachedDisksIdsOpt <- service.deleteAllRuntimes(userInfo, cloudContextGcp, false)
      attachedDisksIds = attachedDisksIdsOpt.getOrElse(Vector.empty)

      runtimes <- service.listRuntimes(userInfo, Some(cloudContextGcp), Map.empty)
      messages <- publisherQueue.tryTakeN(Some(2))

    } yield {
      attachedDisksIds.length shouldEqual 2
      runtimes.map(_.status) shouldEqual List(RuntimeStatus.PreDeleting, RuntimeStatus.PreDeleting)
      messages.map(_.messageType) shouldBe List(LeoPubsubMessageType.DeleteRuntime, LeoPubsubMessageType.DeleteRuntime)
      val deleteRuntimeMessages = messages.map(_.asInstanceOf[DeleteRuntimeMessage])
      deleteRuntimeMessages.map(_.runtimeId) shouldBe runtimes.map(_.id)
      deleteRuntimeMessages.map(_.persistentDiskToDelete) shouldBe List(None, None)
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail deleteAll runtimes if one runtime is in a non deletable state" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted
    val runtimeIds =
      Vector(RuntimeSamResourceId(UUID.randomUUID.toString), RuntimeSamResourceId(UUID.randomUUID.toString))
    val mockAuthProvider = mockAuthorize(
      userInfo,
      readerRuntimeSamIds = Set(runtimeIds(0), runtimeIds(1)),
      readerProjectSamIds = Set(ProjectSamResourceId(project))
    )
    when(mockAuthProvider.isUserProjectReader(any, isEq(userInfo))(any)).thenReturn(IO.pure(true))
    when(
      mockAuthProvider.getActionsWithProjectFallback[RuntimeSamResourceId, RuntimeAction](any, any, isEq(userInfo))(any,
                                                                                                                    any
      )
    )
      .thenReturn(
        IO.pure(
          (List(RuntimeAction.GetRuntimeStatus, RuntimeAction.DeleteRuntime),
           List(ProjectAction.GetRuntimeStatus, ProjectAction.DeleteRuntime)
          )
        )
      )

    val publisherQueue = QueueFactory.makePublisherQueue()
    val service = makeRuntimeService(authProvider = mockAuthProvider, publisherQueue = publisherQueue)

    val res = for {
      pd1 <- makePersistentDisk().save()
      _ <- IO(
        makeCluster(0)
          .copy(samResource = runtimeIds(0), status = RuntimeStatus.Deleting)
          .saveWithRuntimeConfig(
            RuntimeConfig
              .GceWithPdConfig(
                MachineTypeName("n1-standard-4"),
                Some(pd1.id),
                bootDiskSize = DiskSize(50),
                zone = ZoneName("us-central1-a"),
                None
              )
          )
      )
      pd2 <- makePersistentDisk(Some(DiskName("disk2"))).save()
      _ <- IO(
        makeCluster(1)
          .copy(samResource = runtimeIds(1), status = RuntimeStatus.Running)
          .saveWithRuntimeConfig(
            RuntimeConfig
              .GceWithPdConfig(
                MachineTypeName("n1-standard-4"),
                Some(pd2.id),
                bootDiskSize = DiskSize(50),
                zone = ZoneName("us-central1-a"),
                None
              )
          )
      )

      attachedDisksIdsOpt <- service.deleteAllRuntimes(userInfo, cloudContextGcp, false)
      attachedDisksIds = attachedDisksIdsOpt.getOrElse(Vector.empty)

      runtimes <- service.listRuntimes(userInfo, Some(cloudContextGcp), Map.empty)
      messages <- publisherQueue.tryTakeN(Some(2))

    } yield {
      attachedDisksIds.length shouldEqual 2
      runtimes.map(_.status) shouldEqual List(RuntimeStatus.Deleting, RuntimeStatus.Running)
      messages shouldBe List.empty
    }
    the[NonDeletableRuntimesInProjectFoundException] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "stop a runtime" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted

    val res = for {
      publisherQueue <- Queue.bounded[IO, LeoPubsubMessage](10)
      service = makeRuntimeService(publisherQueue)
      samResource <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      testRuntime <- IO(makeCluster(1).copy(samResource = samResource).save())

      _ <- service.stopRuntime(userInfo, testRuntime.cloudContext, testRuntime.runtimeName)
      res <- withLeoPublisher(publisherQueue) {
        for {
          dbRuntimeOpt <- clusterQuery
            .getActiveClusterByNameMinimal(testRuntime.cloudContext, testRuntime.runtimeName)
            .transaction
          message <- publisherQueue.tryTake
        } yield {
          dbRuntimeOpt.get.status shouldBe RuntimeStatus.Stopping
          message shouldBe None
        }
      }
    } yield res

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not stop a stopping runtime and also not error" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted

    val res = for {
      publisherQueue <- Queue.bounded[IO, LeoPubsubMessage](10)
      service = makeRuntimeService(publisherQueue)
      samResource <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      testRuntime <- IO(makeCluster(1).copy(samResource = samResource, status = RuntimeStatus.PreStopping).save())

      _ <- service.stopRuntime(userInfo, testRuntime.cloudContext, testRuntime.runtimeName)
      res <- withLeoPublisher(publisherQueue) {
        for {
          dbRuntimeOpt <- clusterQuery
            .getActiveClusterByNameMinimal(testRuntime.cloudContext, testRuntime.runtimeName)
            .transaction
          message <- publisherQueue.tryTake
        } yield {
          dbRuntimeOpt.get.status shouldBe RuntimeStatus.PreStopping
          message shouldBe None
        }
      }
    } yield res

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "start a runtime" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted

    val res = for {
      publisherQueue <- Queue.bounded[IO, LeoPubsubMessage](10)
      service = makeRuntimeService(publisherQueue)
      samResource <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      testRuntime <- IO(makeCluster(1).copy(samResource = samResource, status = RuntimeStatus.Stopped).save())

      _ <- service.startRuntime(userInfo, GoogleProject(testRuntime.cloudContext.asString), testRuntime.runtimeName)
      res <- withLeoPublisher(publisherQueue) {
        for {
          dbRuntimeOpt <- clusterQuery
            .getActiveClusterByNameMinimal(testRuntime.cloudContext, testRuntime.runtimeName)
            .transaction
          message <- publisherQueue.tryTake
        } yield {
          dbRuntimeOpt.get.status shouldBe RuntimeStatus.Starting
          message shouldBe None
        }
      }
    } yield res

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "update autopause" in isolatedDbTest {
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted

    val res = for {
      // remove some existing items in the queue just to be safe
      _ <- publisherQueue.tryTake
      _ <- publisherQueue.tryTake
      _ <- publisherQueue.tryTake
      samResource <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      testRuntime <- IO(makeCluster(1).copy(samResource = samResource, status = RuntimeStatus.Running).save())
      req = UpdateRuntimeRequest(None, false, Some(true), Some(120.minutes), Map.empty, Set.empty)

      _ <- runtimeService.updateRuntime(
        userInfo,
        GoogleProject(testRuntime.cloudContext.asString),
        testRuntime.runtimeName,
        req
      )
      dbRuntimeOpt <- clusterQuery
        .getActiveClusterByNameMinimal(testRuntime.cloudContext, testRuntime.runtimeName)
        .transaction
      dbRuntime = dbRuntimeOpt.get
      messageOpt <- publisherQueue.tryTake
    } yield {
      dbRuntime.autopauseThreshold shouldBe 120
      messageOpt shouldBe None
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  val reqMaps: List[LabelMap] = List(
    Map.empty,
    Map("apples" -> "are_great", "grapes" -> "are_cool"),
    Map("new_entry" -> "i_am_new")
  )

  val startLabelMap: LabelMap = Map("apples" -> "to_oranges", "grapes" -> "make_wine")

  val finalUpsertMaps: List[LabelMap] = List(
    Map("apples" -> "to_oranges", "grapes" -> "make_wine"),
    Map("apples" -> "are_great", "grapes" -> "are_cool"),
    Map("apples" -> "to_oranges", "grapes" -> "make_wine", "new_entry" -> "i_am_new")
  )

  reqMaps.foreach { upsertLabels =>
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted

    it should s"Process upsert labels correctly for $upsertLabels" in isolatedDbTest {
      val res = for {
        samResource <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
        testRuntime <- IO(
          makeCluster(1)
            .copy(samResource = samResource, status = RuntimeStatus.Running, labels = startLabelMap)
            .save()
        )
        req = UpdateRuntimeRequest(None, false, Some(true), Some(120.minutes), upsertLabels, Set.empty)
        _ <- runtimeService.updateRuntime(
          userInfo,
          GoogleProject(testRuntime.cloudContext.asString),
          testRuntime.runtimeName,
          req
        )
        dbLabelMap <- labelQuery
          .getAllForResource(testRuntime.id, LabelResourceType.runtime)(scala.concurrent.ExecutionContext.global)
          .transaction
        _ <- publisherQueue.tryTake

      } yield finalUpsertMaps.contains(dbLabelMap) shouldBe true
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  val deleteLists: List[Set[String]] = List(
    Set(""),
    Set("apples"),
    Set("apples", "oranges"),
    Set("apples", "grapes")
  )

  val finalDeleteMaps: List[LabelMap] = List(
    Map("grapes" -> "make_wine"),
    Map("apples" -> "to_oranges", "grapes" -> "make_wine"),
    Map.empty
  )

  deleteLists.foreach { deleteLabelSet =>
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted

    it should s"Process reqlabels correctly for $deleteLabelSet" in isolatedDbTest {
      val res = for {
        samResource <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
        testRuntime <- IO(
          makeCluster(1)
            .copy(samResource = samResource, status = RuntimeStatus.Running, labels = startLabelMap)
            .save()
        )
        req = UpdateRuntimeRequest(None, false, Some(true), Some(120.minutes), Map.empty, deleteLabelSet)
        _ <- runtimeService.updateRuntime(
          userInfo,
          GoogleProject(testRuntime.cloudContext.asString),
          testRuntime.runtimeName,
          req
        )
        dbLabelMap <- labelQuery.getAllForResource(testRuntime.id, LabelResourceType.runtime).transaction
        _ <- publisherQueue.tryTake
      } yield finalDeleteMaps.contains(dbLabelMap) shouldBe true
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  List(RuntimeStatus.Creating, RuntimeStatus.Stopping, RuntimeStatus.Deleting, RuntimeStatus.Starting).foreach {
    status =>
      val userInfo = UserInfo(
        OAuth2BearerToken(""),
        WorkbenchUserId("userId"),
        WorkbenchEmail("user1@example.com"),
        0
      ) // this email is allowlisted
      it should s"fail to update a runtime in $status status" in isolatedDbTest {
        val res = for {
          samResource <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
          testRuntime <- IO(makeCluster(1).copy(samResource = samResource, status = status).save())
          req = UpdateRuntimeRequest(None, false, Some(true), Some(120.minutes), Map.empty, Set.empty)
          fail <- runtimeService
            .updateRuntime(userInfo, GoogleProject(testRuntime.cloudContext.asString), testRuntime.runtimeName, req)
            .attempt
        } yield fail shouldBe Left(RuntimeCannotBeUpdatedException(testRuntime.projectNameString, testRuntime.status))
        res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      }
  }

  "RuntimeServiceInterp.processUpdateRuntimeConfigRequest" should "fail to update the wrong cloud service type" in {
    val req = UpdateRuntimeConfigRequest.DataprocConfig(None, Some(DiskSize(100)), None, None)
    val res = for {
      ctx <- appContext.ask[AppContext]
      fail <- runtimeService.processUpdateRuntimeConfigRequest(req, false, testClusterRecord, gceRuntimeConfig).attempt
    } yield fail shouldBe Left(
      WrongCloudServiceException(CloudService.GCE, CloudService.Dataproc, ctx.traceId)
    )
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  "RuntimeServiceInterp.processUpdateGceConfigRequest" should "not update a GCE runtime when there are no changes" in {
    val req = UpdateRuntimeConfigRequest.GceConfig(Some(gceRuntimeConfig.machineType), Some(gceRuntimeConfig.diskSize))
    val res = for {
      _ <- runtimeService.processUpdateRuntimeConfigRequest(req, false, testClusterRecord, gceRuntimeConfig)
      messageOpt <- publisherQueue.tryTake
    } yield messageOpt shouldBe None
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "update patchInProgress flag if stopToUpdateMachineType is true" in isolatedDbTest {
    val req =
      UpdateRuntimeConfigRequest.GceConfig(Some(MachineTypeName("n1-standard-8")), Some(gceRuntimeConfig.diskSize))
    val runtime = testCluster.copy(status = RuntimeStatus.Running)
    val res = for {
      ctx <- appContext.ask[AppContext]
      savedRuntime <- IO(runtime.save())
      clusterRecordOpt <- clusterQuery
        .getActiveClusterRecordByName(runtime.cloudContext, runtime.runtimeName)
        .transaction
      _ <- runtimeService.processUpdateRuntimeConfigRequest(
        req,
        true,
        clusterRecordOpt.getOrElse(throw new Exception(s"cluster ${savedRuntime.projectNameString} not found")),
        gceRuntimeConfig
      )
      patchInProgress <- patchQuery.isInprogress(savedRuntime.id).transaction
      message <- publisherQueue.take
    } yield {
      patchInProgress shouldBe true
      message shouldBe UpdateRuntimeMessage(
        savedRuntime.id,
        Some(MachineTypeName("n1-standard-8")),
        true,
        None,
        None,
        None,
        Some(ctx.traceId)
      )
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "update a GCE machine type in Stopped state" in {
    val req = UpdateRuntimeConfigRequest.GceConfig(Some(MachineTypeName("n1-micro-2")), None)
    val res = for {
      ctx <- appContext.ask[AppContext]
      _ <- runtimeService.processUpdateRuntimeConfigRequest(
        req,
        false,
        testClusterRecord.copy(status = RuntimeStatus.Stopped),
        gceRuntimeConfig
      )
      message <- publisherQueue.take
    } yield message shouldBe UpdateRuntimeMessage(
      testClusterRecord.id,
      Some(MachineTypeName("n1-micro-2")),
      false,
      None,
      None,
      None,
      Some(ctx.traceId)
    )
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "update a GCE machine type in Running state" in isolatedDbTest {
    val req = UpdateRuntimeConfigRequest.GceConfig(Some(MachineTypeName("n1-micro-2")), None)
    val res = for {
      ctx <- appContext.ask[AppContext]
      runtime <- IO(makeCluster(1).save())
      cr <- clusterQuery.getActiveClusterRecordByName(runtime.cloudContext, runtime.runtimeName).transaction
      _ <- runtimeService.processUpdateRuntimeConfigRequest(
        req,
        true,
        cr.get.copy(status = RuntimeStatus.Running),
        gceRuntimeConfig
      )
      message <- publisherQueue.take
    } yield message shouldBe UpdateRuntimeMessage(
      runtime.id,
      Some(MachineTypeName("n1-micro-2")),
      true,
      None,
      None,
      None,
      Some(ctx.traceId)
    )
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to update a GCE machine type in Running state with allowStop set to false" in {
    val req = UpdateRuntimeConfigRequest.GceConfig(Some(MachineTypeName("n1-micro-2")), None)
    val res = for {
      _ <- runtimeService.processUpdateRuntimeConfigRequest(
        req,
        false,
        testClusterRecord.copy(status = RuntimeStatus.Running),
        gceRuntimeConfig
      )
    } yield ()
    res.attempt.unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldBe Left(
      RuntimeMachineTypeCannotBeChangedException(testClusterRecord.projectNameString, RuntimeStatus.Running)
    )
  }

  it should "increase the disk on a GCE runtime" in isolatedDbTest {
    val req = UpdateRuntimeConfigRequest.GceConfig(None, Some(DiskSize(1024)))
    val res = for {
      ctx <- appContext.ask[AppContext]
      runtime <- IO(makeCluster(1).save())
      cr <- clusterQuery.getActiveClusterRecordByName(runtime.cloudContext, runtime.runtimeName).transaction
      _ <- runtimeService.processUpdateRuntimeConfigRequest(req, false, cr.get, gceRuntimeConfig)
      message <- publisherQueue.take
    } yield message shouldBe UpdateRuntimeMessage(
      runtime.id,
      None,
      true,
      Some(DiskUpdate.NoPdSizeUpdate(DiskSize(1024))),
      None,
      None,
      Some(ctx.traceId)
    )
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "increase the persistent disk is attached to a GCE runtime" in isolatedDbTest {
    val disk = makePersistentDisk(None).save().unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val req = UpdateRuntimeConfigRequest.GceConfig(None, Some(DiskSize(1024)))
    val res = for {
      ctx <- appContext.ask[AppContext]
      runtime <- IO(makeCluster(1).save())
      cr <- clusterQuery.getActiveClusterRecordByName(runtime.cloudContext, runtime.runtimeName).transaction
      _ <- runtimeService.processUpdateRuntimeConfigRequest(
        req,
        true,
        cr.get,
        gceWithPdRuntimeConfig.copy(persistentDiskId = Some(disk.id))
      )
      message <- publisherQueue.take
    } yield message shouldBe UpdateRuntimeMessage(
      runtime.id,
      None,
      true,
      Some(DiskUpdate.PdSizeUpdate(disk.id, disk.name, DiskSize(1024))),
      None,
      None,
      Some(ctx.traceId)
    )
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to increase the disk on a GCE runtime if allowStop is false" in {
    val disk = makePersistentDisk(None).save().unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val req = UpdateRuntimeConfigRequest.GceConfig(None, Some(DiskSize(1024)))
    val res = for {
      _ <- runtimeService.processUpdateRuntimeConfigRequest(
        req,
        false,
        testClusterRecord,
        gceWithPdRuntimeConfig.copy(persistentDiskId = Some(disk.id))
      )
    } yield ()
    res.attempt.unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldBe Left(
      RuntimeDiskSizeCannotBeChangedException(testCluster.projectNameString)
    )
  }

  it should "fail to decrease the disk on a GCE runtime" in {
    val req = UpdateRuntimeConfigRequest.GceConfig(None, Some(DiskSize(50)))
    val res = for {
      _ <- runtimeService.processUpdateRuntimeConfigRequest(req, false, testClusterRecord, gceRuntimeConfig)
    } yield ()
    res.attempt.unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldBe Left(
      RuntimeDiskSizeCannotBeDecreasedException(testClusterRecord.projectNameString)
    )
  }

  "RuntimeServiceInterp.processUpdateDataprocConfigRequest" should "not update a Dataproc runtime when there are no changes" in isolatedDbTest {
    val req = UpdateRuntimeConfigRequest.DataprocConfig(
      Some(defaultDataprocRuntimeConfig.masterMachineType),
      Some(defaultDataprocRuntimeConfig.masterDiskSize),
      Some(defaultDataprocRuntimeConfig.numberOfWorkers),
      defaultDataprocRuntimeConfig.numberOfPreemptibleWorkers
    )
    val res = for {
      ctx <- appContext.ask[AppContext]
      _ <- IO(
        testCluster.saveWithRuntimeConfig(
          defaultDataprocRuntimeConfig,
          dataprocInstances = List(
            DataprocInstance(
              DataprocInstanceKey(GoogleProject(testCluster.cloudContext.asString), zone, InstanceName("instance-0")),
              1,
              GceInstanceStatus.Running,
              Some(IP("")),
              DataprocRole.Master,
              ctx.now
            )
          )
        )
      )
      clusterRecord <- clusterQuery
        .getActiveClusterRecordByName(testCluster.cloudContext, testCluster.runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      _ <- runtimeService.processUpdateDataprocConfigRequest(
        req,
        false,
        clusterRecord.get,
        defaultDataprocRuntimeConfig
      )
      messageOpt <- publisherQueue.tryTake
    } yield messageOpt shouldBe None
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "disallow updating dataproc cluster number of workers if runtime is not Running" in {
    val req = UpdateRuntimeConfigRequest.DataprocConfig(None, None, Some(50), None)
    val res = for {
      ctx <- appContext.ask[AppContext]
      res <- runtimeService
        .processUpdateDataprocConfigRequest(
          req,
          false,
          testClusterRecord.copy(status = RuntimeStatus.Starting),
          defaultDataprocRuntimeConfig
        )
        .attempt
    } yield {
      val expectedException = new LeoException(
        s"Bad request. Number of workers can only be updated if the dataproc cluster is Running. Cluster is in Starting currently",
        StatusCodes.BadRequest,
        traceId = Option(ctx.traceId)
      )
      res.swap.toOption
        .getOrElse(throw new Exception("this test failed"))
        .asInstanceOf[LeoException] shouldEqual expectedException
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "update Dataproc workers and preemptibles" in isolatedDbTest {
    val req = UpdateRuntimeConfigRequest.DataprocConfig(None, None, Some(50), Some(1000))
    val res = for {
      ctx <- appContext.ask[AppContext]
      cluster = testCluster.copy(
        status = RuntimeStatus.Running
      )
      _ <- IO(
        cluster.saveWithRuntimeConfig(
          defaultDataprocRuntimeConfig,
          dataprocInstances = List(
            DataprocInstance(
              DataprocInstanceKey(GoogleProject(testCluster.cloudContext.asString), zone, InstanceName("instance-0")),
              1,
              GceInstanceStatus.Running,
              Some(IP("")),
              DataprocRole.Master,
              ctx.now
            )
          )
        )
      )
      clusterRecord <- clusterQuery
        .getActiveClusterRecordByName(cluster.cloudContext, cluster.runtimeName)
        .transaction
      _ <- runtimeService.processUpdateDataprocConfigRequest(
        req,
        false,
        clusterRecord.get,
        defaultDataprocRuntimeConfig
      )
      message <- publisherQueue.take
    } yield message shouldBe UpdateRuntimeMessage(
      clusterRecord.get.id,
      None,
      false,
      None,
      Some(50),
      Some(1000),
      Some(ctx.traceId)
    )
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "update a Dataproc master machine type in Stopped state" in isolatedDbTest {
    val req = UpdateRuntimeConfigRequest.DataprocConfig(Some(MachineTypeName("n1-micro-2")), None, None, None)
    val res = for {
      ctx <- appContext.ask[AppContext]
      _ <- IO(
        testCluster.saveWithRuntimeConfig(
          defaultDataprocRuntimeConfig,
          dataprocInstances = List(
            DataprocInstance(
              DataprocInstanceKey(GoogleProject(testCluster.cloudContext.asString), zone, InstanceName("instance-0")),
              1,
              GceInstanceStatus.Running,
              Some(IP("")),
              DataprocRole.Master,
              ctx.now
            )
          )
        )
      )
      clusterRecord <- clusterQuery
        .getActiveClusterRecordByName(testCluster.cloudContext, testCluster.runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      _ <- runtimeService.processUpdateDataprocConfigRequest(
        req,
        false,
        clusterRecord.get.copy(status = RuntimeStatus.Stopped),
        defaultDataprocRuntimeConfig
      )
      message <- publisherQueue.take
    } yield message shouldBe UpdateRuntimeMessage(
      clusterRecord.get.id,
      Some(MachineTypeName("n1-micro-2")),
      false,
      None,
      None,
      None,
      Some(ctx.traceId)
    )
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "update a Dataproc machine type in Running state" in isolatedDbTest {
    val req = UpdateRuntimeConfigRequest.DataprocConfig(Some(MachineTypeName("n1-micro-2")), None, None, None)
    val res = for {
      ctx <- appContext.ask[AppContext]
      _ <- IO(
        testCluster.saveWithRuntimeConfig(
          defaultDataprocRuntimeConfig,
          dataprocInstances = List(
            DataprocInstance(
              DataprocInstanceKey(GoogleProject(testCluster.cloudContext.asString), zone, InstanceName("instance-0")),
              1,
              GceInstanceStatus.Running,
              Some(IP("")),
              DataprocRole.Master,
              ctx.now
            )
          )
        )
      )
      clusterRecord <- clusterQuery
        .getActiveClusterRecordByName(testCluster.cloudContext, testCluster.runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      _ <- runtimeService.processUpdateDataprocConfigRequest(
        req,
        true,
        clusterRecord.get.copy(status = RuntimeStatus.Running),
        defaultDataprocRuntimeConfig
      )
      message <- publisherQueue.take
    } yield message shouldBe UpdateRuntimeMessage(
      clusterRecord.get.id,
      Some(MachineTypeName("n1-micro-2")),
      true,
      None,
      None,
      None,
      Some(ctx.traceId)
    )
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to update a Dataproc machine type in Running state with allowStop set to false" in {
    val req = UpdateRuntimeConfigRequest.DataprocConfig(Some(MachineTypeName("n1-micro-2")), None, None, None)
    val res = for {
      _ <- runtimeService.processUpdateDataprocConfigRequest(
        req,
        false,
        testClusterRecord.copy(status = RuntimeStatus.Running),
        defaultDataprocRuntimeConfig
      )
    } yield ()
    res.attempt.unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldBe Left(
      RuntimeMachineTypeCannotBeChangedException(testClusterRecord.projectNameString, RuntimeStatus.Running)
    )
  }

  it should "fail to update a Dataproc machine type when workers are also updated" in {
    val req = UpdateRuntimeConfigRequest.DataprocConfig(Some(MachineTypeName("n1-micro-2")), None, Some(50), None)
    val res = for {
      _ <- runtimeService.processUpdateDataprocConfigRequest(
        req,
        false,
        testClusterRecord.copy(status = RuntimeStatus.Running),
        defaultDataprocRuntimeConfig
      )
    } yield ()
    res.attempt.unsafeRunSync()(cats.effect.unsafe.IORuntime.global).isLeft shouldBe true
  }

  it should "increase the disk on a Dataproc runtime" in isolatedDbTest {
    val req = UpdateRuntimeConfigRequest.DataprocConfig(None, Some(DiskSize(1024)), None, None)
    val res = for {
      ctx <- appContext.ask[AppContext]
      masterInstance = DataprocInstance(
        DataprocInstanceKey(GoogleProject(testCluster.cloudContext.asString), zone, InstanceName("instance-0")),
        1,
        GceInstanceStatus.Running,
        Some(IP("")),
        DataprocRole.Master,
        ctx.now
      )
      _ <- IO(
        testCluster.saveWithRuntimeConfig(
          defaultDataprocRuntimeConfig,
          dataprocInstances = List(
            masterInstance
          )
        )
      )
      clusterRecord <- clusterQuery
        .getActiveClusterRecordByName(testCluster.cloudContext, testCluster.runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      _ <- runtimeService.processUpdateDataprocConfigRequest(req, true, clusterRecord.get, defaultDataprocRuntimeConfig)
      message <- publisherQueue.take
    } yield message shouldBe UpdateRuntimeMessage(
      clusterRecord.get.id,
      None,
      true,
      Some(DiskUpdate.Dataproc(DiskSize(1024), masterInstance)),
      None,
      None,
      Some(ctx.traceId)
    )
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to decrease the disk on a Dataproc runtime" in isolatedDbTest {
    val req = UpdateRuntimeConfigRequest.DataprocConfig(None, Some(DiskSize(50)), None, None)
    val res = for {
      ctx <- appContext.ask[AppContext]
      _ <- IO(
        testCluster.saveWithRuntimeConfig(
          defaultDataprocRuntimeConfig,
          dataprocInstances = List(
            DataprocInstance(
              DataprocInstanceKey(GoogleProject(testCluster.cloudContext.asString), zone, InstanceName("instance-0")),
              1,
              GceInstanceStatus.Running,
              Some(IP("")),
              DataprocRole.Master,
              ctx.now
            )
          )
        )
      )
      clusterRecord <- clusterQuery
        .getActiveClusterRecordByName(testCluster.cloudContext, testCluster.runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      _ <- runtimeService.processUpdateDataprocConfigRequest(req, true, clusterRecord.get, defaultDataprocRuntimeConfig)
    } yield ()
    res.attempt.unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldBe Left(
      RuntimeDiskSizeCannotBeDecreasedException(testClusterRecord.projectNameString)
    )
  }

  "RuntimeServiceInterp.processDiskConfigRequest" should "process a create disk request" in isolatedDbTest {
    val req = PersistentDiskRequest(diskName, Some(DiskSize(500)), None, Map("foo" -> "bar"))
    val res = for {
      context <- appContext.ask[AppContext]
      diskResult <- RuntimeServiceInterp.processPersistentDiskRequest(
        req,
        zone,
        project,
        userInfo,
        userEmail,
        serviceAccount,
        FormattedBy.GCE,
        allowListAuthProvider,
        MockSamService,
        ConfigReader.appConfig.persistentDisk,
        Some(workspaceId)
      )(implicitly, implicitly, implicitly, scala.concurrent.ExecutionContext.global)
      disk = diskResult.disk
      persistedDisk <- persistentDiskQuery
        .getById(disk.id)(scala.concurrent.ExecutionContext.global)
        .transaction
    } yield {
      diskResult.creationNeeded shouldBe true
      disk.cloudContext shouldBe cloudContextGcp
      disk.zone shouldBe ConfigReader.appConfig.persistentDisk.defaultZone
      disk.name shouldBe diskName
      disk.status shouldBe DiskStatus.Creating
      disk.auditInfo.creator shouldBe userInfo.userEmail
      disk.auditInfo.createdDate shouldBe context.now
      disk.auditInfo.destroyedDate shouldBe None
      disk.auditInfo.dateAccessed shouldBe context.now
      disk.size shouldBe DiskSize(500)
      disk.diskType shouldBe ConfigReader.appConfig.persistentDisk.defaultDiskType
      disk.blockSize shouldBe ConfigReader.appConfig.persistentDisk.defaultBlockSizeBytes
      disk.labels shouldBe DefaultDiskLabels(
        diskName,
        cloudContextGcp,
        userInfo.userEmail,
        serviceAccount
      ).toMap ++ Map(
        "foo" -> "bar"
      )

      persistedDisk shouldBe defined
      persistedDisk.get shouldEqual disk
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "reject a request if requested PD's zone is different from target zone" in isolatedDbTest {
    val req = PersistentDiskRequest(diskName, Some(DiskSize(500)), None, Map("foo" -> "bar"))
    val res = for {
      context <- appContext.ask[AppContext]
      _ <- makePersistentDisk(Some(req.name), Some(FormattedBy.GCE), None, Some(zone), Some(cloudContextGcp)).save()
      // save a PD with default zone
      targetZone = ZoneName("europe-west2-c")
      diskResult <- RuntimeServiceInterp
        .processPersistentDiskRequest(
          req,
          targetZone,
          project,
          userInfo,
          userEmail,
          serviceAccount,
          FormattedBy.GCE,
          allowListAuthProvider,
          MockSamService,
          ConfigReader.appConfig.persistentDisk,
          Some(workspaceId)
        )(implicitly, implicitly, implicitly, scala.concurrent.ExecutionContext.global)
        .attempt
    } yield diskResult shouldBe (Left(
      BadRequestException(
        s"existing disk Gcp/${project.value}/${req.name.value} is in zone ${zone.value}, and cannot be attached to a runtime in zone ${targetZone.value}. Please create your runtime in zone us-central1-a if you'd like to use this disk; or opt to use a new disk",
        Some(context.traceId)
      )
    ))

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "persist non-default zone disk properly" in isolatedDbTest {
    val req = PersistentDiskRequest(diskName, Some(DiskSize(500)), None, Map("foo" -> "bar"))
    val targetZone = ZoneName("europe-west2-c")

    val res = for {
      diskResult <- RuntimeServiceInterp
        .processPersistentDiskRequest(
          req,
          targetZone,
          project,
          userInfo,
          userEmail,
          serviceAccount,
          FormattedBy.GCE,
          allowListAuthProvider,
          MockSamService,
          ConfigReader.appConfig.persistentDisk,
          Some(workspaceId)
        )(implicitly, implicitly, implicitly, scala.concurrent.ExecutionContext.global)
      persistedDisk <- persistentDiskQuery
        .getById(diskResult.disk.id)(scala.concurrent.ExecutionContext.global)
        .transaction
    } yield persistedDisk.get.zone shouldBe targetZone

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "return existing disk if a disk with the same name already exists" in isolatedDbTest {
    val res = for {
      t <- appContext.ask[AppContext]
      disk <- makePersistentDisk(None).save()
      req = PersistentDiskRequest(disk.name, Some(DiskSize(50)), None, Map("foo" -> "bar"))
      returnedDisk <- RuntimeServiceInterp
        .processPersistentDiskRequest(
          req,
          zone,
          project,
          userInfo,
          userEmail,
          serviceAccount,
          FormattedBy.GCE,
          allowListAuthProvider,
          MockSamService,
          ConfigReader.appConfig.persistentDisk,
          Some(workspaceId)
        )(implicitly, implicitly, implicitly, scala.concurrent.ExecutionContext.global)
        .attempt
    } yield returnedDisk shouldBe Right(PersistentDiskRequestResult(disk, false))

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to create a disk when caller has no permission" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("badUser"), WorkbenchEmail("badEmail"), 0)
    val req = PersistentDiskRequest(diskName, Some(DiskSize(500)), None, Map("foo" -> "bar"))

    val thrown = the[ForbiddenError] thrownBy {
      RuntimeServiceInterp
        .processPersistentDiskRequest(
          req,
          zone,
          project,
          userInfo,
          userEmail,
          serviceAccount,
          FormattedBy.GCE,
          allowListAuthProvider,
          MockSamService,
          ConfigReader.appConfig.persistentDisk,
          Some(workspaceId)
        )(implicitly, implicitly, implicitly, scala.concurrent.ExecutionContext.global)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

    thrown shouldBe ForbiddenError(userInfo.userEmail)
  }

  it should "fail to process a disk reference when the disk is already attached" in isolatedDbTest {
    val res = for {
      t <- appContext.ask[AppContext]
      savedDisk <- makePersistentDisk(None).save()
      _ <- IO(
        makeCluster(1).saveWithRuntimeConfig(
          RuntimeConfig.GceWithPdConfig(
            defaultMachineType,
            Some(savedDisk.id),
            bootDiskSize = DiskSize(50),
            ZoneName("us-central1-a"),
            None
          )
        )
      )
      req = PersistentDiskRequest(savedDisk.name, Some(savedDisk.size), Some(savedDisk.diskType), savedDisk.labels)
      err <- RuntimeServiceInterp
        .processPersistentDiskRequest(
          req,
          zone,
          project,
          userInfo,
          userEmail,
          serviceAccount,
          FormattedBy.GCE,
          allowListAuthProvider,
          MockSamService,
          ConfigReader.appConfig.persistentDisk,
          Some(workspaceId)
        )(implicitly, implicitly, implicitly, scala.concurrent.ExecutionContext.global)
        .attempt
    } yield err shouldBe Left(DiskAlreadyAttachedException(CloudContext.Gcp(project), savedDisk.name, t.traceId))

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to process a disk reference when the disk is already formatted by another app" in isolatedDbTest {
    val res = for {
      t <- appContext.ask[AppContext]
      gceDisk <- makePersistentDisk(Some(DiskName("gceDisk")), Some(FormattedBy.GCE)).save()
      req = PersistentDiskRequest(gceDisk.name, Some(gceDisk.size), Some(gceDisk.diskType), gceDisk.labels)
      formatGceDiskError <- RuntimeServiceInterp
        .processPersistentDiskRequest(
          req,
          zone,
          project,
          userInfo,
          userEmail,
          serviceAccount,
          FormattedBy.Galaxy,
          allowListAuthProvider,
          MockSamService,
          ConfigReader.appConfig.persistentDisk,
          Some(workspaceId)
        )(implicitly, implicitly, implicitly, scala.concurrent.ExecutionContext.global)
        .attempt
      galaxyDisk <- makePersistentDisk(Some(DiskName("galaxyDisk")), Some(FormattedBy.Galaxy)).save()
      req = PersistentDiskRequest(galaxyDisk.name, Some(galaxyDisk.size), Some(galaxyDisk.diskType), galaxyDisk.labels)
      formatGalaxyDiskError <- RuntimeServiceInterp
        .processPersistentDiskRequest(
          req,
          zone,
          project,
          userInfo,
          userEmail,
          serviceAccount,
          FormattedBy.GCE,
          allowListAuthProvider,
          MockSamService,
          ConfigReader.appConfig.persistentDisk,
          Some(workspaceId)
        )(implicitly, implicitly, implicitly, scala.concurrent.ExecutionContext.global)
        .attempt
    } yield {
      formatGceDiskError shouldBe Left(
        DiskAlreadyFormattedByOtherApp(CloudContext.Gcp(project), gceDisk.name, t.traceId, FormattedBy.GCE)
      )
      formatGalaxyDiskError shouldBe Left(
        DiskAlreadyFormattedByOtherApp(CloudContext.Gcp(project), galaxyDisk.name, t.traceId, FormattedBy.Galaxy)
      )
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to attach a disk when caller has no attach permission" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("badUser"), WorkbenchEmail("badEmail"), 0)
    val res = for {
      savedDisk <- makePersistentDisk(None).save()
      req = PersistentDiskRequest(savedDisk.name, Some(savedDisk.size), Some(savedDisk.diskType), savedDisk.labels)
      _ <- RuntimeServiceInterp.processPersistentDiskRequest(
        req,
        zone,
        project,
        userInfo,
        userEmail,
        serviceAccount,
        FormattedBy.GCE,
        allowListAuthProvider,
        MockSamService,
        ConfigReader.appConfig.persistentDisk,
        Some(workspaceId)
      )(implicitly, implicitly, implicitly, scala.concurrent.ExecutionContext.global)
    } yield ()

    val thrown = the[ForbiddenError] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

    thrown shouldBe ForbiddenError(userInfo.userEmail)
  }

  it should "test getToolFromImages - get the Jupyter tool from list of cluster images" in isolatedDbTest {
    val tool = getToolFromImages(Set(jupyterImage, welderImage, proxyImage, cryptoDetectorImage))
    tool shouldBe Some(Tool.Jupyter)
  }

  it should "test getToolFromImages - get the RStudio tool from list of cluster images" in isolatedDbTest {
    val tool = getToolFromImages(Set(rstudioImage, welderImage, proxyImage, cryptoDetectorImage))
    tool shouldBe Some(Tool.RStudio)
  }

  private def withLeoPublisher(
    publisherQueue: Queue[IO, LeoPubsubMessage]
  )(validations: IO[Assertion]): IO[Assertion] = {

    val mockCloudPublisher = mock[CloudPublisher[IO]]
    def noOpPipe[A]: Pipe[IO, A, Unit] = _.evalMap(_ => IO.unit)
    when(mockCloudPublisher.publish[LeoPubsubMessage](any)).thenReturn(noOpPipe)
    when(mockCloudPublisher.publishOne[LeoPubsubMessage](any, any)(any, any)).thenReturn(IO.unit)

    val leoPublisher = new LeoPublisher[IO](publisherQueue, mockCloudPublisher)(
      implicitly,
      implicitly,
      implicitly,
      scala.concurrent.ExecutionContext.global,
      loggerIO
    )
    withInfiniteStream(leoPublisher.process, validations)
  }

}
