package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.IO
import cats.effect.std.Queue
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.RuntimeSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.dao.sam.{SamException, SamService}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model.SamResource.RuntimeSamResource
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{StartRuntimeMessage, StopRuntimeMessage}
import org.broadinstitute.dsde.workbench.leonardo.util.QueueFactory
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.mockito.ArgumentMatchers.{any, eq => isEq}
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global

class RuntimeV2ServiceInterpSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent with MockitoSugar {

  def makeInterp(queue: Queue[IO, LeoPubsubMessage] = QueueFactory.makePublisherQueue(),
                 samService: SamService[IO] = MockSamService
  ) = new RuntimeV2ServiceInterp[IO](queue, samService)

  def setRuntimeDeleted(workspaceId: WorkspaceId, name: RuntimeName): IO[Long] =
    for {
      now <- IO.realTimeInstant
      runtime <- RuntimeServiceDbQueries
        .getRuntimeByWorkspaceId(workspaceId, name)
        .transaction

      _ <- clusterQuery
        .completeDeletion(runtime.id, now)
        .transaction
    } yield runtime.id

  def mockUserInfo(email: String = userEmail.toString()): UserInfo =
    UserInfo(OAuth2BearerToken(""), WorkbenchUserId(s"userId-${email}"), WorkbenchEmail(email), 0)

  val runtimeV2Service =
    new RuntimeV2ServiceInterp[IO](
      QueueFactory.makePublisherQueue(),
      MockSamService
    )

  val runtimeV2Service2 =
    new RuntimeV2ServiceInterp[IO](
      QueueFactory.makePublisherQueue(),
      MockSamService
    )

  it should "publish start a runtime message properly" in isolatedDbTest {
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)
    val res = for {
      ctx <- appContext.ask[AppContext]
      runtime <- IO(
        makeCluster(0)
          .copy(
            status = RuntimeStatus.Stopped,
            workspaceId = Some(workspaceId),
            auditInfo = auditInfo.copy(creator = userInfo.userEmail)
          )
          .save()
      )
      _ <- azureService
        .startRuntime(userInfo, runtime.runtimeName, runtime.workspaceId.get)
      msg <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with

    } yield msg shouldBe Some(StartRuntimeMessage(runtime.id, Some(ctx.traceId)))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to start a runtime if permission denied" in isolatedDbTest {
    // User is runtime creator, but does not have access to the workspace
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("user"), WorkbenchEmail("email"), 0)
    val workspaceId = WorkspaceId(UUID.randomUUID())
    val samService = mock[SamService[IO]]
    when(
      samService.checkAuthorized(isEq(userInfo.accessToken.token), any(), isEq(RuntimeAction.StopStartRuntime))(any())
    )
      .thenReturn(IO.raiseError(SamException.create("no access", StatusCodes.Forbidden.intValue, TraceId(""))))
    when(
      samService.checkAuthorized(isEq(userInfo.accessToken.token), any(), isEq(RuntimeAction.GetRuntimeStatus))(any())
    ).thenReturn(IO.unit)
    val interp = makeInterp(samService = samService)

    val res = for {
      runtime <- IO(
        makeCluster(0)
          .copy(
            status = RuntimeStatus.Running,
            workspaceId = Some(workspaceId),
            auditInfo = auditInfo.copy(creator = userInfo.userEmail)
          )
          .save()
      )
      r <- interp
        .startRuntime(userInfo, runtime.runtimeName, runtime.workspaceId.get)
        .attempt
    } yield {
      val exception = r.swap.toOption.get
      exception.getMessage shouldBe s"email is unauthorized. If you have proper permissions to use the workspace, make sure you are also added to the billing account"
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to start a runtime when runtime doesn't exist in DB" in isolatedDbTest {
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val res =
      runtimeV2Service
        .startRuntime(userInfo, runtimeName, workspaceId)
        .attempt
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val exception = res.swap.toOption.get
    exception.isInstanceOf[RuntimeNotFoundByWorkspaceIdException] shouldBe true
    exception.getMessage shouldBe s"Runtime clusterName1 not found in workspace ${workspaceId.value}"
  }

  it should "fail to start a runtime when runtime is not in startable statuses" in isolatedDbTest {
    val res = for {
      runtime <- IO(
        makeCluster(0)
          .copy(
            status = RuntimeStatus.Running,
            workspaceId = Some(workspaceId),
            auditInfo = auditInfo.copy(creator = userInfo.userEmail)
          )
          .save()
      )
      res <- runtimeV2Service
        .startRuntime(userInfo, runtime.runtimeName, runtime.workspaceId.get)
        .attempt
    } yield {
      val exception = res.swap.toOption.get
      exception.isInstanceOf[RuntimeCannotBeStartedException] shouldBe true
      exception.getMessage shouldBe "Runtime Gcp/dsp-leo-test cannot be started in Running status"
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "publish stop a runtime message properly" in isolatedDbTest {
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)
    val res = for {
      ctx <- appContext.ask[AppContext]
      runtime <- IO(
        makeCluster(0)
          .copy(
            status = RuntimeStatus.Running,
            workspaceId = Some(workspaceId),
            auditInfo = auditInfo.copy(creator = userInfo.userEmail)
          )
          .save()
      )
      _ <- azureService
        .stopRuntime(userInfo, runtime.runtimeName, runtime.workspaceId.get)
      msg <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with

    } yield msg shouldBe Some(StopRuntimeMessage(runtime.id, Some(ctx.traceId)))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to stop a runtime if permission denied" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("user"), WorkbenchEmail("email"), 0)
    val workspaceId = WorkspaceId(UUID.randomUUID())
    val samService = mock[SamService[IO]]
    when(
      samService.checkAuthorized(isEq(userInfo.accessToken.token), any(), isEq(RuntimeAction.StopStartRuntime))(any())
    )
      .thenReturn(IO.raiseError(SamException.create("no access", StatusCodes.Forbidden.intValue, TraceId(""))))
    when(
      samService.checkAuthorized(isEq(userInfo.accessToken.token), any(), isEq(RuntimeAction.GetRuntimeStatus))(any())
    ).thenReturn(IO.unit)
    val interp = makeInterp(samService = samService)

    val res = for {
      runtime <- IO(
        makeCluster(0)
          .copy(
            status = RuntimeStatus.Running,
            workspaceId = Some(workspaceId),
            auditInfo = auditInfo.copy(creator = userInfo.userEmail)
          )
          .save()
      )
      r <- interp
        .stopRuntime(userInfo, runtime.runtimeName, runtime.workspaceId.get)
        .attempt
    } yield {
      val exception = r.swap.toOption.get
      exception.getMessage shouldBe s"email is unauthorized. If you have proper permissions to use the workspace, make sure you are also added to the billing account"
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to stop a runtime when runtime doesn't exist in DB" in isolatedDbTest {
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val res =
      runtimeV2Service
        .stopRuntime(userInfo, runtimeName, workspaceId)
        .attempt
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val exception = res.swap.toOption.get
    exception.isInstanceOf[RuntimeNotFoundByWorkspaceIdException] shouldBe true
    exception.getMessage shouldBe s"Runtime clusterName1 not found in workspace ${workspaceId.value}"
  }

  it should "fail to stop a runtime when runtime is not in startable statuses" in isolatedDbTest {
    val res = for {
      runtime <- IO(
        makeCluster(0)
          .copy(
            status = RuntimeStatus.Stopped,
            workspaceId = Some(workspaceId),
            auditInfo = auditInfo.copy(creator = userInfo.userEmail)
          )
          .save()
      )
      res <- runtimeV2Service
        .stopRuntime(userInfo, runtime.runtimeName, runtime.workspaceId.get)
        .attempt
    } yield {
      val exception = res.swap.toOption.get
      exception.isInstanceOf[RuntimeCannotBeStoppedException] shouldBe true
      exception.getMessage shouldBe s"Runtime Gcp/dsp-leo-test/${runtime.runtimeName.asString} cannot be stopped in Stopped status"
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes" in isolatedDbTest {
    val runtimeId1 = UUID.randomUUID.toString
    val runtimeId2 = UUID.randomUUID.toString
    val projectIdGcp = cloudContextGcp.asString

    val samService = mock[SamService[IO]]
    when(samService.listResources(isEq(userInfo.accessToken.token), isEq(RuntimeSamResource.resourceType))(any()))
      .thenReturn(IO.pure(List(runtimeId1, runtimeId2)))
    val testService = makeInterp(samService = samService)

    val res = for {
      samResource1 <- IO(RuntimeSamResourceId(runtimeId1))
      samResource2 <- IO(RuntimeSamResourceId(runtimeId2))
      // GCP runtime 1
      runtime1 <- IO(makeCluster(1).copy(samResource = samResource1, workspaceId = workspaceIdOpt).save())
      // GCP runtime 2
      runtime2 <- IO(makeCluster(2).copy(samResource = samResource2, workspaceId = workspaceIdOpt).save())
      // AN-570
//      runtime2 <- IO(
//        makeCluster(2)
//          .copy(
//            samResource = samResource2,
//            cloudContext = CloudContext.Azure(CommonTestData.azureCloudContext),
//            workspaceId = Some(WorkspaceId(UUID.fromString(workspaceIdAzure)))
//          )
//          .save()
//      )
      listResponse <- testService.listRuntimes(userInfo, None, None, Map.empty)
    } yield {
      listResponse.map(_.samResource).toSet shouldBe Set(samResource1, samResource2)
      listResponse should contain(
        ListRuntimeResponse2(
          id = runtime1.id,
          workspaceId = workspaceIdOpt,
          samResource = runtime1.samResource,
          clusterName = runtime1.runtimeName,
          cloudContext = runtime1.cloudContext,
          auditInfo = runtime1.auditInfo,
          runtimeConfig = defaultDataprocRuntimeConfig,
          proxyUrl = Runtime
            .getProxyUrl(proxyUrlBase, cloudContextGcp, runtime1.runtimeName, Set(jupyterImage), None, Map.empty),
          runtime1.status,
          runtime1.labels,
          runtime1.patchInProgress
        )
      )
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes with a workspace" in isolatedDbTest {
    val runtimeId1 = UUID.randomUUID.toString
    val runtimeId2 = UUID.randomUUID.toString
    val runtimeId3 = UUID.randomUUID.toString
    val runtimeId4 = UUID.randomUUID.toString
    val runtimeId5 = UUID.randomUUID.toString
    val projectIdGcp1 = "gcp-context-1"
    val projectIdGcp2 = "gcp-context-2"
    val workspaceId1 = UUID.randomUUID.toString
    val workspaceId2 = UUID.randomUUID.toString
    val workspaceId3 = UUID.randomUUID.toString

    val samService = mock[SamService[IO]]
    when(samService.listResources(any(), isEq(RuntimeSamResource.resourceType))(any()))
      .thenReturn(IO.pure(List(runtimeId1, runtimeId2, runtimeId3, runtimeId4, runtimeId5)))

    val testService = makeInterp(samService = samService)

    val res = for {
      samResource1 <- IO(RuntimeSamResourceId(runtimeId1))
      samResource2 <- IO(RuntimeSamResourceId(runtimeId2))
      samResource3 <- IO(RuntimeSamResourceId(runtimeId3))
      samResource4 <- IO(RuntimeSamResourceId(runtimeId4))
      samResource5 <- IO(RuntimeSamResourceId(runtimeId5))
      workspace1 <- IO(WorkspaceId(UUID.fromString(workspaceId1)))
      workspace2 <- IO(WorkspaceId(UUID.fromString(workspaceId2)))
      workspace3 <- IO(WorkspaceId(UUID.fromString(workspaceId3)))

      // hidden runtime 1, owned workspace 1, GCP
      _ <- IO(
        makeCluster(1)
          .copy(
            samResource = samResource1,
            workspaceId = Some(workspace1),
            cloudContext = CloudContext.Gcp(GoogleProject(projectIdGcp1))
          )
          .save()
      )
      // AN-570
//      _ <- IO(
//        makeCluster(1)
//          .copy(
//            samResource = samResource1,
//            workspaceId = Some(workspace1),
//            cloudContext = CloudContext.Azure(
//              AzureCloudContext(
//                TenantId(workspaceId1),
//                SubscriptionId(workspaceId1),
//                ManagedResourceGroupName(workspaceId1)
//              )
//            )
//          )
//          .save()
//      )
      // hidden runtime 2, read workspace 2, owned project 1, Gcp
      _ <- IO(
        makeCluster(2)
          .copy(
            samResource = samResource2,
            workspaceId = Some(workspace2),
            cloudContext = CloudContext.Gcp(GoogleProject(projectIdGcp1))
          )
          .save()
      )
      // read runtime 3, read workspace 2, owned project 1, Gcp
      _ <- IO(
        makeCluster(3)
          .copy(
            samResource = samResource3,
            workspaceId = Some(workspace2),
            cloudContext = CloudContext.Gcp(GoogleProject(projectIdGcp1))
          )
          .save()
      )
      // read runtime 4, read workspace 3, GCP
      _ <- IO(
        makeCluster(4)
          .copy(
            samResource = samResource4,
            workspaceId = Some(workspace3),
            cloudContext = CloudContext.Gcp(GoogleProject(projectIdGcp1))
          )
          .save()
      )
      // AN-570
//      _ <- IO(
//        makeCluster(4)
//          .copy(
//            samResource = samResource4,
//            workspaceId = Some(workspace3),
//            cloudContext = CloudContext.Azure(
//              AzureCloudContext(
//                TenantId(workspaceId3),
//                SubscriptionId(workspaceId3),
//                ManagedResourceGroupName(workspaceId3)
//              )
//            )
//          )
//          .save()
//      )
      // read runtime 5, read project 2, Gcp
      _ <- IO(
        makeCluster(5)
          .copy(samResource = samResource5, cloudContext = CloudContext.Gcp(GoogleProject(projectIdGcp2)))
          .save()
      )

      responseIdsWorkspace1 <- testService.listRuntimes(userInfo, Some(workspace1), None, Map.empty)
      responseIdsWorkspace2 <- testService.listRuntimes(userInfo, Some(workspace2), None, Map.empty)
      responseIdsWorkspace3 <- testService.listRuntimes(userInfo, Some(workspace3), None, Map.empty)
//      responseIdsAzure <- testService.listRuntimes(userInfo, None, Some(CloudProvider.Azure), Map.empty) AN-570
      responseIdsGcp <- testService.listRuntimes(userInfo, None, Some(CloudProvider.Gcp), Map.empty)
      // AN-570
//      responseIdsAzureWorkspace1 <- testService.listRuntimes(userInfo,
//                                                             Some(workspace1),
//                                                             Some(CloudProvider.Azure),
//                                                             Map.empty
//      )
//      responseIdsAzureWorkspace2 <- testService.listRuntimes(userInfo,
//                                                             Some(workspace2),
//                                                             Some(CloudProvider.Azure),
//                                                             Map.empty
//      )
      responseIdsGcpWorkspace1 <- testService.listRuntimes(userInfo,
                                                           Some(workspace1),
                                                           Some(CloudProvider.Gcp),
                                                           Map.empty
      )
      responseIdsGcpWorkspace2 <- testService.listRuntimes(userInfo,
                                                           Some(workspace2),
                                                           Some(CloudProvider.Gcp),
                                                           Map.empty
      )
    } yield {
      responseIdsWorkspace1.map(_.samResource).toSet shouldBe Set(samResource1)
      responseIdsWorkspace2.map(_.samResource).toSet shouldBe Set(samResource2, samResource3)
      responseIdsWorkspace3.map(_.samResource).toSet shouldBe Set(samResource4)
//      responseIdsAzure.map(_.samResource).toSet shouldBe Set(samResource1, samResource4) AN-570
      responseIdsGcp.map(_.samResource).toSet shouldBe Set(samResource2, samResource3, samResource5)
//      responseIdsAzureWorkspace1.map(_.samResource).toSet shouldBe Set(samResource1) AN-570
//      responseIdsAzureWorkspace2.map(_.samResource).toSet shouldBe Set.empty AN-570
      responseIdsGcpWorkspace1.map(_.samResource).toSet shouldBe Set.empty
      responseIdsGcpWorkspace2.map(_.samResource).toSet shouldBe Set(samResource2, samResource3)
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes with parameters" in isolatedDbTest {
    val runtimeId1 = RuntimeSamResourceId(UUID.randomUUID.toString)
    val runtimeId2 = RuntimeSamResourceId(UUID.randomUUID.toString)
    val workspaceId1 = WorkspaceId(UUID.randomUUID)

    val samService = mock[SamService[IO]]
    when(samService.listResources(any(), isEq(RuntimeSamResource.resourceType))(any()))
      .thenReturn(IO.pure(List(runtimeId1.resourceId, runtimeId2.resourceId)))
    val testService = makeInterp(samService = samService)
    val res = for {
      samResource1 <- IO(runtimeId1)
      samResource2 <- IO(runtimeId2)
      runtime1 <- IO(
        makeCluster(1)
          .copy(samResource = samResource1, workspaceId = Some(workspaceId1))
          .save()
      )
      _ <- setRuntimeDeleted(workspaceId1, runtime1.runtimeName)

      runtime2 <- IO(makeCluster(2).copy(samResource = samResource2, workspaceId = Some(workspaceId1)).save())
      _ <- labelQuery.save(runtime2.id, LabelResourceType.Runtime, "foo", "bar").transaction
      listResponse1 <- testService.listRuntimes(
        userInfo,
        None,
        None,
        Map("foo" -> "bar")
      ) // hit
      listResponse2 <- testService.listRuntimes(
        userInfo,
        None,
        None,
        Map("FOO" -> "BAR")
      ) // hit, case insensitive
      listResponse3 <- testService.listRuntimes(
        userInfo,
        None,
        None,
        Map("foo!@#$%^&*()_+=';:\"" -> "!@#$%^&*()_+=';:\"bar")
      ) // miss, with weird characters
      listResponse4 <- testService.listRuntimes(
        userInfo,
        None,
        None,
        Map("foo" -> "not-bar")
      ) // miss value
      listResponse5 <- testService.listRuntimes(
        userInfo,
        None,
        None,
        Map("not-foo" -> "bar")
      ) // miss key
    } yield {
      listResponse1.map(_.samResource).toSet shouldBe Set(samResource2)
      listResponse2.map(_.samResource).toSet shouldBe Set(samResource2)
      listResponse3.map(_.samResource).toSet shouldBe Set.empty
      listResponse4.map(_.samResource).toSet shouldBe Set.empty
      listResponse5.map(_.samResource).toSet shouldBe Set.empty
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes filtered by creator" in isolatedDbTest {
//    val wsmId1 = WsmResourceSamResourceId(WsmControlledResourceId(UUID.randomUUID)) AN-570
    val runtimeId1 = RuntimeSamResourceId(UUID.randomUUID.toString)
    val runtimeId2 = RuntimeSamResourceId(UUID.randomUUID.toString)
    val runtimeId3 = RuntimeSamResourceId(UUID.randomUUID.toString)
    val runtimeId4 = RuntimeSamResourceId(UUID.randomUUID.toString)
    val workspaceId1 = WorkspaceId(UUID.randomUUID)
    val userInfoCreator = mockUserInfo("karen@styx.hel")
    val userInfoOther = mockUserInfo("mike@heavn.io")
    val samService = mock[SamService[IO]]
    when(
      samService.listResources(isEq(userInfoCreator.accessToken.token), isEq(RuntimeSamResource.resourceType))(any())
    )
      .thenReturn(IO.pure(List(runtimeId1.resourceId, runtimeId3.resourceId)))

    val testService = makeInterp(samService = samService)
    val res = for {
      // runtime 1: I created, in a workspace I can read => visible
      samResource1 <- IO(RuntimeSamResourceId(runtimeId1.resourceId.toString))
      runtime1 <- IO(
        makeCluster(1, Some(userInfoCreator.userEmail))
          .copy(samResource = samResource1, workspaceId = Some(workspaceId1))
          .save()
      )

      // runtime 2: I created, but I don't have permission => hidden
      samResource2 <- IO(runtimeId2)
      runtime2 <- IO(
        makeCluster(2, Some(userInfoCreator.userEmail))
          .copy(samResource = samResource2, workspaceId = Some(WorkspaceId(UUID.randomUUID)))
          .save()
      )

      // runtime 3: someone else created, but I can read => hidden if role=creator, else visible
      samResource3 <- IO(runtimeId3)
      runtime3 <- IO(
        makeCluster(3, Some(userInfoOther.userEmail))
          .copy(samResource = samResource3, workspaceId = Some(workspaceId1))
          .save()
      )

      listResponseCreator <- testService.listRuntimes(userInfoCreator, None, None, Map("role" -> "creator"))
      listResponseAny <- testService.listRuntimes(userInfoCreator, None, None, Map.empty)
    } yield {
      listResponseCreator.map(_.samResource).toSet shouldBe Set(samResource1)
      listResponseAny.map(_.samResource).toSet shouldBe Set(samResource1, samResource3)
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  // See https://broadworkbench.atlassian.net/browse/PROD-440
  // AoU relies on the ability for project owners to list other users' runtimes.
  it should "list runtimes belonging to other users" in isolatedDbTest {
    val runtimeId1 = RuntimeSamResourceId(UUID.randomUUID.toString)
    val runtimeId2 = RuntimeSamResourceId(UUID.randomUUID.toString)
    val workspaceId1 = WorkspaceId(UUID.randomUUID)
    val userInfo = mockUserInfo("karen@styx.hel")
    val samService = mock[SamService[IO]]
    when(samService.listResources(isEq(userInfo.accessToken.token), isEq(RuntimeSamResource.resourceType))(any()))
      .thenReturn(IO.pure(List(runtimeId1.resourceId, runtimeId2.resourceId)))

    val testService = makeInterp(samService = samService)

    // Make runtimes belonging to different users than the calling user
    val res = for {
      samResource1 <- IO(runtimeId1)
      samResource2 <- IO(runtimeId2)
      runtime1 = LeoLenses.runtimeToCreator.replace(WorkbenchEmail("different_user1@example.com"))(
        makeCluster(1).copy(samResource = samResource1, workspaceId = Some(workspaceId1))
      )
      runtime2 = LeoLenses.runtimeToCreator.replace(WorkbenchEmail("different_user2@example.com"))(
        makeCluster(2).copy(samResource = samResource2, workspaceId = Some(workspaceId1))
      )
      _ <- IO(runtime1.save())
      _ <- IO(runtime2.save())
      listResponse <- testService.listRuntimes(userInfo, None, None, Map.empty)
    } yield listResponse.map(_.samResource).toSet shouldBe Set(samResource1, samResource2)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes, rejecting invalid label parameters" in isolatedDbTest {
    runtimeV2Service
      .listRuntimes(userInfo, None, None, Map("_labels" -> "foo=bar;bam=yes"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ParseLabelsException] shouldBe true
    runtimeV2Service
      .listRuntimes(userInfo, None, None, Map("_labels" -> "foo=bar,bam"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ParseLabelsException] shouldBe true

    runtimeV2Service
      .listRuntimes(userInfo, None, None, Map("_labels" -> "bogus"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ParseLabelsException] shouldBe true

    runtimeV2Service
      .listRuntimes(userInfo, None, None, Map("_labels" -> "a,b"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ParseLabelsException] shouldBe true
  }
}
