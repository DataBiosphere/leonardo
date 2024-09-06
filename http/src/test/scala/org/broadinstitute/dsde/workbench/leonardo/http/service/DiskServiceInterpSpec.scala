package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.time.Instant
import java.util.UUID
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.IO
import cats.mtl.Ask
import com.google.api.services.cloudresourcemanager.model.{Ancestor, ResourceId}
import com.google.cloud.compute.v1.Disk
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleProjectDAO
import org.broadinstitute.dsde.workbench.google2.mock.MockGoogleDiskService
import org.broadinstitute.dsde.workbench.google2.{DiskName, GoogleDiskService, MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.PersistentDiskAction.ReadPersistentDisk
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.{PersistentDiskSamResourceId, ProjectSamResourceId}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.defaultMockitoAnswer
import org.broadinstitute.dsde.workbench.leonardo.auth.AllowlistAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model.{
  BadRequestException,
  ForbiddenError,
  LeoAuthProvider,
  NonDeletableDisksInProjectFoundException,
  SamResourceAction
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessageType
import org.broadinstitute.dsde.workbench.leonardo.util.QueueFactory
import org.broadinstitute.dsde.workbench.model
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any

import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import scala.collection.immutable.List
import scala.concurrent.Future

trait DiskServiceInterpSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent with MockitoSugar {
  val emptyCreateDiskReq = CreateDiskRequest(
    Map.empty,
    None,
    None,
    None,
    None,
    None
  )

  implicit val appContext: Ask[IO, AppContext] = Ask.const[IO, AppContext](
    AppContext(model.TraceId("traceId"), Instant.now())
  )

  def makeDiskService(dontCloneFromTheseGoogleFolders: Vector[String] = Vector.empty,
                      googleProjectDAO: GoogleProjectDAO = new MockGoogleProjectDAO,
                      allowListAuthProvider: AllowlistAuthProvider = allowListAuthProvider
  ) = {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val diskService = new DiskServiceInterp(
      ConfigReader.appConfig.persistentDisk.copy(dontCloneFromTheseGoogleFolders = dontCloneFromTheseGoogleFolders),
      allowListAuthProvider,
      publisherQueue,
      Some(MockGoogleDiskService),
      Some(googleProjectDAO),
      MockSamService
    )
    (diskService, publisherQueue)
  }
}
class DiskServiceInterpTest
    extends AnyFlatSpec
    with DiskServiceInterpSpec
    with LeonardoTestSuite
    with TestComponent
    with MockitoSugar {

  "DiskService" should "fail with AuthorizationError if user doesn't have project level permission" in {
    val (diskService, _) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("email"), 0)
    val googleProject = GoogleProject("googleProject")

    val res = for {
      d <- diskService
        .createDisk(
          userInfo,
          googleProject,
          DiskName("diskName1"),
          emptyCreateDiskReq
        )
        .attempt
    } yield d shouldBe (Left(ForbiddenError(userInfo.userEmail)))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "successfully create a persistent disk" in isolatedDbTest {
    val (diskService, publisherQueue) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed
    val cloudContext = CloudContext.Gcp(GoogleProject("project1"))
    val diskName = DiskName("diskName1")

    val res = for {
      context <- appContext.ask[AppContext]
      d <- diskService
        .createDisk(
          userInfo,
          GoogleProject(cloudContext.asString),
          diskName,
          emptyCreateDiskReq
        )
        .attempt
      diskOpt <- persistentDiskQuery.getActiveByName(cloudContext, diskName).transaction
      disk = diskOpt.get
      message <- publisherQueue.take
    } yield {
      d shouldBe Right(())
      disk.cloudContext shouldBe cloudContext
      disk.name shouldBe diskName
      val expectedMessage = CreateDiskMessage.fromDisk(disk, Some(context.traceId))

      message shouldBe expectedMessage
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "successfully create a persistent disk clone, same project, nothing forbidden" in isolatedDbTest {
    val googleProject = GoogleProject("project1")
    successfulDiskCloneTest(googleProject, googleProject, Map(googleProject -> "folder"), Vector.empty)
  }

  it should "successfully create a persistent disk clone, same forbidden project" in isolatedDbTest {
    val googleProject = GoogleProject("project1")
    val folder = "folder"
    successfulDiskCloneTest(googleProject, googleProject, Map(googleProject -> folder), Vector(folder))
  }

  it should "successfully create a persistent disk clone, different project, not forbidden" in isolatedDbTest {
    val googleProject1 = GoogleProject("project1")
    val googleProject2 = GoogleProject("project2")
    successfulDiskCloneTest(googleProject1,
                            googleProject2,
                            Map(googleProject1 -> "folder1", googleProject2 -> "folder2"),
                            Vector("anotherFolder")
    )
  }

  private def successfulDiskCloneTest(sourceProject: GoogleProject,
                                      targetProject: GoogleProject,
                                      projectToFolder: Map[GoogleProject, String],
                                      forbiddenFolders: Vector[String]
  ) = {
    val dummyDiskLink = "dummyDiskLink"

    val publisherQueue = QueueFactory.makePublisherQueue()
    val diskService = new DiskServiceInterp(
      ConfigReader.appConfig.persistentDisk.copy(dontCloneFromTheseGoogleFolders = forbiddenFolders),
      allowListAuthProvider,
      publisherQueue,
      Some(new MockGoogleDiskService {
        override def getDisk(project: GoogleProject, zone: ZoneName, diskName: DiskName)(implicit
          ev: Ask[IO, TraceId]
        ): IO[Option[Disk]] =
          IO.pure(Some(Disk.newBuilder().setSelfLink(dummyDiskLink).build()))
      }),
      Some(new MockGoogleProjectDAOWithCustomAncestors(projectToFolder)),
      MockSamService
    )

    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed
    val diskName = DiskName("diskName1")
    val diskCloneName = DiskName("clone")
    val expectedFormattedBy = FormattedBy.GCE

    val res = for {
      context <- appContext.ask[AppContext]
      _ <- diskService
        .createDisk(
          userInfo,
          sourceProject,
          diskName,
          emptyCreateDiskReq
        )

      persistedDiskOpt <- persistentDiskQuery.getActiveByName(CloudContext.Gcp(sourceProject), diskName).transaction
      persistedDisk = persistedDiskOpt.get
      _ <- persistentDiskQuery
        .updateStatusAndIsFormatted(persistedDisk.id, persistedDisk.status, expectedFormattedBy, Instant.now())
        .transaction

      createDiskMessage <- publisherQueue.take // need to take this off the queue

      _ <- diskService
        .createDisk(
          userInfo,
          targetProject,
          diskCloneName,
          emptyCreateDiskReq.copy(sourceDisk = Some(SourceDiskRequest(sourceProject, diskName)))
        )
      diskOpt <- persistentDiskQuery.getActiveByName(CloudContext.Gcp(targetProject), diskCloneName).transaction
      disk = diskOpt.get
      createCloneMessage <- publisherQueue.take
    } yield {
      disk.cloudContext shouldBe CloudContext.Gcp(targetProject)
      disk.name shouldBe diskCloneName
      disk.formattedBy shouldBe Some(expectedFormattedBy)
      val expectedMessage = CreateDiskMessage.fromDisk(disk, Some(context.traceId))

      createCloneMessage shouldBe expectedMessage
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail with BadRequestException if source disk does not exist" in isolatedDbTest {
    val (diskService, _) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed
    val googleProject = GoogleProject("project1")
    val diskName = DiskName("diskName1")

    val res = for {
      context <- appContext.ask[AppContext]
      cloneAttempt <- diskService
        .createDisk(
          userInfo,
          googleProject,
          DiskName(diskName.value + "-clone"),
          emptyCreateDiskReq.copy(sourceDisk = Some(SourceDiskRequest(googleProject, diskName)))
        )
        .attempt
    } yield cloneAttempt shouldBe (Left(BadRequestException("source disk does not exist", Some(context.traceId))))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail with BadRequestException if source disk is in different, forbidden folder" in isolatedDbTest {
    val sourceGoogleProject = GoogleProject("sourceProject1")
    val forbiddenFolder = "forbiddenFolder"
    val (diskService, _) =
      makeDiskService(Vector(forbiddenFolder),
                      new MockGoogleProjectDAOWithCustomAncestors(Map(sourceGoogleProject -> forbiddenFolder))
      )
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed
    val googleProject = GoogleProject("project1")
    val diskName = DiskName("diskName1")

    val res = for {
      context <- appContext.ask[AppContext]
      cloneAttempt <- diskService
        .createDisk(
          userInfo,
          googleProject,
          DiskName(diskName.value + "-clone"),
          emptyCreateDiskReq.copy(sourceDisk = Some(SourceDiskRequest(sourceGoogleProject, diskName)))
        )
        .attempt
    } yield cloneAttempt shouldBe (Left(
      BadRequestException(s"persistent disk clone from $sourceGoogleProject to $googleProject not permitted",
                          Some(context.traceId)
      )
    ))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail with BadRequestException if user doesn't have permission to source disk" in isolatedDbTest {
    val dummyDiskLink = "dummyDiskLink"
    val authProviderMock = mock[LeoAuthProvider[IO]](defaultMockitoAnswer[IO])
    val googleDiskServiceMock = mock[GoogleDiskService[IO]](defaultMockitoAnswer[IO])

    val publisherQueue = QueueFactory.makePublisherQueue()
    val diskService = new DiskServiceInterp(
      ConfigReader.appConfig.persistentDisk,
      authProviderMock,
      publisherQueue,
      Some(googleDiskServiceMock),
      Some(new MockGoogleProjectDAO),
      MockSamService
    )
    val userInfoCreator =
      UserInfo(OAuth2BearerToken(""), WorkbenchUserId("creator"), WorkbenchEmail("creator@example.com"), 0)
    val userInfoCloner =
      UserInfo(OAuth2BearerToken(""), WorkbenchUserId("cloner"), WorkbenchEmail("cloner@example.com"), 0)

    val googleProject = GoogleProject("project1")
    val diskName = DiskName("diskName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())
    when(
      authProviderMock.hasPermission(ArgumentMatchers.eq(ProjectSamResourceId(googleProject)),
                                     ArgumentMatchers.eq(ProjectAction.CreatePersistentDisk),
                                     ArgumentMatchers.eq(userInfoCreator)
      )(any(), any())
    ).thenReturn(IO.pure(true))

    when(
      authProviderMock.hasPermission(ArgumentMatchers.eq(ProjectSamResourceId(googleProject)),
                                     ArgumentMatchers.eq(ProjectAction.CreatePersistentDisk),
                                     ArgumentMatchers.eq(userInfoCloner)
      )(any(), any())
    ).thenReturn(IO.pure(true))

    when(
      authProviderMock.isUserProjectReader(ArgumentMatchers.eq(CloudContext.Gcp(googleProject)),
                                           ArgumentMatchers.eq(userInfoCloner)
      )(any())
    ).thenReturn(IO.pure(true))

    when(
      googleDiskServiceMock.getDisk(googleProject, ConfigReader.appConfig.persistentDisk.defaultZone, diskName)
    ).thenReturn(IO.pure(Some(Disk.newBuilder().setSelfLink(dummyDiskLink).build())))

    val res = for {
      context <- appContext.ask[AppContext]
      _ <- diskService
        .createDisk(
          userInfoCreator,
          googleProject,
          diskName,
          emptyCreateDiskReq
        )

      _ <- DiskServiceDbQueries
        .getGetPersistentDiskResponse(CloudContext.Gcp(googleProject), diskName, context.traceId)
        .transaction
        .map { r =>
          when(
            authProviderMock.hasPermissionWithProjectFallback(
              ArgumentMatchers.eq(r.samResource),
              ArgumentMatchers.eq(PersistentDiskAction.ReadPersistentDisk),
              ArgumentMatchers.eq(ProjectAction.ReadPersistentDisk),
              ArgumentMatchers.eq(userInfoCloner),
              ArgumentMatchers.eq(googleProject)
            )(any[SamResourceAction[PersistentDiskSamResourceId, ReadPersistentDisk.type]], any[Ask[IO, TraceId]])
          ).thenReturn(IO.pure(false))
        }

      cloneAttempt <- diskService
        .createDisk(
          userInfoCloner,
          googleProject,
          DiskName(diskName.value + "-clone"),
          emptyCreateDiskReq.copy(sourceDisk = Some(SourceDiskRequest(googleProject, diskName)))
        )
        .attempt
    } yield cloneAttempt shouldBe Left(BadRequestException("source disk does not exist", Some(context.traceId)))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "get a disk" in isolatedDbTest {
    val (diskService, _) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      samResource <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk <- makePersistentDisk(None).copy(samResource = samResource).save()
      _ <- labelQuery.save(disk.id.value, LabelResourceType.PersistentDisk, "label1", "value1").transaction
      _ <- labelQuery.save(disk.id.value, LabelResourceType.PersistentDisk, "label2", "value2").transaction
      labelResp <- labelQuery.getAllForResource(disk.id.value, LabelResourceType.PersistentDisk).transaction
      getResponse <- diskService.getDisk(userInfo, disk.cloudContext, disk.name)
    } yield {
      getResponse.samResource shouldBe disk.samResource
      getResponse.labels shouldBe labelResp
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to get a disk when user doesn't have project access" in isolatedDbTest {
    val (diskService, _) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      samResource <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk <- makePersistentDisk(None).copy(samResource = samResource).save()
      getResponse <- diskService.getDisk(userInfo4, disk.cloudContext, disk.name)
    } yield getResponse

    a[ForbiddenError] should be thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "list disks" in isolatedDbTest {
    val (diskService, _) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      disk1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      disk2 <- makePersistentDisk(Some(DiskName("d2"))).save()
      listResponse <- diskService.listDisks(userInfo, None, Map("includeLabels" -> "key1,key2,key4"))
    } yield {
      listResponse.map(_.id).toSet shouldBe Set(disk1.id, disk2.id)
      listResponse.map(_.labels).toSet shouldBe Set(Map("key1" -> "value1", "key2" -> "value2"))
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list azure and gcp disks" in isolatedDbTest {
    val (diskService, _) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      disk1 <- makePersistentDisk(Some(DiskName("d1")), cloudContextOpt = Some(cloudContextGcp)).save()
      disk2 <- makePersistentDisk(Some(DiskName("d2")), cloudContextOpt = Some(cloudContextAzure)).save()
      listResponse <- diskService.listDisks(userInfo, None, Map("includeLabels" -> "key1,key2,key4"))
    } yield {
      listResponse.map(_.id).toSet shouldBe Set(disk1.id, disk2.id)
      listResponse.map(_.labels).toSet shouldBe Set(Map("key1" -> "value1", "key2" -> "value2"))
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list disks with a project" in isolatedDbTest {
    val (diskService, _) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      disk1 <- makePersistentDisk(Some(DiskName("d1")), cloudContextOpt = Some(cloudContextGcp)).save()
      disk2 <- makePersistentDisk(Some(DiskName("d2")), cloudContextOpt = Some(cloudContextGcp)).save()
      _ <- makePersistentDisk(None, cloudContextOpt = Some(CloudContext.Gcp(GoogleProject("non-default")))).save()
      listResponse <- diskService.listDisks(userInfo, Some(cloudContextGcp), Map.empty)
    } yield listResponse.map(_.id).toSet shouldBe Set(disk1.id, disk2.id)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list disks with project access" in isolatedDbTest {
    val (diskService, _) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      disk1 <- makePersistentDisk(Some(DiskName("d1")), cloudContextOpt = Some(cloudContextGcp)).save()
      disk2 <- makePersistentDisk(Some(DiskName("d2")), cloudContextOpt = Some(CloudContext.Gcp(project2))).save()
      _ <- makePersistentDisk(None, cloudContextOpt = Some(CloudContext.Gcp(GoogleProject("non-default")))).save()
      listResponse <- diskService.listDisks(userInfo, Some(cloudContextGcp), Map.empty)
    } yield listResponse.map(_.id).toSet shouldBe Set(disk1.id)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list disks with parameters" in isolatedDbTest {
    val (diskService, _) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      disk1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      _ <- makePersistentDisk(Some(DiskName("d2"))).save()
      _ <- labelQuery.save(disk1.id.value, LabelResourceType.PersistentDisk, "foo", "bar").transaction
      listResponse <- diskService.listDisks(userInfo, None, Map("foo" -> "bar"))
    } yield listResponse.map(_.id).toSet shouldBe Set(disk1.id)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list disks belonging to other users" in isolatedDbTest {
    val (diskService, _) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    // Make disks belonging to different users than the calling user
    val res = for {
      disk1 <- LeoLenses.diskToCreator
        .set(WorkbenchEmail("a_different_user@example.com"))(
          makePersistentDisk(Some(DiskName("d1")), cloudContextOpt = Some(CloudContext.Gcp(project2)))
        )
        .save()
      disk2 <- LeoLenses.diskToCreator
        .set(WorkbenchEmail("a_different_user2@example.com"))(makePersistentDisk(Some(DiskName("d2"))))
        .save()
      listResponse <- diskService.listDisks(userInfo, None, Map.empty)
    } yield
    // Since the calling user is allow-listed in the auth provider, it should return
    // the disks belonging to other users.
    listResponse.map(_.id).toSet shouldBe Set(disk1.id, disk2.id)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list disks belonging to self and others, if not filtered by role=creator" in isolatedDbTest {
    val (diskService, _) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      disk1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      // Make second disk belonging to different user than the calling user
      disk2 <- LeoLenses.diskToCreator
        .set(WorkbenchEmail("a_different_user@example.com"))(makePersistentDisk(Some(DiskName("d2"))))
        .save()
      listResponse <- diskService.listDisks(userInfo, None, Map.empty)
    } yield
    // Since the calling user has access to both disks, should see both
    listResponse.map(_.id).toSet shouldBe Set(disk1.id, disk2.id)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list disks belonging to self only, if filtered by role=creator" in isolatedDbTest {
    val (diskService, _) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      disk1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      // Make second disk belonging to different user than the calling user
      disk2 <- LeoLenses.diskToCreator
        .set(WorkbenchEmail("a_different_user@example.com"))(makePersistentDisk(Some(DiskName("d2"))))
        .save()
      listResponse <- diskService.listDisks(userInfo, None, Map("role" -> "creator"))
    } yield
    // Since the calling user created disk1 only, only disk1 is visible when filtered by role=creator
    listResponse.map(_.id).toSet shouldBe Set(disk1.id)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to list disks if filtered by role=not_creator" in isolatedDbTest {
    val (diskService, _) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      disk1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      // Make second disk belonging to different user than the calling user
      disk2 <- LeoLenses.diskToCreator
        .set(WorkbenchEmail("a_different_user@example.com"))(makePersistentDisk(Some(DiskName("d2"))))
        .save()
      listResponse <- diskService.listDisks(userInfo, None, Map("role" -> "manager"))
    } yield listResponse

    a[BadRequestException] should be thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "delete a disk" in isolatedDbTest {
    val (diskService, publisherQueue) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      context <- appContext.ask[AppContext]
      diskSamResource <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk <- makePersistentDisk(None).copy(samResource = diskSamResource).save()

      _ <- diskService.deleteDisk(userInfo, GoogleProject(disk.cloudContext.asString), disk.name)
      dbDiskOpt <- persistentDiskQuery
        .getActiveByName(disk.cloudContext, disk.name)
        .transaction
      dbDisk = dbDiskOpt.get
      message <- publisherQueue.take
    } yield {
      dbDisk.status shouldBe DiskStatus.Deleting
      val expectedMessage = DeleteDiskMessage(disk.id, Some(context.traceId))
      message shouldBe expectedMessage
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to delete a disk if it is attached to a runtime" in isolatedDbTest {
    val (diskService, _) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      t <- appContext.ask[AppContext]
      diskSamResource <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk <- makePersistentDisk(None).copy(samResource = diskSamResource).save()
      _ <- IO(
        makeCluster(1).saveWithRuntimeConfig(
          RuntimeConfig.GceWithPdConfig(MachineTypeName("n1-standard-4"),
                                        Some(disk.id),
                                        bootDiskSize = DiskSize(50),
                                        zone = ZoneName("us-west2-b"),
                                        None
          )
        )
      )
      err <- diskService.deleteDisk(userInfo, GoogleProject(disk.cloudContext.asString), disk.name).attempt
    } yield err shouldBe Left(DiskAlreadyAttachedException(CloudContext.Gcp(project), disk.name, t.traceId))

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to delete a disk if user lost project access" in isolatedDbTest {
    val (diskService, _) = makeDiskService(allowListAuthProvider = allowListAuthProvider2)
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      t <- appContext.ask[AppContext]
      diskSamResource <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk <- makePersistentDisk(None).copy(samResource = diskSamResource).save()
      _ <- IO(
        makeCluster(1).saveWithRuntimeConfig(
          RuntimeConfig.GceWithPdConfig(MachineTypeName("n1-standard-4"),
                                        Some(disk.id),
                                        bootDiskSize = DiskSize(50),
                                        zone = ZoneName("us-west2-b"),
                                        None
          )
        )
      )
      err <- diskService.deleteDisk(userInfo, GoogleProject(disk.cloudContext.asString), disk.name).attempt
    } yield err shouldBe Left(ForbiddenError(userInfo.userEmail, Some(t.traceId)))

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete a disk records but not queue delete disk message" in isolatedDbTest {
    val (diskService, publisherQueue) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      context <- appContext.ask[AppContext]
      diskSamResource <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk <- makePersistentDisk(Some(DiskName("d1")), cloudContextOpt = Some(cloudContextGcp))
        .copy(samResource = diskSamResource)
        .save()

      listResponse <- diskService.listDisks(userInfo, Some(cloudContextGcp), Map.empty)

      _ <- diskService.deleteDiskRecords(userInfo, cloudContextGcp, listResponse.head)
      status <- persistentDiskQuery
        .getStatus(disk.id)
        .transaction

      message <- publisherQueue.tryTake

    } yield {
      status shouldBe Some(DiskStatus.Deleted)
      message shouldBe None
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to delete a disk records if the user does not have permission" in isolatedDbTest {
    val (diskService1, _) = makeDiskService()
    val (diskService2, _) = makeDiskService(allowListAuthProvider = allowListAuthProvider2)
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      context <- appContext.ask[AppContext]
      diskSamResource <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk <- makePersistentDisk(Some(DiskName("d1")), cloudContextOpt = Some(cloudContextGcp))
        .copy(samResource = diskSamResource)
        .save()

      listResponse <- diskService1.listDisks(userInfo, Some(cloudContextGcp), Map.empty)

      err <- diskService2.deleteDiskRecords(userInfo, cloudContextGcp, listResponse.head).attempt
      status <- persistentDiskQuery
        .getStatus(disk.id)
        .transaction

    } yield {
      status shouldBe Some(DiskStatus.Ready)
      err shouldBe Left(DiskNotFoundException(cloudContextGcp, disk.name, context.traceId))
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete all disks records but not queue delete disk messages" in isolatedDbTest {
    val (diskService, publisherQueue) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      context <- appContext.ask[AppContext]
      diskSamResource1 <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk1 <- makePersistentDisk(Some(DiskName("d1")), cloudContextOpt = Some(cloudContextGcp))
        .copy(samResource = diskSamResource1)
        .save()

      diskSamResource2 <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk2 <- makePersistentDisk(Some(DiskName("d2")), cloudContextOpt = Some(cloudContextGcp))
        .copy(samResource = diskSamResource2)
        .save()

      _ <- diskService.deleteAllDisksRecords(userInfo, cloudContextGcp)

      disks <- diskService.listDisks(userInfo, Some(cloudContextGcp), Map("includeDeleted" -> "true"))

      message <- publisherQueue.tryTake

    } yield {
      disks.map(_.status) shouldEqual List(DiskStatus.Deleted, DiskStatus.Deleted)
      message shouldBe None
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete all orphaned disks" in isolatedDbTest {
    val (diskService, publisherQueue) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      context <- appContext.ask[AppContext]
      // 1 and 2 are the attached disks while 3 and 4 are orphaned. Only 3 and 4 should be deleted
      diskSamResource1 <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk1 <- makePersistentDisk(Some(DiskName("d1")), cloudContextOpt = Some(cloudContextGcp))
        .copy(samResource = diskSamResource1)
        .save()
      diskSamResource2 <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk2 <- makePersistentDisk(Some(DiskName("d2")), cloudContextOpt = Some(cloudContextGcp))
        .copy(samResource = diskSamResource2)
        .save()
      diskSamResource3 <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk3 <- makePersistentDisk(Some(DiskName("d3")), cloudContextOpt = Some(cloudContextGcp))
        .copy(samResource = diskSamResource1)
        .save()
      diskSamResource4 <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk4 <- makePersistentDisk(Some(DiskName("d4")), cloudContextOpt = Some(cloudContextGcp))
        .copy(samResource = diskSamResource4)
        .save()

      _ <- diskService.deleteAllOrphanedDisks(userInfo, cloudContextGcp, Vector(disk1.id), Vector(disk2.name))

      disks <- diskService.listDisks(userInfo, Some(cloudContextGcp), Map("includeDeleted" -> "true"))

      messages <- publisherQueue.tryTakeN(Some(2))

    } yield {
      disks.map(_.status.toString).sorted shouldBe Vector(DiskStatus.Deleting.toString,
                                                          DiskStatus.Deleting.toString,
                                                          DiskStatus.Ready.toString,
                                                          DiskStatus.Ready.toString
      )
      messages.map(_.messageType) shouldBe List(LeoPubsubMessageType.DeleteDisk, LeoPubsubMessageType.DeleteDisk)
      val deleteDiskMessages = messages.map(_.asInstanceOf[DeleteDiskMessage])
      deleteDiskMessages.map(_.diskId.toString).sorted shouldBe List(disk3.id.toString, disk4.id.toString).sorted
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to delete all orphaned disks if a disk is not deletable" in isolatedDbTest {
    val (diskService, publisherQueue) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      context <- appContext.ask[AppContext]
      // 1 and 2 are the attached disks while 3 and 4 are orphaned. Only 3 and 4 should be deleted
      diskSamResource1 <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk1 <- makePersistentDisk(Some(DiskName("d1")), cloudContextOpt = Some(cloudContextGcp))
        .copy(samResource = diskSamResource1)
        .save()
      diskSamResource2 <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk2 <- makePersistentDisk(Some(DiskName("d2")), cloudContextOpt = Some(cloudContextGcp))
        .copy(samResource = diskSamResource2, status = DiskStatus.Deleting)
        .save()

      _ <- diskService.deleteAllOrphanedDisks(userInfo, cloudContextGcp, Vector.empty, Vector.empty)

      disks <- diskService.listDisks(userInfo, Some(cloudContextGcp), Map("includeDeleted" -> "true"))

      messages <- publisherQueue.tryTakeN(Some(2))

    } yield {
      messages shouldBe List.empty
      disks.map(_.status) shouldBe List(DiskStatus.Ready, DiskStatus.Deleting)
    }

    the[NonDeletableDisksInProjectFoundException] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  List(DiskStatus.Creating, DiskStatus.Restoring, DiskStatus.Failed, DiskStatus.Deleting, DiskStatus.Deleted).foreach {
    status =>
      val userInfo = UserInfo(OAuth2BearerToken(""),
                              WorkbenchUserId("userId"),
                              WorkbenchEmail("user1@example.com"),
                              0
      ) // this email is allow-listed
      it should s"fail to update a disk in $status status" in isolatedDbTest {
        val (diskService, _) = makeDiskService()
        val res = for {
          t <- appContext.ask[AppContext]
          diskSamResource <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
          disk <- makePersistentDisk(None).copy(samResource = diskSamResource, status = status).save()
          req = UpdateDiskRequest(Map.empty, DiskSize(600))
          fail <- diskService
            .updateDisk(userInfo, GoogleProject(disk.cloudContext.asString), disk.name, req)
            .attempt
        } yield fail shouldBe Left(
          DiskCannotBeUpdatedException(disk.projectNameString, disk.status, traceId = t.traceId)
        )
        res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      }
  }

  it should "update a disk in Ready status" in isolatedDbTest {
    val (diskService, publisherQueue) = makeDiskService()
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      context <- appContext.ask[AppContext]
      diskSamResource <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk <- makePersistentDisk(None).copy(samResource = diskSamResource).save()
      req = UpdateDiskRequest(Map.empty, DiskSize(600))
      _ <- diskService.updateDisk(userInfo, GoogleProject(disk.cloudContext.asString), disk.name, req)
      message <- publisherQueue.take
    } yield {
      val expectedMessage = UpdateDiskMessage(disk.id, DiskSize(600), Some(context.traceId))
      message shouldBe expectedMessage
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
}

class MockGoogleProjectDAOWithCustomAncestors(customAncestors: Map[GoogleProject, String])
    extends MockGoogleProjectDAO {
  override def getAncestry(projectName: String): Future[Seq[Ancestor]] =
    customAncestors
      .get(GoogleProject(projectName))
      .map(folder =>
        Future.successful(
          Seq(
            new Ancestor().setResourceId(new ResourceId().setId(projectName).setType("project")),
            new Ancestor().setResourceId(new ResourceId().setId(folder).setType("folder"))
          )
        )
      )
      .getOrElse(super.getAncestry(projectName))
}
