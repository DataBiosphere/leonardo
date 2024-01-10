package org.broadinstitute.dsde.workbench.leonardo
package runtimes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.DoneCheckableSyntax._
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.google2.Generators.genDiskName
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, DiskName, GoogleDiskService}
import org.broadinstitute.dsde.workbench.leonardo.DiskModelGenerators._
import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient._
import org.broadinstitute.dsde.workbench.leonardo.TestUser.{getAuthTokenAndAuthorization, Ron}
import org.broadinstitute.dsde.workbench.leonardo.http.{
  CreateDiskRequest,
  PersistentDiskRequest,
  RuntimeConfigRequest,
  SourceDiskRequest,
  UpdateDiskRequest
}
import org.broadinstitute.dsde.workbench.leonardo.notebooks.{NotebookTestUtils, Python3}
import org.http4s.client.Client
import org.http4s.Status
import org.http4s.headers.Authorization
import org.scalatest.tagobjects.Retryable
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

import java.util.concurrent.TimeoutException
import scala.concurrent.duration._

@DoNotDiscover
class RuntimeCreationDiskSpec
    extends BillingProjectFixtureSpec
    with ParallelTestExecution
    with LeonardoTestUtils
    with NotebookTestUtils {
  implicit val (authTokenForOldApiClient: IO[AuthToken], auth: IO[Authorization]) = getAuthTokenAndAuthorization(Ron)

  override def withFixture(test: NoArgTest) =
    if (isRetryable(test))
      withRetry(super.withFixture(test))
    else
      super.withFixture(test)

  val dependencies = for {
    diskService <- googleDiskService
    httpClient <- LeonardoApiClient.client
  } yield RuntimeCreationPdSpecDependencies(httpClient, diskService)

  "create runtime and mount disk correctly" in { googleProject =>
    val runtimeName = randomClusterName
    val createRuntimeRequest = defaultCreateRuntime2Request.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.GceConfig(
          None,
          Some(DiskSize(20)),
          Some(LeonardoApiClient.defaultCreateRequestZone),
          None
        )
      )
    )

    // validate disk still exists after runtime is deleted
    val res = dependencies.use { dep =>
      implicit val client = dep.httpClient
      for {
        getRuntimeResponse <- LeonardoApiClient.createRuntimeWithWait(googleProject, runtimeName, createRuntimeRequest)
        implicit0(authToken: AuthToken) <- Ron.authToken()
        output <- SSH.executeGoogleCommand(
          getRuntimeResponse.googleProject,
          LeonardoApiClient.defaultCreateRequestZone.value,
          getRuntimeResponse.runtimeName,
          "sudo docker exec -it jupyter-server df -H"
        )
        _ <- LeonardoApiClient.deleteRuntimeWithWait(googleProject, runtimeName)
      } yield {
        output should include("/dev/sdb")
        output should include("/home/jupyter")
      }
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  "create runtime with SSD disk" in { googleProject =>
    val runtimeName = randomClusterName
    val createRuntimeRequest = defaultCreateRuntime2Request.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.GceWithPdConfig(
          None,
          PersistentDiskRequest(
            DiskName("ssd-disk"),
            None,
            Some(DiskType.SSD),
            Map.empty
          ),
          Some(LeonardoApiClient.defaultCreateRequestZone),
          None
        )
      )
    )

    // validate disk still exists after runtime is deleted
    val res = dependencies.use { dep =>
      implicit val client = dep.httpClient
      for {
        getRuntimeResponse <- LeonardoApiClient.createRuntimeWithWait(googleProject, runtimeName, createRuntimeRequest)
        _ <- LeonardoApiClient.deleteRuntimeWithWait(googleProject, runtimeName)
      } yield getRuntimeResponse.diskConfig.map(_.diskType) shouldBe Some(DiskType.SSD)
    }
    res.unsafeRunSync()
  }

  "create runtime with Balanced disk" in { googleProject =>
    val runtimeName = randomClusterName
    val createRuntimeRequest = defaultCreateRuntime2Request.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.GceWithPdConfig(
          None,
          PersistentDiskRequest(
            DiskName("balanced-disk"),
            None,
            Some(DiskType.Balanced),
            Map.empty
          ),
          Some(LeonardoApiClient.defaultCreateRequestZone),
          None
        )
      )
    )

    // validate disk still exists after runtime is deleted
    val res = dependencies.use { dep =>
      implicit val client = dep.httpClient
      for {
        getRuntimeResponse <- LeonardoApiClient.createRuntimeWithWait(googleProject, runtimeName, createRuntimeRequest)
        _ <- LeonardoApiClient.deleteRuntimeWithWait(googleProject, runtimeName)
      } yield getRuntimeResponse.diskConfig.map(_.diskType) shouldBe Some(DiskType.Balanced)
    }
    res.unsafeRunSync()
  }

  "create runtime and attach a persistent disk" in { googleProject =>
    val diskName = genDiskName.sample.get
    val diskSize = genDiskSize.sample.get
    val runtimeName = randomClusterName
    val createRuntimeRequest = defaultCreateRuntime2Request.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.GceWithPdConfig(
          None,
          PersistentDiskRequest(
            diskName,
            Some(diskSize),
            None,
            Map.empty
          ),
          Some(LeonardoApiClient.defaultCreateRequestZone),
          None
        )
      )
    )

    import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient.eitherDoneCheckable

    // validate disk still exists after runtime is deleted
    val res = dependencies.use { dep =>
      implicit val client = dep.httpClient
      for {
        _ <- LeonardoApiClient.createRuntimeWithWait(googleProject, runtimeName, createRuntimeRequest)
        _ <- LeonardoApiClient.deleteRuntime(googleProject, runtimeName, deleteDisk = false)
        getRuntimeResponse <- getRuntime(googleProject, runtimeName)
        // Validate disk is detached
        _ = getRuntimeResponse.runtimeConfig.asInstanceOf[RuntimeConfig.GceWithPdConfig].persistentDiskId shouldBe None
        ioa = getRuntime(googleProject, runtimeName).attempt
        res <- IO.sleep(20 seconds) >> streamFUntilDone(ioa, 50, 5 seconds).compile.lastOrError
        _ <-
          if (res.isDone) IO.unit
          else IO.raiseError(new TimeoutException(s"delete runtime ${googleProject.value}/${runtimeName.asString}"))
        disk <- LeonardoApiClient.getDisk(googleProject, diskName)
        _ <- IO(disk.status shouldBe DiskStatus.Ready)
        _ <- IO(disk.size shouldBe diskSize)
        _ <- LeonardoApiClient.deleteDiskWithWait(googleProject, diskName)
        listofDisks <- LeonardoApiClient.listDisk(googleProject, true)
      } yield listofDisks.collect { case resp if resp.name == diskName => resp.status } shouldBe List(
        DiskStatus.Deleted
      ) // assume we won't have multiple disks with same name in the same project in tests
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  // TODO: remove selenium from this, needs re-write
  // see https://broadworkbench.atlassian.net/browse/IA-4723
  "create runtime and attach an existing persistent disk or clone" taggedAs Retryable in { googleProject =>
    val randomeName = randomClusterName
    val runtimeName =
      randomeName.copy(asString = randomeName.asString + "pd-spec") // just to make sure the test runtime name is unique
    val runtimeWithDataName = randomeName.copy(asString = randomeName.asString + "pd-spec-data-persist")
    val runtimeWithCloneName = randomeName.copy(asString = randomeName.asString + "pd-spec-clone")
    val diskName = genDiskName.sample.get
    val diskCloneName = DiskName(diskName.value + "-clone")
    val diskSize = DiskSize(110)
    val newDiskSize = DiskSize(150)

    val res = dependencies.use { dep =>
      implicit val client = dep.httpClient

      val createRuntimeRequest = defaultCreateRuntime2Request.copy(
        runtimeConfig = Some(
          RuntimeConfigRequest.GceWithPdConfig(
            None,
            PersistentDiskRequest(
              diskName,
              Some(DiskSize(500)), // this will be ignored since in this test we'll pre create a disk
              None,
              Map.empty
            ),
            Some(LeonardoApiClient.defaultCreateRequestZone),
            None
          )
        )
      )

      val createRuntime2Request = createRuntimeRequest.copy(toolDockerImage =
        Some(ContainerImage(LeonardoConfig.Leonardo.pythonImageUrl, ContainerRegistry.GCR))
      ) // this just needs to be a different image from default image Leonardo uses, which is gatk

      val createRuntimeCloneRequest = createRuntime2Request.copy(
        runtimeConfig = Some(
          RuntimeConfigRequest.GceWithPdConfig(
            None,
            PersistentDiskRequest(
              diskCloneName,
              Some(DiskSize(500)),
              None,
              Map.empty
            ),
            Some(LeonardoApiClient.defaultCreateRequestZone),
            None
          )
        )
      )

      val createDiskCloneRequest =
        CreateDiskRequest(Map.empty,
                          Some(newDiskSize),
                          None,
                          None,
                          defaultCreateDiskRequest.zone,
                          Some(SourceDiskRequest(googleProject, diskName))
        )

      for {
        _ <- LeonardoApiClient.createDiskWithWait(googleProject,
                                                  diskName,
                                                  defaultCreateDiskRequest.copy(size = Some(diskSize))
        )
        implicit0(authToken: AuthToken) <- Ron.authToken()
        runtime <- createRuntimeWithWait(googleProject, runtimeName, createRuntimeRequest)
        clusterCopy = ClusterCopy.fromGetRuntimeResponseCopy(runtime)
        // validate that saved files and user installed packages persist
        _ <- IO(withWebDriver { implicit driver =>
          withNewNotebook(clusterCopy, Python3) { notebookPage =>
            val createNewFile =
              """! echo 'this should save' >> /home/jupyter/test.txt""".stripMargin
            notebookPage.executeCell(createNewFile)
            notebookPage.executeCell("! pip install simplejson")
          }
        })
        // validate that disk remained attached when runtime is stopped and
        // disk remains mounted after restarting
        _ <- IO(stopRuntime(googleProject, runtimeName, true))
        stoppedRuntime <- getRuntime(googleProject, runtimeName)
        _ <- LeonardoApiClient.startRuntimeWithWait(googleProject, runtimeName)
        _ <- IO(withWebDriver { implicit driver =>
          withNewNotebook(clusterCopy, Python3) { notebookPage =>
            val res = notebookPage.executeCell("! df -H").get
            res should include("/dev/sdb")
            res should include("/home/jupyter")

            val persistedData =
              """! cat /home/jupyter/test.txt""".stripMargin
            notebookPage.executeCell(persistedData).get should include("this should save")
          }
        })
        _ <- deleteRuntimeWithWait(googleProject, runtimeName, false)
        _ <- LeonardoApiClient.patchDisk(googleProject, diskName, UpdateDiskRequest(Map.empty, newDiskSize))
        _ <- IO.sleep(5 seconds)

        // Creating new runtime with existing disk should have test.txt file and user installed package
        runtimeWithData <- createRuntimeWithWait(googleProject, runtimeWithDataName, createRuntime2Request)
        _ <- verifyDisk(ClusterCopy.fromGetRuntimeResponseCopy(runtimeWithData))

        // clone the disk
        _ <- LeonardoApiClient.createDiskWithWait(googleProject, diskCloneName, createDiskCloneRequest)
        // Creating new runtime with clone of existing disk should have test.txt file and user installed package
        runtimeWithClone <- createRuntimeWithWait(googleProject, runtimeWithCloneName, createRuntimeCloneRequest)
        _ <- verifyDisk(ClusterCopy.fromGetRuntimeResponseCopy(runtimeWithClone))

        _ <- deleteRuntimeWithWait(googleProject, runtimeWithDataName, deleteDisk = true)
        _ <- deleteRuntimeWithWait(googleProject, runtimeWithCloneName, deleteDisk = true)

        // Disk deletion may take some time so we're retrying to reduce flaky test failures
        getDiskAttempt = getDisk(googleProject, diskName).attempt
        diskResp <- streamFUntilDone(getDiskAttempt, 15, 5 seconds).compile.lastOrError
      } yield {
        runtime.diskConfig.map(_.name) shouldBe Some(diskName)
        runtime.diskConfig.map(_.size) shouldBe Some(diskSize)
        stoppedRuntime.status shouldBe ClusterStatus.Stopped
        stoppedRuntime.diskConfig.map(_.name) shouldBe Some(diskName)
        diskResp match {
          case Left(value) =>
            value match {
              case RestError(_, Status.NotFound, _) => succeed
              case e                                => fail("getDisk failed", e)
            }
          case Right(value) =>
            fail(s"disk shouldn't exist anymore. But we get ${value}")
        }
      }
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  private def verifyDisk(clusterCopyWithData: ClusterCopy)(implicit authToken: AuthToken) =
    IO(withWebDriver { implicit driver =>
      withNewNotebook(clusterCopyWithData, Python3) { notebookPage =>
        val persistedData =
          """! cat /home/jupyter/test.txt""".stripMargin
        notebookPage.executeCell(persistedData).get should include("this should save")
        val persistedPackage = "! pip show simplejson"
        notebookPage.executeCell(persistedPackage).get should include(
          "/home/jupyter/.local/lib/python3.10/site-packages"
        )

        val res = notebookPage
          .executeCell(
            "! df -h --output=size $HOME"
          )
          .get
        res should include("148G")
      }
    })
}

final case class RuntimeCreationPdSpecDependencies(httpClient: Client[IO], googleDiskService: GoogleDiskService[IO])
