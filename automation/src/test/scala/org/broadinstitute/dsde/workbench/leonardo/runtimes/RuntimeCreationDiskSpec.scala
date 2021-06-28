package org.broadinstitute.dsde.workbench.leonardo
package runtimes

import java.util.concurrent.TimeoutException

import cats.syntax.all._
import cats.effect.IO
import org.broadinstitute.dsde.workbench.google2.Generators.genDiskName
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, GoogleDiskService, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.DiskModelGenerators._
import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient._
import org.broadinstitute.dsde.workbench.leonardo.http.{PersistentDiskRequest, RuntimeConfigRequest}
import org.broadinstitute.dsde.workbench.leonardo.notebooks.{NotebookTestUtils, Python3}
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

import scala.concurrent.duration._
import org.broadinstitute.dsde.workbench.DoneCheckableSyntax._
import org.http4s.Status
import org.scalatest.tagobjects.Retryable

@DoNotDiscover
class RuntimeCreationDiskSpec
    extends GPAllocFixtureSpec
    with ParallelTestExecution
    with LeonardoTestUtils
    with NotebookTestUtils {
  implicit val authTokenForOldApiClient = ronAuthToken
  implicit val auth: Authorization = Authorization(Credentials.Token(AuthScheme.Bearer, ronCreds.makeAuthToken().value))

  override def withFixture(test: NoArgTest) =
    if (isRetryable(test))
      withRetry(super.withFixture(test))
    else
      super.withFixture(test)

  val zone = ZoneName("us-central1-a")

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
          None,
          None
        )
      )
    )

    // validate disk still exists after runtime is deleted
    val res = dependencies.use { dep =>
      implicit val client = dep.httpClient
      for {
        getRuntimeResponse <- LeonardoApiClient.createRuntimeWithWait(googleProject, runtimeName, createRuntimeRequest)
        clusterCopy = ClusterCopy.fromGetRuntimeResponseCopy(getRuntimeResponse)
        _ <- IO(
          withWebDriver { implicit driver =>
            withNewNotebook(clusterCopy, Python3) { notebookPage =>
              //all other packages cannot be tested for their versions in this manner
              //warnings are ignored because they are benign warnings that show up for python2 because of compilation against an older numpy
              val res = notebookPage
                .executeCell(
                  "! df -H"
                )
                .get
              res should include("/dev/sdb")
              res should include("/home/jupyter-user/notebooks")
            }
          }
        )
        _ <- LeonardoApiClient.deleteRuntimeWithWait(googleProject, runtimeName)
      } yield ()
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
          None,
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
        res <- testTimer.sleep(20 seconds) >> streamFUntilDone(ioa, 50, 5 seconds).compile.lastOrError
        _ <- if (res.isDone) IO.unit
        else IO.raiseError(new TimeoutException(s"delete runtime ${googleProject.value}/${runtimeName.asString}"))
        disk <- LeonardoApiClient.getDisk(googleProject, diskName)
        _ <- IO(disk.status shouldBe DiskStatus.Ready)
        _ <- IO(disk.size shouldBe diskSize)
        _ <- LeonardoApiClient.deleteDiskWithWait(googleProject, diskName)
        listofDisks <- LeonardoApiClient.listDisk(googleProject, true)
      } yield {
        listofDisks.collect { case resp if resp.name == diskName => resp.status } shouldBe List(
          DiskStatus.Deleted
        ) //assume we won't have multiple disks with same name in the same project in tests
      }
    }
    res.unsafeRunSync()
  }

  "create runtime and attach an existing persistent disk" taggedAs Retryable in { googleProject =>
    val randomeName = randomClusterName
    val runtimeName = randomeName.copy(asString = randomeName.asString + "pd-spec") // just to make sure the test runtime name is unique
    val runtimeWithDataName = randomeName.copy(asString = randomeName.asString + "pd-spec-data-persist")
    val diskName = genDiskName.sample.get
    val diskSize = genDiskSize.sample.get

    val res = dependencies.use { dep =>
      implicit val client = dep.httpClient

      val createRuntimeRequest = defaultCreateRuntime2Request.copy(
        runtimeConfig = Some(
          RuntimeConfigRequest.GceWithPdConfig(
            None,
            PersistentDiskRequest(
              diskName,
              Some(DiskSize(500)), //this will be ignored since in this test we'll pre create a disk
              None,
              Map.empty
            ),
            None,
            None
          )
        )
      )

      for {
        _ <- LeonardoApiClient.createDiskWithWait(googleProject,
                                                  diskName,
                                                  defaultCreateDiskRequest.copy(size = Some(diskSize)))
        runtime <- createRuntimeWithWait(googleProject, runtimeName, createRuntimeRequest)
        clusterCopy = ClusterCopy.fromGetRuntimeResponseCopy(runtime)
        // validate that saved files and user installed packages persist
        _ <- IO(withWebDriver { implicit driver =>
          withNewNotebook(clusterCopy, Python3) { notebookPage =>
            val createNewFile =
              """! echo 'this should save' >> /home/jupyter-user/notebooks/test.txt""".stripMargin
            notebookPage.executeCell(createNewFile)
            notebookPage.executeCell("! pip install beautifulSoup4")
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
            res should include("/home/jupyter-user/notebooks")
          }
        })
        _ <- deleteRuntimeWithWait(googleProject, runtimeName, false)

        // Creating new runtime with existing disk should have test.txt file and user installed package
        runtimeWithData <- createRuntimeWithWait(googleProject, runtimeWithDataName, createRuntimeRequest)
        clusterCopyWithData = ClusterCopy.fromGetRuntimeResponseCopy(runtimeWithData)
        _ <- IO(withWebDriver { implicit driver =>
          withNewNotebook(clusterCopyWithData, Python3) { notebookPage =>
            val persistedData =
              """! cat /home/jupyter-user/notebooks/test.txt""".stripMargin
            notebookPage.executeCell(persistedData).get should include("this should save")
            val persistedPackage = "! pip show beautifulSoup4"
            notebookPage.executeCell(persistedPackage).get should include("/home/jupyter-user/notebooks/packages")
          }
        })
        _ <- deleteRuntimeWithWait(googleProject, runtimeWithDataName, deleteDisk = true)
        getDiskAttempt = getDisk(googleProject, diskName).attempt

        // Disk deletion may take some time so we're retrying to reduce flaky test failures
        diskResp <- streamFUntilDone(getDiskAttempt, 15, 5 seconds).compile.lastOrError
      } yield {
        runtime.diskConfig.map(_.name) shouldBe Some(diskName)
        runtime.diskConfig.map(_.size) shouldBe Some(diskSize)
        stoppedRuntime.status shouldBe ClusterStatus.Stopped
        stoppedRuntime.diskConfig.map(_.name) shouldBe Some(diskName)
        diskResp.swap.toOption.get.asInstanceOf[RestError].statusCode shouldBe Status.NotFound
      }
    }

    res.unsafeRunSync()
  }
}

final case class RuntimeCreationPdSpecDependencies(httpClient: Client[IO], googleDiskService: GoogleDiskService[IO])
