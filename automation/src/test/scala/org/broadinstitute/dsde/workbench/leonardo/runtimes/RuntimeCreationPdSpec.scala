package org.broadinstitute.dsde.workbench.leonardo
package runtimes

import java.time.Instant

import cats.effect.IO
import org.broadinstitute.dsde.workbench.google2.Generators.genDiskName
import org.broadinstitute.dsde.workbench.google2.{GoogleDiskService, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.DiskModelGenerators._
import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient._
import org.broadinstitute.dsde.workbench.leonardo.http.{PersistentDiskRequest, RuntimeConfigRequest}
import org.broadinstitute.dsde.workbench.leonardo.notebooks.{NotebookTestUtils, Python3}
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

//@DoNotDiscover
class RuntimeCreationPdSpec
    extends GPAllocFixtureSpec
    with ParallelTestExecution
    with LeonardoTestUtils
    with PropertyBasedTesting
    with NotebookTestUtils
    with GPAllocBeforeAndAfterAll {
  implicit val authTokenForOldApiClient = ronAuthToken
  implicit val auth: Authorization = Authorization(Credentials.Token(AuthScheme.Bearer, ronCreds.makeAuthToken().value))

  val zone = ZoneName("us-central1-a")

  val dependencies = for {
    diskService <- googleDiskService
    httpClient <- LeonardoApiClient.client
  } yield RuntimeCreationPdSpecDependencies(httpClient, diskService)

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
          )
        )
      )
    )

    // validate disk still exists after runtime is deleted
    val res = dependencies.use { dep =>
      implicit val client = dep.httpClient
      for {
        _ <- LeonardoApiClient.createRuntimeWithWait(googleProject, runtimeName, createRuntimeRequest)
        _ <- LeonardoApiClient.deleteRuntimeWithWait(googleProject, runtimeName)
        disk <- LeonardoApiClient.getDisk(googleProject, diskName)
        _ <- LeonardoApiClient.deleteDiskWithWait(googleProject, diskName)
        listofDisks <- LeonardoApiClient.listDisk(googleProject, true)
      } yield {
        disk.status shouldBe DiskStatus.Ready
        disk.size shouldBe diskSize
        listofDisks.collect { case resp if resp.name == diskName => resp.status } shouldBe List(
          DiskStatus.Deleted
        ) //assume we won't have multiple disks with same name in the same project in tests
      }
    }
    res.unsafeRunSync()
  }

  "create runtime and attach an existing a persistent disk" in { googleProject =>
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
            )
          )
        )
      )

      for {
        _ <- LeonardoApiClient.createDiskWithWait(googleProject,
                                                  diskName,
                                                  defaultCreateDiskRequest.copy(size = Some(diskSize)))
        runtime <- createRuntimeWithWait(googleProject, runtimeName, createRuntimeRequest)
        _ <- IO(withWebDriver { implicit driver =>
          withNewNotebook(
            ClusterCopy(
              runtime.runtimeName,
              runtime.googleProject,
              runtime.serviceAccount,
              runtime.runtimeConfig,
              runtime.status,
              runtime.serviceAccount,
              runtime.labels,
              None,
              runtime.errors,
              Instant.now(),
              false,
              runtime.autopauseThreshold,
              false
            ),
            Python3
          ) { notebookPage =>
            val createNewFile =
              """! echo 'this should save' >> /home/jupyter-user/notebooks/test.txt""".stripMargin
            notebookPage.executeCell(createNewFile)
          //notebookPage.saveAndCheckpoint()
          }
        })
        _ <- deleteRuntimeWithWait(googleProject, runtimeName)

        // Creating new runtime with existing disk should have test.txt file
        runtimeWithData <- createRuntimeWithWait(googleProject, runtimeWithDataName, createRuntimeRequest)
        _ <- IO(withWebDriver { implicit driver =>
          withNewNotebook(
            ClusterCopy(
              runtimeWithData.runtimeName,
              runtimeWithData.googleProject,
              runtimeWithData.serviceAccount,
              runtimeWithData.runtimeConfig,
              runtimeWithData.status,
              runtimeWithData.serviceAccount,
              runtimeWithData.labels,
              None,
              runtimeWithData.errors,
              Instant.now(),
              false,
              runtimeWithData.autopauseThreshold,
              false
            ),
            Python3
          ) { notebookPage =>
            val persistedData =
              """! cat /home/jupyter-user/notebooks/test.txt""".stripMargin
            notebookPage.executeCell(persistedData).get should include("this should save")
          //notebookPage.saveAndCheckpoint()
          }
        })
        _ <- deleteRuntimeWithWait(googleProject, runtimeWithDataName)
        _ <- deleteDiskWithWait(googleProject, diskName)
      } yield {
        runtime.diskConfig.map(_.name) shouldBe Some(diskName)
        runtime.diskConfig.map(_.size) shouldBe Some(diskSize)
      }
    }

    res.unsafeRunSync()
  }
}

final case class RuntimeCreationPdSpecDependencies(httpClient: Client[IO], googleDiskService: GoogleDiskService[IO])
