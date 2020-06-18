package org.broadinstitute.dsde.workbench.leonardo
package runtimes

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
class RuntimeCreationDiskSpec
    extends GPAllocFixtureSpec
    with ParallelTestExecution
    with LeonardoTestUtils
    with NotebookTestUtils
    with GPAllocBeforeAndAfterAll {
  implicit val authTokenForOldApiClient = ronAuthToken
  implicit val auth: Authorization = Authorization(Credentials.Token(AuthScheme.Bearer, ronCreds.makeAuthToken().value))

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
          Some(DiskSize(20))
        )
      )
    )

    // validate disk still exists after runtime is deleted
    val res = dependencies.use { dep =>
      implicit val client = dep.httpClient
      for {
        getRuntimeResponse <- LeonardoApiClient.createRuntimeWithWait(googleProject, runtimeName, createRuntimeRequest)
        clusterCopy = ClusterCopy(
          getRuntimeResponse.runtimeName,
          getRuntimeResponse.googleProject,
          getRuntimeResponse.serviceAccount,
          getRuntimeResponse.runtimeConfig,
          getRuntimeResponse.status,
          getRuntimeResponse.auditInfo.creator,
          getRuntimeResponse.labels,
          getRuntimeResponse.asyncRuntimeFields.map(_.stagingBucket),
          getRuntimeResponse.errors,
          getRuntimeResponse.auditInfo.dateAccessed,
          false,
          getRuntimeResponse.autopauseThreshold,
          false
        )
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
        _ <- createRuntimeWithWait(googleProject, runtimeName, createRuntimeRequest)
        runtime <- getRuntime(googleProject, runtimeName)
        _ <- deleteRuntimeWithWait(googleProject, runtimeName)
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
