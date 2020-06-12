package org.broadinstitute.dsde.workbench.leonardo
package runtimes

import cats.effect.IO
import org.broadinstitute.dsde.workbench.google2.Generators.genDiskName
import org.broadinstitute.dsde.workbench.google2.{GoogleDiskService, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.DiskModelGenerators._
import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient._
import org.broadinstitute.dsde.workbench.leonardo.http.{PersistentDiskRequest, RuntimeConfigRequest}
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

@DoNotDiscover
class RuntimeCreationPdSpec
    extends GPAllocFixtureSpec
    with ParallelTestExecution
    with LeonardoTestUtils
    with PropertyBasedTesting
    with GPAllocBeforeAndAfterAll {
  implicit val authTokenForOldApiClient = ronAuthToken
  implicit val auth: Authorization = Authorization(Credentials.Token(AuthScheme.Bearer, ronCreds.makeAuthToken().value))

  val zone = ZoneName("us-central1-a")

  val dependencies = for {
    diskService <- googleDiskService
    httpClient <- LeonardoApiClient.client
  } yield RuntimeCreationPdSpecDependencies(httpClient, diskService)

  "create and attach a persistent disk" in { googleProject =>
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
        listofDisks.headOption.map(_.status) shouldBe (DiskStatus.Deleted) //assume we won't have multiple disks with same name in the same project in tests
      }
    }
    res.unsafeRunSync()
  }

  "create and attach an existing a persistent disk" in { googleProject =>
    val runtimeName = randomClusterName
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
