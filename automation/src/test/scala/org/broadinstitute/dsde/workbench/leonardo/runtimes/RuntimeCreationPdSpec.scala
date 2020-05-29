package org.broadinstitute.dsde.workbench.leonardo
package runtimes

import cats.effect.IO
import org.broadinstitute.dsde.workbench.google2.{DiskName, GoogleDiskService, ZoneName}
import org.http4s.{AuthScheme, Credentials}
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.scalatest.ParallelTestExecution

//@DoNotDiscover
class RuntimeCreationPdSpec
    extends GPAllocFixtureSpec
    with ParallelTestExecution
    with LeonardoTestUtils
    with GPAllocBeforeAndAfterAll {
  implicit val authTokenForOldApiClient = ronAuthToken
  implicit val auth: Authorization = Authorization(Credentials.Token(AuthScheme.Bearer, ronCreds.makeAuthToken().value))

  val zone = ZoneName("us-central1-a")

  val dependencies = for {
    diskService <- googleDiskService
    httpClient <- LeonardoApiClient.client
  } yield RuntimeCreationPdSpecDep(httpClient, diskService)

  "create and attach a persistent disk" in { googleProject =>
    val runtimeName = randomClusterName
    val diskName = DiskName("test-disk-1")
    val runtimeRequest = defaultRuntimeRequest.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.GceWithPdConfig(
          "gce",
          None,
          PersistentDiskRequest(
            diskName.value,
            Some(50),
            None,
            None,
            Map.empty
          )
        )
      )
    )

    withNewRuntime(googleProject, runtimeName, runtimeRequest, deleteRuntimeAfter = false) { runtime =>
      Leonardo.cluster
        .getRuntime(runtime.googleProject, runtime.clusterName)
        .status shouldBe ClusterStatus.Running

      // validate disk still exists after runtime is deleted
      val res = dependencies.use { dep =>
        implicit val client = dep.httpClient
        for {
          _ <- LeonardoApiClient.deleteRuntimeWithWait(googleProject, runtimeName)
          _ = println("deleted runtime")
          disk <- LeonardoApiClient.getDisk(googleProject, diskName)
          _ <- LeonardoApiClient.deleteDiskWithWait(googleProject, diskName)
          diskAfterDelete <- LeonardoApiClient.getDisk(googleProject, diskName)
          //          diskAfterDelete <- dep.googleDiskService.getDisk(googleProject, zone, diskName).compile.last
        } yield {
          disk.status shouldBe DiskStatus.Ready
          diskAfterDelete.status shouldBe DiskStatus.Deleted
        }
      }
      res.unsafeRunSync()
    }
  }
}

final case class RuntimeCreationPdSpecDep(httpClient: Client[IO], googleDiskService: GoogleDiskService[IO])
