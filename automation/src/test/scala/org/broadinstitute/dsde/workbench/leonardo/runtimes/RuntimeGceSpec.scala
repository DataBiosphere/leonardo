package org.broadinstitute.dsde.workbench.leonardo
package runtimes

import java.util.UUID
import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.Generators.genDiskName
import org.broadinstitute.dsde.workbench.google2.{MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient.defaultCreateRuntime2Request
import org.broadinstitute.dsde.workbench.leonardo.http.{PersistentDiskRequest, RuntimeConfigRequest}
import org.broadinstitute.dsde.workbench.leonardo.notebooks.NotebookTestUtils
import org.broadinstitute.dsde.workbench.model.TraceId
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

@DoNotDiscover
class RuntimeGceSpec
    extends GPAllocFixtureSpec
    with ParallelTestExecution
    with LeonardoTestUtils
    with NotebookTestUtils {
  implicit val authTokenForOldApiClient = ronAuthToken
  implicit val auth: Authorization = Authorization(Credentials.Token(AuthScheme.Bearer, ronCreds.makeAuthToken().value))
  implicit val traceId = Ask.const[IO, TraceId](TraceId(UUID.randomUUID()))

  "should create a GCE instance in a non-default zone" in { project =>
    val runtimeName = randomClusterName
    val diskName = genDiskName.sample.get
    val targetZone = ZoneName(
      "europe-west1-b"
    )

    // In a europe zone
    val createRuntimeRequest = defaultCreateRuntime2Request.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.GceWithPdConfig(
          Some(MachineTypeName("n1-standard-4")),
          PersistentDiskRequest(
            diskName,
            None,
            None,
            Map.empty
          ),
          Some(targetZone)
        )
      )
    )
    val res = LeonardoApiClient.client.use { c =>
      implicit val httpClient = c
      for {
        getRuntimeResponse <- LeonardoApiClient.createRuntimeWithWait(project, runtimeName, createRuntimeRequest)
        _ = getRuntimeResponse.runtimeConfig.asInstanceOf[RuntimeConfig.GceWithPdConfig].zone shouldBe targetZone
        disk <- LeonardoApiClient.getDisk(project, getRuntimeResponse.diskConfig.get.name)
        _ = disk.zone shouldBe targetZone
        _ <- LeonardoApiClient.deleteRuntime(project, runtimeName)
      } yield ()
    }

    res.unsafeRunSync()
  }
}
