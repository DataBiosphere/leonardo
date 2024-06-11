package org.broadinstitute.dsde.workbench.leonardo
package runtimes

import cats.effect.IO
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, DiskName, MachineTypeName}
import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient._
import org.broadinstitute.dsde.workbench.leonardo.TestUser.{getAuthTokenAndAuthorization, Ron}
import org.broadinstitute.dsde.workbench.leonardo.http.{
  PersistentDiskRequest,
  RuntimeConfigRequest,
  UpdateRuntimeConfigRequest,
  UpdateRuntimeRequest
}
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.http4s.headers.Authorization
import org.scalatest.tagobjects.Retryable
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

import scala.concurrent.duration._

@DoNotDiscover
class RuntimePatchSpec extends BillingProjectFixtureSpec with ParallelTestExecution with LeonardoTestUtils {
  implicit val (ronAuthToken: IO[AuthToken], ronAuthorization: IO[Authorization]) = getAuthTokenAndAuthorization(Ron)

  override def withFixture(test: NoArgTest) =
    if (isRetryable(test))
      withRetry(super.withFixture(test))
    else
      super.withFixture(test)

  // this is an end to end test of the pub/sub infrastructure
  // TODO: verify startup script on start https://broadworkbench.atlassian.net/browse/IA-4931
  "Patch endpoint should perform a stop/start transition for GCE VM with PD" taggedAs (Tags.SmokeTest, Retryable) in {
    googleProject =>
      // create a new GCE runtime
      val runtimeName = randomClusterName

      val newMasterMachineType = MachineTypeName("n1-standard-2")
      val newDiskSize = DiskSize(20)
      val updateRuntimeRequest = UpdateRuntimeRequest(Some(
                                                        UpdateRuntimeConfigRequest.GceConfig(
                                                          Some(newMasterMachineType),
                                                          Some(newDiskSize)
                                                        )
                                                      ),
                                                      true,
                                                      None,
                                                      None,
                                                      Map.empty,
                                                      Set.empty
      )
      val createRuntimeRequest = defaultCreateRuntime2Request.copy(
        runtimeConfig = Some(
          RuntimeConfigRequest.GceWithPdConfig(
            Some(MachineTypeName("n1-standard-4")),
            PersistentDiskRequest(
              DiskName("pd-test"),
              Some(DiskSize(10)),
              None,
              Map.empty
            ),
            None,
            None
          )
        )
      )

      val res = LeonardoApiClient.client.use { implicit c =>
        val startingDoneCheckable: DoneCheckable[GetRuntimeResponseCopy] =
          x => x.status == ClusterStatus.Running

        for {
          _ <- createRuntimeWithWait(googleProject, runtimeName, createRuntimeRequest)
          _ <- updateRuntime(googleProject, runtimeName, updateRuntimeRequest)
          _ <- IO.sleep(70 seconds) // We need this because DB update happens in subscriber for update API.
          ioa = LeonardoApiClient.getRuntime(googleProject, runtimeName)
          getRuntimeResult <- ioa
          monitoringStartingResult <- IO.sleep(50 seconds) >> streamFUntilDone(ioa, 30, 10 seconds)(
            implicitly,
            startingDoneCheckable
          ).compile.lastOrError
        } yield {
          monitoringStartingResult.status shouldBe ClusterStatus.Running
          val res = monitoringStartingResult.runtimeConfig
            .asInstanceOf[RuntimeConfig.GceWithPdConfig]
          res.machineType shouldBe newMasterMachineType
        }
      }
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
}
