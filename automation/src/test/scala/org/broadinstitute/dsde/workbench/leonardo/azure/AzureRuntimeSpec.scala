package org.broadinstitute.dsde.workbench.leonardo.azure

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.google2.streamUntilDoneOrTimeout
import org.broadinstitute.dsde.workbench.leonardo.LeonardoConfig.Leonardo.workspaceId
import org.broadinstitute.dsde.workbench.leonardo.TestUser.{getAuthTokenAndAuthorization, Ron}
import org.broadinstitute.dsde.workbench.leonardo.{ClusterStatus, LeonardoApiClient, LeonardoTestUtils}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{ParallelTestExecution, Retries}

import scala.concurrent.duration._

//@DoNotDiscover
class AzureRuntimeSpec
    extends AnyFlatSpec
    with LeonardoTestUtils
    with ParallelTestExecution
    with TableDrivenPropertyChecks
    with Retries {
  implicit val (ronAuthToken, ronAuthorization) = getAuthTokenAndAuthorization(Ron)

  it should "create, get, delete azure runtime" in {
    val runtimeName = randomClusterName
    val res = LeonardoApiClient.client.use { implicit client =>
      for {
        _ <- loggerIO.info(s"AzureRuntimeSpec: About to create runtime")
//        rat <- Ron.authToken()
//           Create the app
        _ <- LeonardoApiClient.createAzureRuntime(workspaceId, runtimeName)

        // Verify the initial getApp call
        getRuntime = LeonardoApiClient.getAzureRuntime(workspaceId, runtimeName)
        getRuntimeResponse <- getRuntime
        _ = getRuntimeResponse.status should (be(ClusterStatus.Creating) or be(ClusterStatus.PreCreating))

        // Verify the runtime eventually becomes Running
        _ <- IO.sleep(300 seconds)
        monitorCreateResult <- streamUntilDoneOrTimeout(
          getRuntime,
          120,
          10 seconds,
          s"AzureRuntimeSpec: runtime ${workspaceId.value}/${runtimeName.asString} did not finish creating after 20 minutes"
        )(implicitly, runtimeInStateOrError(ClusterStatus.Running))
        _ <- loggerIO.info(
          s"AzureRuntime: runtime ${workspaceId.value}/${runtimeName.asString} monitor result: $monitorCreateResult"
        )
        _ = monitorCreateResult.status shouldBe ClusterStatus.Running

        _ <- IO.sleep(1 minute)

        // Delete the app
        _ <- LeonardoApiClient.deleteAzureRuntime(workspaceId, runtimeName)

        // Verify getApp again
        getRuntimeResponse <- getRuntime
        _ = getRuntimeResponse.status should (be(ClusterStatus.Deleting) or be(ClusterStatus.PreDeleting))

        //TODO: eventually with list we can verify deleted
      } yield ()
    }
    res.unsafeRunSync()
  }
}
