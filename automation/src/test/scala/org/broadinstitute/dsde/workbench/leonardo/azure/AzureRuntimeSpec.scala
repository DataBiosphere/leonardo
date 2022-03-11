package org.broadinstitute.dsde.workbench.leonardo.azure

import cats.effect.IO
import org.broadinstitute.dsde.workbench.google2.streamUntilDoneOrTimeout
import org.broadinstitute.dsde.workbench.leonardo.TestUser.{Ron, getAuthTokenAndAuthorization}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Outcome, Retries, ParallelTestExecution}
import org.broadinstitute.dsde.workbench.leonardo.{ClusterStatus, AppName, RuntimeStatus, GPAllocFixtureSpec, GPAllocUtils, GPAllocBeforeAndAfterAll, LeonardoApiClient, LeonardoTestUtils}
import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient._
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.http4s.{Credentials, AuthScheme}
import org.http4s.headers.Authorization
import org.scalatest.freespec.FixtureAnyFreeSpec
import org.scalatest.tagobjects.Retryable

import scala.concurrent.duration._

//@DoNotDiscover
class AzureRuntimeSpec extends FixtureAnyFreeSpec
  with LeonardoTestUtils
  with ParallelTestExecution
  with TableDrivenPropertyChecks
  with Retries {
  implicit val (ronAuthToken, ronAuthorization) = getAuthTokenAndAuthorization(Ron)

  "create, get, delete azure runtime" taggedAs (Tags.SmokeTest, Retryable) in { _ =>
      val runtimeName = randomClusterName

    withNewWorkspace { workspaceId =>
      LeonardoApiClient.client.use { implicit client =>
        for {
          _ <- loggerIO.info(s"AzureRuntimeSpec: About to create runtime")

          rat <- Ron.authToken()
          implicit0(auth: Authorization) = Authorization(Credentials.Token(AuthScheme.Bearer, rat.value))

//           Create the app
            _ <- LeonardoApiClient.createAzureRuntime(workspaceId, runtimeName)

          _ <- LeonardoApiClient.createAzureRuntime(workspaceId, runtimeName)

          // Verify the initial getApp call
          getRuntime = LeonardoApiClient.getAzureRuntime(workspaceId, runtimeName)
          getRuntimeResponse <- getRuntime
          _ = getRuntimeResponse.status should (be(ClusterStatus.Creating) or be(ClusterStatus.PreCreating))

          // Verify the runtime eventually becomes Running
          _ <- IO.sleep(120 seconds)
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
    }
  }

  override def withFixture(test: NoArgTest) =
    if (isRetryable(test))
      withRetry(super.withFixture(test))
    else
      super.withFixture(test)

  override type FixtureParam = this.type

  override protected def withFixture(test: OneArgTest): Outcome =
    if (isRetryable(test))
      withRetry(super.withFixture(test))
    else {
      super.withFixture(test)
    }
}
