package org.broadinstitute.dsde.workbench.leonardo.notebooks

import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.TestUser.{getAuthTokenAndAuthorization, Ron}
import org.broadinstitute.dsde.workbench.leonardo.{
  BillingProjectFixtureSpec,
  LeonardoApiClient,
  UserJupyterExtensionConfig
}
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

import scala.concurrent.duration._

/**
 * This spec verifies different cluster creation options, such as extensions, scopes, environment variables.
 *
 * TODO consider removing this spec and moving test cases to RuntimeGceSpec.
 */
@DoNotDiscover
final class NotebookGCECustomizationSpec
    extends BillingProjectFixtureSpec
    with ParallelTestExecution
    with NotebookTestUtils {
  implicit val (ronAuthToken, ronAuthorization) = getAuthTokenAndAuthorization(Ron)
  implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()

    "should populate user-specified environment variables" in { billingProject =>
      // Note: the R image includes R and Python 3 kernels
      val runtimeRequest =
        LeonardoApiClient.defaultCreateRuntime2Request.copy(customEnvironmentVariables = Map("KEY" -> "value"))

      withNewRuntime(billingProject, request = runtimeRequest) { cluster =>
        withWebDriver { implicit driver =>
          withNewNotebook(cluster, Python3) { notebookPage =>
            notebookPage.executeCell("import os")

            val envVar = notebookPage.executeCell("os.getenv('KEY')")
            envVar shouldBe Some("'value'")
          }
        }
      }
    }
  }
}
