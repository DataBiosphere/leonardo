package org.broadinstitute.dsde.workbench.leonardo.notebooks

import java.io.File

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.rstudio.RStudio
import org.http4s.AuthScheme
import org.http4s.headers.Authorization
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

import scala.concurrent.duration._
import scala.util.Try

/**
 * This spec verifies cluster status transitions like pause/resume and cluster PATCH.
 * It is similar in intent to ClusterStatusTransitionsSpec but uses notebooks for validation,
 * so lives in the notebooks sub-package.
 */
@DoNotDiscover
class NotebookGCEClusterMonitoringSpec extends GPAllocFixtureSpec with ParallelTestExecution with NotebookTestUtils {
  implicit val ronToken: AuthToken = ronAuthToken
  implicit val auth: Authorization = Authorization(
    org.http4s.Credentials.Token(AuthScheme.Bearer, ronCreds.makeAuthToken().value)
  )

  "NotebookGCEClusterMonitoringSpec" - {

    "should pause and resume a cluster" in { billingProject =>
      // Create a cluster
      withNewRuntime(billingProject) { runtime =>
        val printStr = "Pause/resume test"

        withWebDriver { implicit driver =>
          // Create a notebook and execute a cell
          withNewNotebook(runtime, kernel = Python3) { notebookPage =>
            notebookPage.executeCell(s"""print("$printStr")""") shouldBe Some(printStr)
          }

          // Stop the runtime
          stopAndMonitorRuntime(runtime.googleProject, runtime.clusterName)

          // Start the runtime
          startAndMonitorRuntime(runtime.googleProject, runtime.clusterName)

          // TODO make tests rename notebooks?
          val notebookPath = new File("Untitled.ipynb")
          // Use a longer timeout than default because opening notebooks after resume can be slow
          withOpenNotebook(runtime, notebookPath, 10.minutes) { notebookPage =>
            // old output should still exist
            val firstCell = notebookPage.firstCell
            notebookPage.cellOutput(firstCell) shouldBe Some(CellOutput(printStr, None))
            // execute a new cell to make sure the notebook kernel still works
            notebookPage.runAllCells()
            notebookPage.executeCell("sum(range(1,10))") shouldBe Some("45")
          }
        }
      }
    }

    "should pause and resume an RStudio cluster" in { billingProject =>
      implicit val ronToken: AuthToken = ronAuthToken

      // Create a cluster
      withNewRuntime(
        billingProject,
        request = LeonardoApiClient.defaultCreateRuntime2Request.copy(
          toolDockerImage = Some(LeonardoConfig.Leonardo.rstudioBioconductorImage)
        )
      ) { runtime =>
        // Make sure RStudio is up
        // See this ticket for adding more comprehensive selenium tests for RStudio:
        // https://broadworkbench.atlassian.net/browse/IA-697
        val getResult = Try(RStudio.getApi(runtime.googleProject, runtime.clusterName))
        getResult.isSuccess shouldBe true
        getResult.get should include("unsupported_browser")
        getResult.get should not include "ProxyException"

        // Stop the cluster
        stopAndMonitorRuntime(runtime.googleProject, runtime.clusterName)

        // Start the cluster
        startAndMonitorRuntime(runtime.googleProject, runtime.clusterName)

        // RStudio should still be up
        // TODO: also check that the session is preserved after IA-697 is done
        val getResultAfterResume = Try(RStudio.getApi(runtime.googleProject, runtime.clusterName))
        getResultAfterResume.isSuccess shouldBe true
        getResultAfterResume.get should include("unsupported_browser")
        getResultAfterResume.get should not include "ProxyException"
      }
    }

  }

}
