package org.broadinstitute.dsde.workbench.leonardo.notebooks

import java.io.File

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.rstudio.RStudioTestUtils
import org.http4s.AuthScheme
import org.http4s.headers.Authorization
import org.openqa.selenium.Keys
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

import scala.concurrent.duration._

/**
 * This spec verifies cluster status transitions like pause/resume and cluster PATCH.
 * It is similar in intent to ClusterStatusTransitionsSpec but uses notebooks for validation,
 * so lives in the notebooks sub-package.
 */
@DoNotDiscover
class NotebookGCEClusterMonitoringSpec
    extends GPAllocFixtureSpec
    with ParallelTestExecution
    with NotebookTestUtils
    with RStudioTestUtils {
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
        withWebDriver { implicit driver =>
          // Make sure RStudio is up
          withNewRStudio(runtime) { rstudioPage =>
            rstudioPage.pressKeys("""ns <- Sys.getEnv("WORKSPACE_NAMESPACE")""")
            rstudioPage.pressKeys(Keys.ENTER.toString)
            await visible cssSelector("[title~='ns']")
            rstudioPage.variableExists("ns") shouldBe true
            rstudioPage.variableExists(runtime.googleProject.value) shouldBe true
          }

          // Stop the cluster
          stopAndMonitorRuntime(runtime.googleProject, runtime.clusterName)

          // Start the cluster
          startAndMonitorRuntime(runtime.googleProject, runtime.clusterName)

          // Make sure RStudio session is preserved
          withNewRStudio(runtime) { rstudioPage =>
            await visible cssSelector("[title~='ns']")
            rstudioPage.variableExists("ns") shouldBe true
            rstudioPage.variableExists(runtime.googleProject.value) shouldBe true
          }
        }
      }
    }

  }

}
