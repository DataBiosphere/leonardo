package org.broadinstitute.dsde.workbench.leonardo.notebooks

import java.io.File

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.dao.Google.googleStorageDAO
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.rstudio.RStudio
import org.broadinstitute.dsde.workbench.model.google.GcsEntityTypes.Group
import org.broadinstitute.dsde.workbench.model.google.GcsRoles.Reader
import org.broadinstitute.dsde.workbench.model.google.{parseGcsPath, EmailGcsEntity, GcsObjectName, GcsPath}
import org.broadinstitute.dsde.workbench.service.Sam
import org.scalatest.tagobjects.Retryable
import org.scalatest.time.{Seconds, Span}
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

  "NotebookGCEClusterMonitoringSpec" - {

    "should pause and resume a cluster" in { billingProject =>
      implicit val ronToken: AuthToken = ronAuthToken

      // Create a cluster
      withNewRuntime(billingProject) { cluster =>
        val printStr = "Pause/resume test"

        withWebDriver { implicit driver =>
          // Create a notebook and execute a cell
          withNewNotebook(cluster, kernel = Python3) { notebookPage =>
            notebookPage.executeCell(s"""print("$printStr")""") shouldBe Some(printStr)
          }

          // Stop the cluster
          stopAndMonitorRuntime(cluster.googleProject, cluster.clusterName)

          // Start the cluster
          startAndMonitorRuntime(cluster.googleProject, cluster.clusterName)

          // TODO make tests rename notebooks?
          val notebookPath = new File("Untitled.ipynb")
          // Use a longer timeout than default because opening notebooks after resume can be slow
          withOpenNotebook(cluster, notebookPath, 10.minutes) { notebookPage =>
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

    "should update welder on a cluster" taggedAs Retryable in { billingProject =>
      implicit val ronToken: AuthToken = ronAuthToken
      val deployWelderLabel = "saturnVersion" // matches deployWelderLabel in Leo reference.conf

      // Create a cluster with welder disabled

      withNewRuntime(
        billingProject,
        request = defaultRuntimeRequest.copy(labels = Map(deployWelderLabel -> "true"),
                                             welderDockerImage = Some(LeonardoConfig.Leonardo.oldWelderDockerImage))
      ) { cluster =>
        // Verify welder is running with old version
        val statusResponse = Welder.getWelderStatus(cluster).attempt.unsafeRunSync()
        statusResponse.isRight shouldBe true
        val oldWelderHash = LeonardoConfig.Leonardo.oldWelderDockerImage.split(":")(1)
        statusResponse.toOption.get.gitHeadCommit should startWith(oldWelderHash)

        // Stop the cluster
        stopAndMonitorRuntime(cluster.googleProject, cluster.clusterName)

        // Start the cluster
        startAndMonitorRuntime(cluster.googleProject, cluster.clusterName)

        // Verify welder is now running
        val curWelderHash = LeonardoConfig.Leonardo.curWelderDockerImage.split(":")(1)
        val newStatusResponse = Welder.getWelderStatus(cluster).attempt.unsafeRunSync()
        newStatusResponse.isRight shouldBe true
        newStatusResponse.toOption.get.gitHeadCommit should startWith(curWelderHash)
      }
    }

    // TODO: remove this test once we stop supporting the legacy image
    "should set environment variables for old image" in { billingProject =>
      implicit val ronToken: AuthToken = ronAuthToken

      withNewRuntime(billingProject,
                     request = defaultRuntimeRequest.copy(toolDockerImage = None /*enableWelder = Some(true)*/ )) {
        cluster =>
          withWebDriver { implicit driver =>
            withNewNotebookInSubfolder(cluster, Python3) { notebookPage =>
              notebookPage.executeCell("import os")
              notebookPage.executeCell("os.getenv('GOOGLE_PROJECT')").get shouldBe s"'${billingProject.value}'"
              notebookPage.executeCell("os.getenv('WORKSPACE_NAMESPACE')").get shouldBe s"'${billingProject.value}'"
              notebookPage.executeCell("os.getenv('WORKSPACE_NAME')").get shouldBe "'Untitled Folder'"
              notebookPage.executeCell("os.getenv('OWNER_EMAIL')").get shouldBe s"'${ronEmail}'"
              // workspace bucket is not wired up in tests
              notebookPage.executeCell("os.getenv('WORKSPACE_BUCKET')") shouldBe None
            }
          }
      }
    }

    "should pause and resume an RStudio cluster" in { billingProject =>
      implicit val ronToken: AuthToken = ronAuthToken

      // Create a cluster
      withNewRuntime(
        billingProject,
        request = defaultRuntimeRequest.copy(
          toolDockerImage = Some(LeonardoConfig.Leonardo.rstudioBaseImageUrl) /* enableWelder = Some(true)*/
        )
      ) { cluster =>
        // Make sure RStudio is up
        // See this ticket for adding more comprehensive selenium tests for RStudio:
        // https://broadworkbench.atlassian.net/browse/IA-697
        val getResult = Try(RStudio.getApi(cluster.googleProject, cluster.clusterName))
        getResult.isSuccess shouldBe true
        getResult.get should include("unsupported_browser")
        getResult.get should not include "ProxyException"

        // Stop the cluster
        stopAndMonitorRuntime(cluster.googleProject, cluster.clusterName)

        // Start the cluster
        startAndMonitorRuntime(cluster.googleProject, cluster.clusterName)

        // RStudio should still be up
        // TODO: also check that the session is preserved after IA-697 is done
        val getResultAfterResume = Try(RStudio.getApi(cluster.googleProject, cluster.clusterName))
        getResultAfterResume.isSuccess shouldBe true
        getResultAfterResume.get should include("unsupported_browser")
        getResultAfterResume.get should not include "ProxyException"
      }
    }

  }

}
