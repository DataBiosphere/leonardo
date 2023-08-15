package org.broadinstitute.dsde.workbench.leonardo.notebooks

import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.{Leonardo, LeonardoConfig, RuntimeFixtureSpec}
import org.scalatest.DoNotDiscover

import scala.concurrent.duration.DurationLong

/**
 * This spec verifies notebook functionality specifically around the Python 3 kernel.
 */
@DoNotDiscover
class NotebookPyKernelSpec extends RuntimeFixtureSpec with NotebookTestUtils {
  implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()

  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.pythonImageUrl)

  "NotebookPyKernelSpec" - {

    "should create a notebook with a working Python 3 kernel, import installed packages and default to notebooks dir in terminal" in {
      runtimeFixture =>
        withWebDriver { implicit driver =>
          withNewNotebook(runtimeFixture.runtime, Python3) { notebookPage =>
            val getPythonVersion =
              """import platform
                |print(platform.python_version())""".stripMargin
            val getBxPython =
              """import bx.bitset
                |bx.bitset.sys.copyright""".stripMargin
            val getPandasLocation = "! pip3 show pandas"
            notebookPage.executeCell("1+1") shouldBe Some("2")
            notebookPage.executeCell(getPythonVersion).get should include("3.10")
            notebookPage.executeCell(getBxPython).get should include("Copyright (c)")
            notebookPage.executeCell(getPandasLocation).get should include("/opt/conda/lib/python3.10/site-packages")
            notebookPage.executeCell("! pwd").get shouldBe "/home/jupyter"
          }
        }
    }

    "should include Content-Security-Policy in headers" in { runtimeFixture =>
      val headers = Notebook.getApiHeaders(runtimeFixture.runtime.googleProject, runtimeFixture.runtime.clusterName)
      val contentSecurityHeader = headers.find(_.name == "Content-Security-Policy")
      contentSecurityHeader shouldBe defined
      contentSecurityHeader.get.value should include("https://bvdp-saturn-dev.appspot.com")
      contentSecurityHeader.get.value should not include "https://bvdp-saturn-prod.appspot.com"
      contentSecurityHeader.get.value should not include "*.terra.bio"
    }

    "should allow BigQuerying via the command line" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime) { notebookPage =>
          val query =
            """! bq query --format=json "SELECT COUNT(*) AS scullion_count FROM [bigquery-public-data.samples.shakespeare] WHERE word='scullion'" """
          val expectedResult = """[{"scullion_count":"2"}]""".stripMargin

          val result = notebookPage.executeCell(query, timeout = 5.minutes).get
          result should include(expectedResult)
        }
      }
    }

    "should update dateAccessed if the notebook is open" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime) { _ =>
          val firstApiCall =
            Leonardo.cluster.getRuntime(runtimeFixture.runtime.googleProject, runtimeFixture.runtime.clusterName)
          // Sleeping for 90s to simulate idle notebook
          logger.info("Sleeping for 120s to simulate idle notebook")
          Thread.sleep(120000)
          val secondApiCall =
            Leonardo.cluster.getRuntime(runtimeFixture.runtime.googleProject, runtimeFixture.runtime.clusterName)
          firstApiCall.auditInfo.dateAccessed should be < secondApiCall.auditInfo.dateAccessed
        }
      }
    }

    s"should have the workspace-related environment variables set in ${Python3.toString} kernel" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        val expectedEVs =
          RuntimeFixtureSpec.getCustomEnvironmentVariables ++
            // variables implicitly set by Leo
            Map(
              "CLUSTER_NAME" -> runtimeFixture.runtime.clusterName.asString,
              "RUNTIME_NAME" -> runtimeFixture.runtime.clusterName.asString,
              "OWNER_EMAIL" -> runtimeFixture.runtime.creator.value,
              // TODO: remove when PPW is rolled out to all workspaces
              // and Leo removes the kernel_bootstrap logic.
              // See https://broadworkbench.atlassian.net/browse/IA-2936
              "WORKSPACE_NAME" -> "home"
            )
        withNewNotebook(runtimeFixture.runtime, Python3) { notebookPage =>
          notebookPage.executeCell("import os")
          expectedEVs.foreach { case (k, v) =>
            val res = notebookPage.executeCell(s"os.getenv('$k')")
            res shouldBe defined
            res.get shouldBe s"'$v'"
          }
        }
      }
    }

    // https://github.com/DataBiosphere/leonardo/issues/891
    "should be able to install python libraries with C bindings" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, Python3) { notebookPage =>
          notebookPage.executeCell("! pip show Cython").get should include("Name: Cython")
          notebookPage.executeCell("! pip install POT").get should include("Successfully installed POT")
        }
      }
    }
  }
}
