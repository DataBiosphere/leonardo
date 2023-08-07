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

    Seq(Python3).foreach { kernel =>
      s"should be able to pip install packages using ${kernel.string}" in { runtimeFixture =>
        withWebDriver { implicit driver =>
          withNewNotebook(runtimeFixture.runtime, kernel) { notebookPage =>
            // install a package that is not installed by default
            notebookPage.executeCell("import fuzzywuzzy").getOrElse("") should include("ModuleNotFoundError")
            pipInstall(notebookPage, kernel, "fuzzywuzzy")
            notebookPage.saveAndCheckpoint()

            // need to restart the kernel for the install to take effect
            notebookPage.restartKernel()

            notebookPage.executeCell("import fuzzywuzzy", cellNumberOpt = Some(1)) shouldBe None
            // user installed packages should be in directory where PD is mounted
            val getFuzzyWuzzyLocation = "! pip3 show fuzzywuzzy"
            notebookPage.executeCell(getFuzzyWuzzyLocation, cellNumberOpt = Some(1)).get should include(
              "/home/jupyter/.local/lib/python3.10/site-packages"
            )
          }
        }
      }
    }

    "should NOT be able to run Spark" in { runtimeFixture =>
      val sparkCommandToFail =
        """try:
          |    name = sc.appName
          |
          |except NameError as err:
          |    print(err)""".stripMargin

      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime) { notebookPage =>
          // As proof of not having Spark installed:
          // We should get an error upon attempting to access the SparkContext object 'sc'
          // since Python kernels do not include Spark installation.
          val sparkErrorMessage = "name 'sc' is not defined"
          notebookPage.executeCell(sparkCommandToFail).get shouldBe sparkErrorMessage
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

    "should allow BigQuerying through python" in { runtimeFixture =>
      val query = """"SELECT
                    |CONCAT(
                    |'https://stackoverflow.com/questions/',
                    |CAST(id as STRING)) as url,
                    |view_count
                    |FROM `bigquery-public-data.stackoverflow.posts_questions`
                    |WHERE tags like '%google-bigquery%'
                    |ORDER BY view_count DESC
                    |LIMIT 10"""".stripMargin
      val bigQuery = s"""from google.cloud import bigquery
                        |bigquery_client = bigquery.Client()
                        |dataset_id = 'my_new_dataset'
                        |dataset_ref = bigquery_client.dataset(dataset_id)
                        |dataset = bigquery.Dataset(dataset_ref)
                        |query_job = bigquery_client.query(""${query}"")
                        |results = query_job.result()""".stripMargin

      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime) { notebookPage =>
          notebookPage.executeCell(bigQuery) shouldBe None
          notebookPage.executeCell("print(results)").get should include(
            "google.cloud.bigquery.table.RowIterator object"
          )
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

    // TODO: uncomment this

    Seq(Python3).foreach { kernel =>
      s"should preinstall google cloud subpackages for ${kernel.string}" in { runtimeFixture =>
        withWebDriver { implicit driver =>
          withNewNotebook(runtimeFixture.runtime, kernel) { notebookPage =>
            // we can no longer test for the version explicitly for packages because we will always have the newest versions
            // as they become available. see https://github.com/DataBiosphere/terra-docker/pull/207
            // warnings are ignored because they are benign warnings that show up for python2 because of compilation against an older numpy
            notebookPage
              .executeCell(
                "import warnings; warnings.simplefilter('ignore')\nfrom google.cloud import bigquery\nprint(bigquery.__version__)"
              )
              .get should not include "Error"
            notebookPage
              .executeCell("from google.cloud import datastore\nprint(datastore.__version__)")
              .get should not include "Error"
            notebookPage
              .executeCell("from google.cloud import storage\nprint(storage.__version__)")
              .get should not include "Error"
          }
        }
      }
    }

    // https://github.com/DataBiosphere/leonardo/issues/797
    s"should be able to import ggplot for ${Python3.toString}" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, Python3) { notebookPage =>
          notebookPage.executeCell("from ggplot import *") shouldBe None
          notebookPage.executeCell("ggplot") shouldBe Some("ggplot.ggplot.ggplot")
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
