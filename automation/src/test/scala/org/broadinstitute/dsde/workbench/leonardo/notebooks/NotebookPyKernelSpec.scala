package org.broadinstitute.dsde.workbench.leonardo.notebooks

import org.broadinstitute.dsde.workbench.leonardo.{ClusterFixtureSpec, Leonardo}
import org.broadinstitute.dsde.workbench.service.Orchestration
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.DoNotDiscover

import scala.concurrent.duration.DurationLong
import scala.language.postfixOps

/**
  * This spec verifies notebook functionality specifically around the Python 2 and 3 kernels.
  */
@DoNotDiscover
class NotebookPyKernelSpec extends ClusterFixtureSpec with NotebookTestUtils {

  "NotebookPyKernelSpec" - {

    "should create a notebook with a working Python 3 kernel and import installed packages" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, Python3) { notebookPage =>
          val getPythonVersion =
            """import platform
              |print(platform.python_version())""".stripMargin
          val getBxPython =
            """import bx.bitset
              |bx.bitset.sys.copyright""".stripMargin

          notebookPage.executeCell("1+1") shouldBe Some("2")
          notebookPage.executeCell(getPythonVersion) shouldBe Some("3.6.8")
          notebookPage.executeCell(getBxPython).get should include("Copyright (c)")
        }
      }
    }

    // This is the negative counterpart of the NotebookUserScriptSpec test named
    // "should allow importing a package that requires a user script that IS installed"
    // to verify that we can only import 'arrow' with the user script specified in that test
    "should error importing a package that isn't installed" in { clusterFixture =>
      withWebDriver { implicit driver =>
        //a cluster without the user script should not be able to import the arrow library
        withNewNotebook(clusterFixture.cluster) { notebookPage =>
          notebookPage.executeCell("""print('Hello Notebook!)'""") shouldBe Some("Hello Notebook!")
          notebookPage.executeCell("""import arrow""").get should include("ImportError: No module named arrow")
        }
      }
    }

    Seq(Python2, Python3).foreach { kernel =>
      s"should be able to pip install packages using ${kernel.string}" in { clusterFixture =>
        withWebDriver { implicit driver =>
          withNewNotebook(clusterFixture.cluster, kernel) { notebookPage =>
            // install a package that is not installed by default
            notebookPage.executeCell("import fuzzywuzzy").getOrElse("") should include (if (kernel == Python2) "ImportError" else "ModuleNotFoundError")
            pipInstall(notebookPage, kernel, "fuzzywuzzy")
            notebookPage.saveAndCheckpoint()

            // need to restart the kernel for the install to take effect
            notebookPage.restartKernel()

            notebookPage.executeCell("import fuzzywuzzy", cellNumberOpt = Some(1)) shouldBe Some("")
          }
        }
      }
    }

    "should NOT be able to run Spark" in { clusterFixture =>
      val sparkCommandToFail =
        """try:
          |    name = sc.appName
          |
          |except NameError as err:
          |    print(err)""".stripMargin

      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster) { notebookPage =>
          // As proof of not having Spark installed:
          // We should get an error upon attempting to access the SparkContext object 'sc'
          // since Python kernels do not include Spark installation.
          val sparkErrorMessage = "name 'sc' is not defined"
          notebookPage.executeCell(sparkCommandToFail).get shouldBe sparkErrorMessage
        }
      }
    }

    "should include Content-Security-Policy in headers" in { clusterFixture =>
      val headers = Notebook.getApiHeaders(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)
      val contentSecurityHeader = headers.find(_.name == "Content-Security-Policy")
      contentSecurityHeader shouldBe 'defined
      contentSecurityHeader.get.value should include ("https://bvdp-saturn-prod.appspot.com")
    }

    "should allow BigQuerying via the command line" in { clusterFixture =>
      // project owners have the bigquery role automatically, so this also tests granting it to users
      val ownerToken = hermioneAuthToken
      Orchestration.billing.addGoogleRoleToBillingProjectUser(clusterFixture.cluster.googleProject.value, ronEmail, "bigquery.jobUser")(ownerToken)
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster) { notebookPage =>
          val query = """! bq query --format=json "SELECT COUNT(*) AS scullion_count FROM publicdata.samples.shakespeare WHERE word='scullion'" """
          val expectedResult = """[{"scullion_count":"2"}]""".stripMargin

          val result = notebookPage.executeCell(query, timeout = 5.minutes).get
          result should include("Current status: DONE")
          result should include(expectedResult)
        }
      }
    }

    "should allow BigQuerying through python" taggedAs Tags.SmokeTest in { clusterFixture =>
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
        withNewNotebook(clusterFixture.cluster) { notebookPage =>
          notebookPage.executeCell(bigQuery) shouldBe None
          notebookPage.executeCell("print(results)").get should include("google.cloud.bigquery.table.RowIterator object")
        }
      }
    }

    "should update dateAccessed if the notebook is open" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster) { notebookPage =>
          val firstApiCall = Leonardo.cluster.get(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)
          //Sleeping for 90s to simulate idle notebook
          logger.info("Sleeping for 90s to simulate idle notebook")
          Thread.sleep(90000)
          val secondApiCall = Leonardo.cluster.get(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)
          firstApiCall.dateAccessed should be < secondApiCall.dateAccessed
        }
      }
    }


    Seq(Python2, Python3).foreach { kernel =>
      s"should preinstall google cloud subpackages for ${kernel.string}" in { clusterFixture =>
        withWebDriver { implicit driver =>
          withNewNotebook(clusterFixture.cluster, kernel) { notebookPage =>
            //all other packages cannot be tested for their versions in this manner
            //warnings are ignored because they are benign warnings that show up for python2 because of compilation against an older numpy
            notebookPage.executeCell("import warnings; warnings.simplefilter('ignore')\nfrom google.cloud import bigquery\nprint(bigquery.__version__)") shouldBe Some("1.7.0")
            notebookPage.executeCell("from google.cloud import datastore\nprint(datastore.__version__)") shouldBe Some("1.7.0")
            notebookPage.executeCell("from google.cloud import storage\nprint(storage.__version__)") shouldBe Some("1.13.0")
          }
        }
      }
    }

    // https://github.com/DataBiosphere/leonardo/issues/797
    Seq(Python2, Python3).foreach { kernel =>
      s"should be able to import ggplot for ${kernel.toString}" in { clusterFixture =>
        withWebDriver { implicit driver =>
          withNewNotebook(clusterFixture.cluster, kernel) { notebookPage =>
            notebookPage.executeCell("from ggplot import *").get should not include ("ImportError")
            notebookPage.executeCell("ggplot") shouldBe Some("ggplot.ggplot.ggplot")
          }
        }
      }
    }

    Seq(Python2, Python3).foreach { kernel =>
      s"should have the workspace-related environment variables set in ${kernel.toString} kernel" in { clusterFixture =>
        withWebDriver { implicit driver =>
          withNewNotebook(clusterFixture.cluster, kernel) { notebookPage =>
            notebookPage.executeCell("! echo $GOOGLE_PROJECT").get shouldBe clusterFixture.cluster.googleProject.value
            notebookPage.executeCell("! echo $WORKSPACE_NAMESPACE").get shouldBe clusterFixture.cluster.googleProject.value
            notebookPage.executeCell("! echo $WORKSPACE_NAME").get shouldBe "notebooks"
            // workspace bucket is not wired up in tests
            notebookPage.executeCell("! echo $WORKSPACE_BUCKET").get shouldBe ""
          }
        }
      }
    }

    // https://github.com/DataBiosphere/leonardo/issues/891
    "should be able to install python libraries with C bindings" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, Python3) { notebookPage =>
          notebookPage.executeCell("! pip install Cython").get should include ("Successfully installed Cython")
          notebookPage.executeCell("! pip install POT").get should include ("Successfully installed POT")
        }
      }
    }

    "should use pet credentials" in { clusterFixture =>
      val petEmail = getAndVerifyPet(clusterFixture.cluster.googleProject)

      // cluster should have been created with the pet service account
      clusterFixture.cluster.serviceAccountInfo.clusterServiceAccount shouldBe Some(petEmail)
      clusterFixture.cluster.serviceAccountInfo.notebookServiceAccount shouldBe None

      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, PySpark2) { notebookPage =>
          // should not have notebook credentials because Leo is not configured to use a notebook service account
          verifyNoNotebookCredentials(notebookPage)
        }
      }
    }
  }
}
