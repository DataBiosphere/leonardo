package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.dao.Google.googleStorageDAO
import org.broadinstitute.dsde.workbench.leonardo.Leonardo.ApiVersion.V2
import org.broadinstitute.dsde.workbench.model.google.{EmailGcsEntity, GcsEntityTypes, GcsObjectName, GcsRoles, GoogleProject}
import org.broadinstitute.dsde.workbench.service.Sam
import org.broadinstitute.dsde.workbench.service.util.Tags

import scala.language.postfixOps

class NotebookInstallSpec extends ClusterFixtureSpec {

  "Leonardo notebooks" - {

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
          notebookPage.executeCell(getPythonVersion) shouldBe Some("3.4.2")
          notebookPage.executeCell(getBxPython).get should include("Copyright (c)")
        }
      }
    }

    // This is the negative counterpart of the NotebookUserScriptSpec test named
    // "should allow importing a package that requires a user script that IS installed"
    // to verify that we can only import 'arrow' with the user script specified in that test
    "should error importing a package that requires an uninstalled user script" in { clusterFixture =>
      withWebDriver { implicit driver =>
        //a cluster without the user script should not be able to import the arrow library
        withNewNotebook(clusterFixture.cluster) { notebookPage =>
          notebookPage.executeCell("""print 'Hello Notebook!'""") shouldBe Some("Hello Notebook!")
          notebookPage.executeCell("""import arrow""").get should include("ImportError: No module named arrow")
        }
      }
    }

    //Test to check if extensions are installed correctly
    //Using nbtranslate extension from here:
    //https://github.com/ipython-contrib/jupyter_contrib_nbextensions/tree/master/src/jupyter_contrib_nbextensions/nbextensions/nbTranslate
    "should install user specified notebook extensions" in { clusterFixture =>
      val translateExtensionFile = ResourceFile("bucket-tests/translate_nbextension.tar.gz")
      withResourceFileInBucket(clusterFixture.billingProject, translateExtensionFile, "application/x-gzip") { translateExtensionBucketPath =>
        val clusterName = ClusterName("user-jupyter-ext" + makeRandomId())
        withNewCluster(clusterFixture.billingProject, clusterName, ClusterRequest(Map(), Option(translateExtensionBucketPath.toUri), None)) { cluster =>
          withWebDriver { implicit driver =>
            withNewNotebook(cluster) { notebookPage =>
              notebookPage.executeCell("1 + 1") shouldBe Some("2")
              //Check if the mark up was translated correctly
              notebookPage.translateMarkup("Hello") should include("Bonjour")
            }
          }
        }
      }
    }

    val sparkCommandToFail =
      """try:
        |    name = sc.appName
        |
        |except NameError as err:
        |    print(err)""".stripMargin


    Seq(Python2, Python3).foreach { kernel =>
      s"should be able to pip install packages using ${kernel.string}" in { clusterFixture =>
        withWebDriver { implicit driver =>
          withNewNotebook(clusterFixture.cluster, kernel) { notebookPage =>
            // install tensorflow
            pipInstall(notebookPage, kernel, "tensorflow==1.9.0")
            notebookPage.saveAndCheckpoint()
          }
        }

        withWebDriver { implicit driver =>
          // need to restart the kernel for the install to take effect
          withNewNotebook(clusterFixture.cluster, kernel) { notebookPage =>
            // verify that tensorflow is installed
            verifyTensorFlow(notebookPage, kernel)
          }
        }
      }

      s"should NOT be able to run Spark using ${kernel.string}" in { clusterFixture =>
        withWebDriver { implicit driver =>
          withNewNotebook(clusterFixture.cluster, kernel) { notebookPage =>
            // As proof of not having Spark installed:
            // We should get an error upon attempting to access the SparkContext object 'sc'
            // since Python kernels do not include Spark installation.
            val sparkErrorMessage = "name 'sc' is not defined"
            notebookPage.executeCell(sparkCommandToFail).get shouldBe sparkErrorMessage
          }
        }
      }
    }

  }

}
