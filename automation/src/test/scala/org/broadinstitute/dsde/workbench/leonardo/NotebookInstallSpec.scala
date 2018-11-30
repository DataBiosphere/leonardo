package org.broadinstitute.dsde.workbench.leonardo

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
          notebookPage.executeCell(getPythonVersion) shouldBe Some("3.6.7")
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
