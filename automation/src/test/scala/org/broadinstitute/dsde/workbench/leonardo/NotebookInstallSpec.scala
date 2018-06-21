package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.dao.Google.googleStorageDAO
import org.broadinstitute.dsde.workbench.model.google.{EmailGcsEntity, GcsEntityTypes, GcsObjectName, GcsRoles, GoogleProject}
import org.broadinstitute.dsde.workbench.service.Sam

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

    // requires a new cluster because we want to pass in a user script in the cluster request
    "should allow user to create a cluster with a script" in { clusterFixture =>
      withWebDriver { implicit driver =>
        //a cluster without the user script should not be able to import the arrow library
        withNewNotebook(clusterFixture.cluster) { notebookPage =>
          notebookPage.executeCell("""print 'Hello Notebook!'""") shouldBe Some("Hello Notebook!")
          notebookPage.executeCell("""import arrow""").get should include("ImportError: No module named arrow")
        }
      }

      // create a new bucket, add the user script to the bucket, create a new cluster using the URI of the user script and create a notebook that will check if the user script ran
      val gpAllocScriptProject = claimGPAllocProject(hermioneCreds)
      val billingScriptProject = GoogleProject(gpAllocScriptProject.projectName)
      withNewGoogleBucket(billingScriptProject) { bucketName =>
        val ronPetServiceAccount = Sam.user.petServiceAccountEmail(clusterFixture.billingProject.value)(ronAuthToken)
        googleStorageDAO.setBucketAccessControl(bucketName, EmailGcsEntity(GcsEntityTypes.User, ronPetServiceAccount), GcsRoles.Owner)

        val userScriptString = "#!/usr/bin/env bash\n\npip2 install arrow"
        val userScriptObjectName = GcsObjectName("user-script.sh")
        val userScriptUri = s"gs://${bucketName.value}/${userScriptObjectName.value}"

        withNewBucketObject(bucketName, userScriptObjectName, userScriptString, "text/plain") { objectName =>
          googleStorageDAO.setObjectAccessControl(bucketName, objectName, EmailGcsEntity(GcsEntityTypes.User, ronPetServiceAccount), GcsRoles.Owner)
          val clusterName = ClusterName("user-script-cluster" + makeRandomId())

          withWebDriver { implicit driver =>
            withNewCluster(clusterFixture.billingProject, clusterName, ClusterRequest(Map(), None, Option(userScriptUri))) { cluster =>
              Thread.sleep(10000)
              withNewNotebook(cluster) { notebookPage =>
                notebookPage.executeCell("""print 'Hello Notebook!'""") shouldBe Some("Hello Notebook!")
                notebookPage.executeCell("""import arrow""")
                notebookPage.executeCell("""arrow.get(727070400)""") shouldBe Some("<Arrow [1993-01-15T04:00:00+00:00]>")
              }
            }(ronAuthToken)
          }

        }
      }
    }

    //Test to check if extensions are installed correctly
    //Using nbtranslate extension from here:
    //https://github.com/ipython-contrib/jupyter_contrib_nbextensions/tree/master/src/jupyter_contrib_nbextensions/nbextensions/nbTranslate
    "should install user specified notebook extensions" in { clusterFixture =>
      val clusterName = ClusterName("user-jupyter-ext" + makeRandomId())
      withNewCluster(clusterFixture.billingProject, clusterName, ClusterRequest(Map(), Option(testJupyterExtensionUri), None)) { cluster =>
        withWebDriver { implicit driver =>
          withNewNotebook(cluster) { notebookPage =>
            notebookPage.executeCell("1 + 1") shouldBe Some("2")
            //Check if the mark up was translated correctly
            notebookPage.translateMarkup("Hello") should include("Bonjour")
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
            pipInstall(notebookPage, kernel, "tensorflow")
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
