package org.broadinstitute.dsde.workbench.leonardo.notebooks

import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.dao.Google.googleStorageDAO
import org.broadinstitute.dsde.workbench.leonardo.{GPAllocFixtureSpec, LeonardoConfig, MachineConfig}
import org.broadinstitute.dsde.workbench.model.google.{EmailGcsEntity, GcsEntityTypes, GcsObjectName, GcsRoles}
import org.broadinstitute.dsde.workbench.service.Sam
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

import scala.concurrent.duration._

/**
 * This spec verfies different cluster creation options, such as user scripts, extensions, etc.
 */
@DoNotDiscover
final class NotebookCustomizationSpec extends GPAllocFixtureSpec with ParallelTestExecution with NotebookTestUtils {

  "NotebookCustomizationSpec" - {

    "should run a user script" taggedAs Tags.SmokeTest in { billingProject =>
      implicit val ronToken: AuthToken = ronAuthToken

      // Create a new bucket
      withNewGoogleBucket(billingProject) { bucketName =>
        val ronPetServiceAccount = Sam.user.petServiceAccountEmail(billingProject.value)(ronAuthToken)
        googleStorageDAO.setBucketAccessControl(bucketName,
                                                EmailGcsEntity(GcsEntityTypes.User, ronPetServiceAccount),
                                                GcsRoles.Owner)

        // Add the user script to the bucket
        val userScriptString = "#!/usr/bin/env bash\n\npip3 install mock"
        val userScriptObjectName = GcsObjectName("user-script.sh")
        val userScriptUri = s"gs://${bucketName.value}/${userScriptObjectName.value}"

        withNewBucketObject(bucketName, userScriptObjectName, userScriptString, "text/plain") { objectName =>
          googleStorageDAO.setObjectAccessControl(bucketName,
                                                  objectName,
                                                  EmailGcsEntity(GcsEntityTypes.User, ronPetServiceAccount),
                                                  GcsRoles.Owner)

          // Create a new cluster using the URI of the user script
          val clusterRequestWithUserScript = defaultClusterRequest.copy(Map(), None, Option(userScriptUri))
          withNewCluster(billingProject, request = clusterRequestWithUserScript) { cluster =>
            Thread.sleep(10000)
            withWebDriver { implicit driver =>
              // Create a notebook that will check if the user script ran
              withNewNotebook(cluster, Python3) { notebookPage =>
                notebookPage.executeCell("""print("Hello Notebook!")""") shouldBe Some("Hello Notebook!")
                notebookPage.executeCell("""import mock""") shouldBe None
              }
            }
          }(ronAuthToken)
        }
      }
    }

    // Using nbtranslate extension from here:
    // https://github.com/ipython-contrib/jupyter_contrib_nbextensions/tree/master/src/jupyter_contrib_nbextensions/nbextensions/nbTranslate
    "should install user specified notebook extensions" in { billingProject =>
      implicit val ronToken: AuthToken = ronAuthToken

      val translateExtensionFile = ResourceFile("bucket-tests/translate_nbextension.tar.gz")
      withResourceFileInBucket(billingProject, translateExtensionFile, "application/x-gzip") {
        translateExtensionBucketPath =>
          val extensionConfig = multiExtensionClusterRequest.copy(
            nbExtensions = multiExtensionClusterRequest.nbExtensions + ("translate" -> translateExtensionBucketPath.toUri)
          )
          withNewCluster(billingProject,
                         request = defaultClusterRequest.copy(userJupyterExtensionConfig = Some(extensionConfig))) {
            cluster =>
              withWebDriver { implicit driver =>
                withNewNotebook(cluster, Python3) { notebookPage =>
                  // Check the extensions were installed
                  val nbExt = notebookPage.executeCell("! jupyter nbextension list")
                  nbExt.get should include("jupyter-gmaps/extension  enabled")
                  nbExt.get should include("pizzabutton/index  enabled")
                  nbExt.get should include("translate_nbextension/main  enabled")
                  // should be installed by default
                  nbExt.get should include("toc2/main  enabled")

                  val serverExt = notebookPage.executeCell("! jupyter serverextension list")
                  serverExt.get should include("pizzabutton  enabled")
                  serverExt.get should include("jupyterlab  enabled")
                  // should be installed by default
                  serverExt.get should include("jupyter_nbextensions_configurator  enabled")

                  // Exercise the translate extension
                  notebookPage.translateMarkup("Yes") should include("Oui")
                }
              }
          }
      }
    }

    "should give cluster user-specified scopes" in { billingProject =>
      implicit val ronToken: AuthToken = ronAuthToken

      withNewCluster(
        billingProject,
        request = defaultClusterRequest.copy(scopes = Set(
                    "https://www.googleapis.com/auth/userinfo.email",
                    "https://www.googleapis.com/auth/userinfo.profile",
                    "https://www.googleapis.com/auth/source.read_only"
                  ))
      ) { cluster =>
        withWebDriver { implicit driver =>
          withNewNotebook(cluster) { notebookPage =>
            val query =
              """! bq query --disable_ssl_validation --format=json "SELECT COUNT(*) AS scullion_count FROM publicdata.samples.shakespeare WHERE word='scullion'" """

            // Result should fail due to insufficient scopes.
            // Note we used to check for 'Invalid credential' in the result but the error message from
            // Google does not seem stable.
            val result = notebookPage.executeCell(query, timeout = 5.minutes).get
            result should include("BigQuery error in query operation")
            result should not include "scullion_count"
          }
        }
      }
    }

    "should populate user-specified environment variables" in { billingProject =>
      implicit val ronToken: AuthToken = ronAuthToken

      // Note: the R image includes R and python 3 kernels
      val clusterRequest = defaultClusterRequest.copy(customClusterEnvironmentVariables = Map("KEY" -> "value"))

      withNewCluster(billingProject, request = clusterRequest) { cluster =>
        withWebDriver { implicit driver =>
          withNewNotebook(cluster, Python3) { notebookPage =>
            notebookPage.executeCell("import os")
            val envVar = notebookPage.executeCell("os.getenv('KEY')")
            envVar shouldBe Some("'value'")
          }
        }
      }
    }

    "should execute user-specified start script" in { billingProject =>
      implicit val ronToken: AuthToken = ronAuthToken

      withNewGoogleBucket(billingProject) { bucketName =>
        val ronPetServiceAccount = Sam.user.petServiceAccountEmail(billingProject.value)(ronAuthToken)
        googleStorageDAO.setBucketAccessControl(bucketName,
                                                EmailGcsEntity(GcsEntityTypes.User, ronPetServiceAccount),
                                                GcsRoles.Owner)

        // Add the user script to the bucket. This script increments and writes a count to file,
        // tracking the number of times it has been invoked.
        val startScriptString = "#!/usr/bin/env bash\n\n" +
          "count=$(cat $JUPYTER_HOME/leo_test_start_count.txt || echo 0)\n" +
          "echo $(($count + 1)) > $JUPYTER_HOME/leo_test_start_count.txt"
        val startScriptObjectName = GcsObjectName("start-script.sh")
        val startScriptUri = s"gs://${bucketName.value}/${startScriptObjectName.value}"

        withNewBucketObject(bucketName, startScriptObjectName, startScriptString, "text/plain") { objectName =>
          googleStorageDAO.setObjectAccessControl(bucketName,
                                                  objectName,
                                                  EmailGcsEntity(GcsEntityTypes.User, ronPetServiceAccount),
                                                  GcsRoles.Owner)

          withNewCluster(billingProject,
                         request = defaultClusterRequest.copy(jupyterStartUserScriptUri = Some(startScriptUri))) {
            cluster =>
              withWebDriver { implicit driver =>
                withNewNotebook(cluster, Python3) { notebookPage =>
                  notebookPage.executeCell("!cat $JUPYTER_HOME/leo_test_start_count.txt").get shouldBe "1"
                }

                // Stop the cluster
                stopAndMonitor(cluster.googleProject, cluster.clusterName)

                // Start the cluster
                startAndMonitor(cluster.googleProject, cluster.clusterName)

                withNewNotebook(cluster, Python3) { notebookPage =>
                  notebookPage.executeCell("!cat $JUPYTER_HOME/leo_test_start_count.txt").get shouldBe "2"
                }
              }
          }

        }
      }
    }

    "should recover from out-of-memory errors" in { billingProject =>
      implicit val ronToken: AuthToken = ronAuthToken

      // Create a cluster with smaller memory size
      withNewCluster(
        billingProject,
        request = defaultClusterRequest.copy(
          machineConfig = Some(
            MachineConfig(masterMachineType = Some("n1-standard-2"))
          ),
          toolDockerImage = Some(LeonardoConfig.Leonardo.pythonImageUrl)
        )
      ) { cluster =>
        withWebDriver { implicit driver =>
          withNewNotebook(cluster) { notebookPage =>
            // try to allocate 6G of RAM, which should not be possible for this machine type
            val cell =
              """import numpy
                |result = [numpy.random.bytes(1024*1024) for x in range(6*1024)]
                |print(len(result))
                |""".stripMargin
            notebookPage.addCodeAndExecute(cell, 5.minutes)

            // Kernel should restart automatically and still be functional
            notebookPage.dismissKernelDied()
            notebookPage.executeCell("print('Still alive!')", cellNumberOpt = Some(1)).get shouldBe "Still alive!"
          }
        }
      }
    }
  }
}
