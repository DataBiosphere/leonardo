package org.broadinstitute.dsde.workbench.leonardo.notebooks

import java.io.File

import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.dao.Google.googleStorageDAO
import org.broadinstitute.dsde.workbench.leonardo.{GPAllocFixtureSpec, LeonardoApiClient, UserScriptPath}
import org.broadinstitute.dsde.workbench.model.google.{EmailGcsEntity, GcsEntityTypes, GcsObjectName, GcsPath, GcsRoles}
import org.broadinstitute.dsde.workbench.service.Sam
import org.http4s.AuthScheme
import org.http4s.headers.Authorization
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

import scala.concurrent.duration._

/**
 * This spec verfies different cluster creation options, such as user scripts, extensions, etc.
 */
@DoNotDiscover
final class NotebookGCECustomizationSpec extends GPAllocFixtureSpec with ParallelTestExecution with NotebookTestUtils {
  implicit val ronToken: AuthToken = ronAuthToken
  implicit val auth: Authorization = Authorization(
    org.http4s.Credentials.Token(AuthScheme.Bearer, ronCreds.makeAuthToken().value)
  )

  "NotebookGCECustomizationSpec" - {

    "should run a user script" in { billingProject =>
      // Create a new bucket
      withNewGoogleBucket(billingProject) { bucketName =>
        val ronPetServiceAccount = Sam.user.petServiceAccountEmail(billingProject.value)(ronAuthToken)
        googleStorageDAO.setBucketAccessControl(bucketName,
                                                EmailGcsEntity(GcsEntityTypes.User, ronPetServiceAccount),
                                                GcsRoles.Owner)

        // Add the user script to the bucket
        val userScriptString = "#!/usr/bin/env bash\n\npip3 install mock"
        val userScriptObjectName = GcsObjectName("user-script.sh")
        val userScriptUri = UserScriptPath.Gcs(GcsPath(bucketName, userScriptObjectName))

        withNewBucketObject(bucketName, userScriptObjectName, userScriptString, "text/plain") { objectName =>
          googleStorageDAO.setObjectAccessControl(bucketName,
                                                  objectName,
                                                  EmailGcsEntity(GcsEntityTypes.User, ronPetServiceAccount),
                                                  GcsRoles.Owner)

          // Create a new cluster using the URI of the user script
          val clusterRequestWithUserScript =
            LeonardoApiClient.defaultCreateRuntime2Request.copy(userScriptUri = Some(userScriptUri))
          withNewRuntime(billingProject, request = clusterRequestWithUserScript) { cluster =>
            Thread.sleep(10000)
            withWebDriver { implicit driver =>
              // Create a notebook that will check if the user script ran
              withNewNotebook(cluster, Python3) { notebookPage =>
                notebookPage.executeCell("""print("Hello Notebook!")""") shouldBe Some("Hello Notebook!")
                notebookPage.executeCell("""import mock""") shouldBe None
              }
            }
          }
        }
      }
    }

    // Using nbtranslate extension from here:
    // https://github.com/ipython-contrib/jupyter_contrib_nbextensions/tree/master/src/jupyter_contrib_nbextensions/nbextensions/nbTranslate
    "should install user specified notebook extensions" in { billingProject =>
      val translateExtensionFile = ResourceFile("bucket-tests/translate_nbextension.tar.gz")
      withResourceFileInBucket(billingProject, translateExtensionFile, "application/x-gzip") {
        translateExtensionBucketPath =>
          val extensionConfig = multiExtensionClusterRequest.copy(
            nbExtensions =
              multiExtensionClusterRequest.nbExtensions + ("translate" -> translateExtensionBucketPath.toUri)
          )
          withNewRuntime(
            billingProject,
            request =
              LeonardoApiClient.defaultCreateRuntime2Request.copy(userJupyterExtensionConfig = Some(extensionConfig))
          ) { runtime =>
            withWebDriver { implicit driver =>
              withNewNotebook(runtime, Python3) { notebookPage =>
                // Check the extensions were installed
                val nbExt = notebookPage.executeCell("! jupyter nbextension list")

                nbExt.get should include("jupyter-iframe-extension/main  enabled")

                nbExt.get should include("translate_nbextension/main  enabled")
                // should be installed by default
                nbExt.get should include("toc2/main  enabled")

                val serverExt = notebookPage.executeCell("! jupyter serverextension list")
                serverExt.get should include("jupyterlab  enabled")
                // should be installed by default
                serverExt.get should include("jupyter_nbextensions_configurator  enabled")

                // Exercise the translate extensionfailure_screenshots/NotebookGCECustomizationSpec_18-34-37-017.png
                notebookPage.translateMarkup("Yes") should include("Oui")
              }
            }
          }
      }
    }

    "should give cluster user-specified scopes" in { billingProject =>
      withNewRuntime(
        billingProject,
        request = LeonardoApiClient.defaultCreateRuntime2Request.copy(
          scopes = Set(
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/userinfo.profile",
            "https://www.googleapis.com/auth/source.read_only",
            "https://www.googleapis.com/auth/devstorage.full_control"
          )
        )
      ) { cluster =>
        withWebDriver { implicit driver =>
          //With Scopes
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
      val runtimeRequest =
        LeonardoApiClient.defaultCreateRuntime2Request.copy(customEnvironmentVariables = Map("KEY" -> "value"))

      withNewRuntime(billingProject, request = runtimeRequest) { cluster =>
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
          "echo $(($count + 1)) > $JUPYTER_HOME/leo_test_start_count.txt\n"

        val startScriptObjectName = GcsObjectName("start-script.sh")
        val startScriptUri = UserScriptPath.Gcs(GcsPath(bucketName, startScriptObjectName))

        withNewBucketObject(bucketName, startScriptObjectName, startScriptString, "text/plain") { objectName =>
          googleStorageDAO.setObjectAccessControl(bucketName,
                                                  objectName,
                                                  EmailGcsEntity(GcsEntityTypes.User, ronPetServiceAccount),
                                                  GcsRoles.Owner)

          withNewRuntime(
            billingProject,
            request = LeonardoApiClient.defaultCreateRuntime2Request.copy(startUserScriptUri = Some(startScriptUri))
          ) { runtime =>
            withWebDriver { implicit driver =>
              withNewNotebook(runtime, Python3) { notebookPage =>
                notebookPage.executeCell("!cat $JUPYTER_HOME/leo_test_start_count.txt").get shouldBe "1"
              }
            }

            // Stop the cluster
            stopAndMonitorRuntime(runtime.googleProject, runtime.clusterName)

            // Start the cluster
            startAndMonitorRuntime(runtime.googleProject, runtime.clusterName)

            val notebookPath = new File("Untitled.ipynb")
            withWebDriver { implicit driver =>
              // Use a longer timeout than default because opening notebooks after resume can be slow
              withOpenNotebook(runtime, notebookPath, 10.minutes) { notebookPage =>
                // execute a new cell to make sure the startup script worked
                notebookPage.runAllCells()
                notebookPage.executeCell("!cat $JUPYTER_HOME/leo_test_start_count.txt").get shouldBe "2"
              }
            }
          }
        }
      }
    }

//    // TODO: This test has flaky selenium logic, ignoring for now. More details in:
//    // https://broadworkbench.atlassian.net/browse/QA-1027
//    "should recover from out-of-memory errors" ignore { billingProject =>
//      implicit val ronToken: AuthToken = ronAuthToken
//
//      // Create a cluster with smaller memory size
//      withNewCluster(
//        billingProject,
//        request = defaultClusterRequest.copy(
//          machineConfig = Some(
//            RuntimeConfigRequestCopy.DataprocConfig(
//              cloudService = CloudService.Dataproc.asString,
//              numberOfWorkers = Some(0),
//              masterMachineType = Some("n1-standard-2"),
//              masterDiskSize = Some(500),
//              None,
//              None,
//              None,
//              None,
//              Map.empty
//            )
//          ),
//          toolDockerImage = Some(LeonardoConfig.Leonardo.pythonImageUrl)
//        )
//      ) { cluster =>
//        withWebDriver { implicit driver =>
//          withNewNotebook(cluster) { notebookPage =>
//            // try to allocate 6G of RAM, which should not be possible for this machine type
//            val cell =
//              """import numpy
//                |result = [numpy.random.bytes(1024*1024) for x in range(6*1024)]
//                |print(len(result))
//                |""".stripMargin
//            notebookPage.addCodeAndExecute(cell, wait = false, timeout = 5.minutes)
//            // Kernel should restart automatically and still be functional
//            notebookPage.validateKernelDiedAndDismiss()
//            notebookPage
//              .executeCell("print('Still alive!')", timeout = 2.minutes, cellNumberOpt = Some(1))
//              .get shouldBe "Still alive!"
//          }
//        }
//      }
//    }
  }
}
