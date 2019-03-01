package org.broadinstitute.dsde.workbench.leonardo.notebooks

import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.dao.Google.googleStorageDAO
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo.Leonardo.ApiVersion.V2
import org.broadinstitute.dsde.workbench.leonardo.{ClusterRequest, UserJupyterExtensionConfig}
import org.broadinstitute.dsde.workbench.model.google.{EmailGcsEntity, GcsEntityTypes, GcsObjectName, GcsRoles}
import org.broadinstitute.dsde.workbench.service.Sam
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.{FreeSpec, ParallelTestExecution}

import scala.language.postfixOps

final class NotebookCustomizationSpec extends FreeSpec
  with NotebookTestUtils with ParallelTestExecution with BillingFixtures {

  "Leonardo" - {
    // Note: There is a complementary test in NotebookInstallSpec that verifies that
    // the 'arrow' import does not work without the user script used below.

    // Create a new bucket
    // Add the user script to the bucket
    // Create a new cluster using the URI of the user script
    // Create a notebook that will check if the user script ran
    "should allow importing a package that requires a user script that IS installed" taggedAs Tags.SmokeTest in {
      withProject { project => implicit token =>
        withNewGoogleBucket(project) { bucketName =>
          val ronPetServiceAccount = Sam.user.petServiceAccountEmail(project.value)(ronAuthToken)
          googleStorageDAO.setBucketAccessControl(
            bucketName,
            EmailGcsEntity(GcsEntityTypes.User, ronPetServiceAccount),
            GcsRoles.Owner)

          val userScriptString = "#!/usr/bin/env bash\n\npip2 install arrow"
          val userScriptObjectName = GcsObjectName("user-script.sh")
          val userScriptUri = s"gs://${bucketName.value}/${userScriptObjectName.value}"

          withNewBucketObject(bucketName, userScriptObjectName, userScriptString, "text/plain") { objectName =>
            googleStorageDAO.setObjectAccessControl(
              bucketName,
              objectName,
              EmailGcsEntity(GcsEntityTypes.User, ronPetServiceAccount),
              GcsRoles.Owner)

            val clusterRequestWithUserScript = ClusterRequest(Map(), None, Option(userScriptUri))
            withNewCluster(project, request = clusterRequestWithUserScript, apiVersion = V2) { cluster =>
              Thread.sleep(10000)
              withWebDriver { implicit driver =>
                withNewNotebook(cluster) { notebookPage =>
                  notebookPage.executeCell("""print 'Hello Notebook!'""") shouldBe Some("Hello Notebook!")
                  notebookPage.executeCell("""import arrow""")
                  notebookPage.executeCell("""arrow.get(727070400)""") shouldBe Some("<Arrow [1993-01-15T04:00:00+00:00]>")
                }
              }
            }(ronAuthToken)
          }
        }
      }
    }

    //Test to check if extensions are installed correctly
    //Using nbtranslate extension from here:
    //https://github.com/ipython-contrib/jupyter_contrib_nbextensions/tree/master/src/jupyter_contrib_nbextensions/nbextensions/nbTranslate
    "should install user specified notebook extensions" in {
      withProject { project => implicit token =>
        val translateExtensionFile = ResourceFile("bucket-tests/translate_nbextension.tar.gz")
        withResourceFileInBucket(project, translateExtensionFile, "application/x-gzip") { translateExtensionBucketPath =>
          val clusterRequestWithExtension = ClusterRequest(Map(), Option(translateExtensionBucketPath.toUri), None)
          withNewCluster(project, request = clusterRequestWithExtension) { cluster =>
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
    }

    "should install user specified lab extensions" in {
      withProject { project => implicit token =>
        withNewCluster(project, request = ClusterRequest(userJupyterExtensionConfig = Some(UserJupyterExtensionConfig(labExtensions = Map("jupyterlab-toc" -> "@jupyterlab/toc"))))) { cluster =>
          withWebDriver { implicit driver =>
            withNewNotebook(cluster) { notebookPage =>
              val query = """!jupyter labextension list"""
              val result = notebookPage.executeCell(query).get
              result should include("@jupyterlab/toc")
            }
          }
        }
      }
    }

    "should install user specified lab extensions from a js file" in {
      withProject { project => implicit token =>
        val exampleLabExtensionFile = ResourceFile("bucket-tests/example_lab_extension.js")
        withResourceFileInBucket(project, exampleLabExtensionFile, "text/plain") { exampleLabExtensionBucketPath =>
          val clusterRequestWithLabExtension = ClusterRequest(userJupyterExtensionConfig = Some(UserJupyterExtensionConfig(labExtensions = Map("example_lab_extension" -> exampleLabExtensionBucketPath.toUri))))
          withNewCluster(project, request = clusterRequestWithLabExtension) { cluster =>
            withWebDriver { implicit driver =>
              withNewNotebook(cluster) { notebookPage =>
                notebookPage.executeCell("!jupyter labextension list").get should include("example_lab_extension")
              }
            }
          }
        }
      }
    }
  }
}
