package org.broadinstitute.dsde.workbench.leonardo.notebooks

import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.dao.Google.googleStorageDAO
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.model.google.{EmailGcsEntity, GcsEntityTypes, GcsObjectName, GcsRoles, GoogleProject}
import org.broadinstitute.dsde.workbench.service.Sam
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.{DoNotDiscover, FreeSpec, ParallelTestExecution}

import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * This spec verfies different cluster creation options, such as user scripts, extensions, etc.
  */
@DoNotDiscover
final class NotebookCustomizationSpec(val billingProject: GoogleProject) extends FreeSpec with ParallelTestExecution with NotebookTestUtils with BillingFixtures {

  "NotebookCustomizationSpec" - {

    "should run a user script" taggedAs Tags.SmokeTest in {
      implicit val ronToken: AuthToken = ronAuthToken

      // Create a new bucket
      withNewGoogleBucket(billingProject) { bucketName =>
        val ronPetServiceAccount = Sam.user.petServiceAccountEmail(billingProject.value)(ronAuthToken)
        googleStorageDAO.setBucketAccessControl(
          bucketName,
          EmailGcsEntity(GcsEntityTypes.User, ronPetServiceAccount),
          GcsRoles.Owner)

        // Add the user script to the bucket
        val userScriptString = "#!/usr/bin/env bash\n\npip2 install arrow"
        val userScriptObjectName = GcsObjectName("user-script.sh")
        val userScriptUri = s"gs://${bucketName.value}/${userScriptObjectName.value}"

        withNewBucketObject(bucketName, userScriptObjectName, userScriptString, "text/plain") { objectName =>
          googleStorageDAO.setObjectAccessControl(
            bucketName,
            objectName,
            EmailGcsEntity(GcsEntityTypes.User, ronPetServiceAccount),
            GcsRoles.Owner)

          // Create a new cluster using the URI of the user script
          val clusterRequestWithUserScript = defaultClusterRequest.copy(Map(), None, Option(userScriptUri))
          withNewCluster(billingProject, request = clusterRequestWithUserScript) { cluster =>
            Thread.sleep(10000)
            withWebDriver { implicit driver =>
              // Create a notebook that will check if the user script ran
              withNewNotebook(cluster) { notebookPage =>
                notebookPage.executeCell("""print('Hello Notebook!')""") shouldBe Some("Hello Notebook!")
                notebookPage.executeCell("""import arrow""")
                notebookPage.executeCell("""arrow.get(727070400)""") shouldBe Some("<Arrow [1993-01-15T04:00:00+00:00]>")
              }
            }
          }(ronAuthToken)
        }
      }
    }

    // Using nbtranslate extension from here:
    // https://github.com/ipython-contrib/jupyter_contrib_nbextensions/tree/master/src/jupyter_contrib_nbextensions/nbextensions/nbTranslate
    "should install user specified notebook extensions" in {
      implicit val ronToken: AuthToken = ronAuthToken

      val translateExtensionFile = ResourceFile("bucket-tests/translate_nbextension.tar.gz")
      withResourceFileInBucket(billingProject, translateExtensionFile, "application/x-gzip") { translateExtensionBucketPath =>
        val extensionConfig = multiExtensionClusterRequest.copy(nbExtensions = multiExtensionClusterRequest.nbExtensions + ("translate" -> translateExtensionBucketPath.toUri))
        withNewCluster(billingProject, request = defaultClusterRequest.copy(userJupyterExtensionConfig = Some(extensionConfig))) { cluster =>
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
              notebookPage.translateMarkup("Hello") should include("Bonjour")
            }
          }
        }
      }
    }

    "should give cluster user-specified scopes" in {
      implicit val ronToken: AuthToken = ronAuthToken

      withNewCluster(billingProject, request = defaultClusterRequest.copy(scopes = Set("https://www.googleapis.com/auth/userinfo.email", "https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/source.read_only"))) { cluster =>
        withWebDriver { implicit driver =>
          withNewNotebook(cluster) { notebookPage =>
            val query = """! bq query --disable_ssl_validation --format=json "SELECT COUNT(*) AS scullion_count FROM publicdata.samples.shakespeare WHERE word='scullion'" """

            val result = notebookPage.executeCell(query, timeout = 5.minutes).get
            result should include("BigQuery error in query operation")
            result.replace(System.lineSeparator(), " ") should include("Invalid credential")
          }
        }
      }
    }
  }
}
