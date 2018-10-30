package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.dao.Google.googleStorageDAO
import org.broadinstitute.dsde.workbench.leonardo.Leonardo.ApiVersion.V2
import org.broadinstitute.dsde.workbench.model.google.{EmailGcsEntity, GcsEntityTypes, GcsObjectName, GcsRoles, GoogleProject}
import org.broadinstitute.dsde.workbench.service.Sam
import org.broadinstitute.dsde.workbench.service.util.Tags

import scala.language.postfixOps

final class NotebookUserScriptSpec extends ClusterFixtureSpec {
  // create a new bucket
  // add the user script to the bucket
  // create a new cluster using the URI of the user script
  // create a notebook that will check if the user script ran
  "Leonardo should allow importing a package that requires a user script that IS installed" taggedAs Tags.SmokeTest in {
    clusterFixture =>
      withProject { project => implicit token =>
        withNewGoogleBucket(project) { bucketName =>
          val ronPetServiceAccount = Sam.user.petServiceAccountEmail(clusterFixture.billingProject.value)(ronAuthToken)
          googleStorageDAO.setBucketAccessControl(bucketName, EmailGcsEntity(GcsEntityTypes.User, ronPetServiceAccount), GcsRoles.Owner)

          val userScriptString = "#!/usr/bin/env bash\n\npip2 install arrow"
          val userScriptObjectName = GcsObjectName("user-script.sh")
          val userScriptUri = s"gs://${bucketName.value}/${userScriptObjectName.value}"

          withNewBucketObject(bucketName, userScriptObjectName, userScriptString, "text/plain") { objectName =>
            googleStorageDAO.setObjectAccessControl(bucketName, objectName, EmailGcsEntity(GcsEntityTypes.User, ronPetServiceAccount), GcsRoles.Owner)

            withWebDriver { implicit driver =>
              withNewCluster(clusterFixture.billingProject, request = ClusterRequest(Map(), None, Option(userScriptUri)), apiVersion = V2) { cluster =>
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
    }
}
