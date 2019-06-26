package org.broadinstitute.dsde.workbench.leonardo.notebooks


import org.scalatest.{FreeSpec, ParallelTestExecution}
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures

import scala.language.postfixOps


class NotebookExtensionSpec extends ClusterFixtureSpec with NotebookTestUtils with ParallelTestExecution with BillingFixtures {
  override def enableWelder: Boolean = true

  "Leonardo welder and notebooks" - {

    "Welder should be up" in { clusterFixture =>
      Welder.getWelderStatus(clusterFixture.cluster)
    }

    "storageLinks and localize should work" in { clusterFixture =>
            withWebDriver { implicit driver =>
        val sampleNotebook = ResourceFile("bucket-tests/gcsFile.ipynb")
        withResourceFileInBucket(clusterFixture.billingProject, sampleNotebook, "text/plain") { googleCloudDir =>

          println("====================================================")
          println("in welder test code")
          println(googleCloudDir)
          //                //storageLinks setup
          //                //localize call
          //                //withOpenNotebook(cluster, localizedPath, 10.minutes) {
        }

//                withWelderInit(gcsFileToLocalize) { localizedPath

        }
      }
  }
}
