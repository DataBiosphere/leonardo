package org.broadinstitute.dsde.workbench.leonardo.lab

import org.broadinstitute.dsde.workbench.leonardo.ClusterFixtureSpec
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.FreeSpec
import org.broadinstitute.dsde.workbench.leonardo._


import scala.util.Try



class LabSpec extends ClusterFixtureSpec with LabTestUtils {

  "Leonardo lab" - {

    "should execute cells" taggedAs Tags.SmokeTest ignore { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewLabNotebook(clusterFixture.cluster) { labNotebookPage =>
          labNotebookPage.runCodeInEmptyCell("1+1") shouldBe Some("2")
          labNotebookPage.runCodeInEmptyCell("2*3") shouldBe Some("6")
          labNotebookPage.runCodeInEmptyCell("""print 'Hello Notebook!'""") shouldBe Some("Hello Notebook!")
        }
      }
    }
  }
}
