package org.broadinstitute.dsde.workbench.leonardo.lab

import org.broadinstitute.dsde.workbench.leonardo.cluster.ClusterFixtureSpec
import org.broadinstitute.dsde.workbench.service.util.Tags

class LabSpec extends ClusterFixtureSpec {

  "Leonardo lab" - {

    "should execute cells" taggedAs Tags.SmokeTest in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewLabNotebook(clusterFixture.cluster) { labNotebookPage =>
          labNotebookPage.executeCell("1+1") shouldBe Some("2")
          labNotebookPage.executeCell("2*3") shouldBe Some("6")
          labNotebookPage.executeCell("""print 'Hello Notebook!'""") shouldBe Some("Hello Notebook!")
        }
      }
    }
  }
}
