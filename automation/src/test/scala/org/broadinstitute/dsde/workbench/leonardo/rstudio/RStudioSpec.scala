package org.broadinstitute.dsde.workbench.leonardo.rstudio

import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.notebooks.NotebookTestUtils
import org.scalatest.DoNotDiscover

import scala.util.Try

@DoNotDiscover
class RStudioSpec extends ClusterFixtureSpec with NotebookTestUtils {

  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.rstudioBaseImageUrl)

  "RStudioSpec" - {

    "should launch RStudio" in { clusterFixture =>
      withWebDriver { _ =>
        // See this ticket for adding more comprehensive selenium tests for RStudio:
        // https://broadworkbench.atlassian.net/browse/IA-697
        val getResult = Try(RStudio.getApi(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName))
        getResult.isSuccess shouldBe true
        getResult.get should include("unsupported_browser")
        getResult.get should not include "ProxyException"
      }
    }
  }
}
