package org.broadinstitute.dsde.workbench.leonardo.rstudio

import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.notebooks.NotebookTestUtils
import org.scalatest.DoNotDiscover

import scala.util.Try

@DoNotDiscover
class RStudioSpec extends RuntimeFixtureSpec with NotebookTestUtils {

  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.rstudioBioconductorImaeg.imageUrl)

  "RStudioSpec" - {

    "should launch RStudio" in { runtimeFixture =>
      withWebDriver { _ =>
        // See this ticket for adding more comprehensive selenium tests for RStudio:
        // https://broadworkbench.atlassian.net/browse/IA-697
        val getResult = Try(RStudio.getApi(runtimeFixture.runtime.googleProject, runtimeFixture.runtime.clusterName))
        getResult.isSuccess shouldBe true
        getResult.get should include("unsupported_browser")
        getResult.get should not include "ProxyException"
      }
    }
  }
}
