package org.broadinstitute.dsde.workbench.leonardo.rstudio

import org.broadinstitute.dsde.workbench.leonardo._
import org.openqa.selenium.{Keys}
import org.scalatest.DoNotDiscover
@DoNotDiscover
class RStudioSpec extends RuntimeFixtureSpec with RStudioTestUtils {

  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.rstudioBioconductorImage.imageUrl)

  "RStudioSpec" - {

    "should launch RStudio" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewRStudio(runtimeFixture.runtime) { rstudioPage =>
//          // See this ticket for adding more comprehensive selenium tests for RStudio:
//          // https://broadworkbench.atlassian.net/browse/IA-697
          Thread.sleep(10000)
          rstudioPage.pressKeys("varA <- 1000")
          rstudioPage.pressKeys(Keys.ENTER.toString)
          Thread.sleep(10000)
          rstudioPage.variableExists("varA") shouldBe true
        }
      }
    }
  }
}
