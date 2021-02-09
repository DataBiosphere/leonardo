package org.broadinstitute.dsde.workbench.leonardo.rstudio

import org.broadinstitute.dsde.workbench.leonardo._
import org.openqa.selenium.Keys
import org.scalatest.DoNotDiscover

@DoNotDiscover
class RStudioSpec extends RuntimeFixtureSpec with RStudioTestUtils {

  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.rstudioBioconductorImage.imageUrl)

  "RStudioSpec" - {

    "should launch RStudio" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewRStudio(runtimeFixture.runtime) { rstudioPage =>
          rstudioPage.pressKeys("varA <- 1000")
          rstudioPage.pressKeys(Keys.ENTER.toString)
          await visible cssSelector("[title~='varA']")
          rstudioPage.variableExists("varA") shouldBe true
          rstudioPage.variableExists("1000") shouldBe true
        }
      }
    }

    "should launch an RShiny app" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewRStudio(runtimeFixture.runtime) { rstudioPage =>
          rstudioPage.withRShinyExample("01_hello")(rshinyPage =>
            rshinyPage.hasHeaderText("Hello Shiny!") shouldBe true
          )
        }
      }
    }
  }
}
