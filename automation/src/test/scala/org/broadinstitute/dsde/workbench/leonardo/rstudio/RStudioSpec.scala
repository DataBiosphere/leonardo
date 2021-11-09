package org.broadinstitute.dsde.workbench.leonardo.rstudio

import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo._
import org.openqa.selenium.Keys
import org.scalatest.DoNotDiscover

@DoNotDiscover
class RStudioSpec extends RuntimeFixtureSpec with RStudioTestUtils {
  implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()

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

    "environment variables should be available in RStudio" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewRStudio(runtimeFixture.runtime) { rstudioPage =>
          val expectedEVs =
            RuntimeFixtureSpec.getCustomEnvironmentVariables ++
              // variables implicitly set by Leo
              Map(
                "CLUSTER_NAME" -> runtimeFixture.runtime.clusterName.asString,
                "RUNTIME_NAME" -> runtimeFixture.runtime.clusterName.asString,
                "OWNER_EMAIL" -> runtimeFixture.runtime.creator.value
              )

          expectedEVs.foreach {
            case (k, v) =>
              rstudioPage.pressKeys(s"""var_$k <- Sys.getenv("$k")""")
              rstudioPage.pressKeys(Keys.ENTER.toString)
              Thread.sleep(2000)
              await visible cssSelector(s"[title~='var_$k']")
              rstudioPage.variableExists(s"var_$k") shouldBe true
              rstudioPage.variableExists(s""""$v"""") shouldBe true
          }
        }
      }
    }

    // Note this test should be last because the test infrastructure does not close the shiny app
    "should launch an RShiny app" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewRStudio(runtimeFixture.runtime) { rstudioPage =>
          rstudioPage.pressKeys(Keys.ENTER.toString)
          rstudioPage.withRShinyExample("01_hello")(rshinyPage =>
            rshinyPage.getExampleHeader shouldBe Some("Hello Shiny!")
          )
        }
      }
    }
  }
}
