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
            rshinyPage.getExampleHeader shouldBe Some("Hello Shiny!")
          )
        }
      }
    }

    "environment variables should be available in RStudio" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewRStudio(runtimeFixture.runtime) { rstudioPage =>
          val expectedEVs = Map(
            "GOOGLE_PROJECT" -> runtimeFixture.runtime.googleProject.value,
            "WORKSPACE_NAMESPACE" -> runtimeFixture.runtime.googleProject.value,
            "CLUSTER_NAME" -> runtimeFixture.runtime.clusterName.asString,
            "RUNTIME_NAME" -> runtimeFixture.runtime.clusterName.asString,
            "OWNER_EMAIL" -> runtimeFixture.runtime.creator.value,
            "WORKSPACE_NAME" -> "TestWorkspace",
            "WORKSPACE_BUCKET" -> "gs://test-workspace-bucket"
          )

          expectedEVs.foreach {
            case (k, v) =>
              rstudioPage.pressKeys(s"""var_$k <- System.getenv("$k")""")
              rstudioPage.pressKeys(Keys.ENTER.toString)
              await visible cssSelector(s"[title~='var_$k']")
              rstudioPage.variableExists(s"var_$k") shouldBe true
              rstudioPage.variableExists(s"$v") shouldBe true
          }
        }
      }
    }
  }
}
