package org.broadinstitute.dsde.workbench.leonardo.notebooks

import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.runtimes.RuntimeGceSpecDependencies
import org.broadinstitute.dsde.workbench.leonardo.{LeonardoApiClient, LeonardoConfig, RuntimeFixtureSpec}
import org.scalatest.DoNotDiscover

import scala.concurrent.duration._

/**
 * This spec verifies notebook functionality specifically around the R kernel.
 */
@DoNotDiscover
class NotebookRKernelSpec extends RuntimeFixtureSpec with NotebookTestUtils {
  implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()

  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.rImageUrl)

  "NotebookRKernelSpec" - {

    // See https://github.com/DataBiosphere/leonardo/issues/398
    "should use UTF-8 encoding" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
          // Check the locale is set to en_US.UTF-8
          notebookPage.executeCell("""Sys.getenv("LC_ALL")""") shouldBe Some("'en_US.UTF-8'")

          // Make sure unicode characters display correctly
          //          notebookPage.executeCell("""install.packages("skimr")""", timeout = 5.minutes)
          //          notebookPage.executeCell("library(skimr)")
          //
          //          val output = notebookPage.executeCell("""data(iris)
          //                                                  |skim(iris)""".stripMargin)
          //
          //          output shouldBe defined
          //          output.get should not include ("<U+")
          //          output.get should include("▂▇▅▇▆▅▂▂") TODO: re-enable this once we understand why `Variable type: numeric` doesn't show any data the same way https://github.com/ropensci/skimr does
        }
      }
    }

    // TODO: temporarily ignored. This was failing because we install SparkR based on Spark 2.2.3, but
    // Dataproc is giving us Spark 2.2.1. However this chart indicates that we should be getting Spark 2.2.3:
    // https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-1.2.
    // Opening a Google ticket and temporarily ignoring this test.
    "should create a notebook with a working R kernel and import installed packages" ignore { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
          notebookPage.executeCell("library(SparkR)").get should include("SparkR")
          notebookPage.executeCell("sparkR.session()")
          notebookPage.executeCell("df <- as.DataFrame(faithful)")
          notebookPage.executeCell("head(df)").get should include("3.600 79")

          val sparkJob =
            """samples <- 200
              |inside <- function(index) {
              |  set.seed(index)
              |  rand <- runif(2, 0.0, 1.0)
              |  sum(rand^2) < 1
              |}
              |res <- spark.lapply(c(1:samples), inside)
              |pi <- length(which(unlist(res)))*4.0/samples
              |cat("Pi is roughly", pi, "\n")""".stripMargin

          notebookPage.executeCell(sparkJob).get should include("Pi is roughly ")
        }
      }
    }

    // TODO: [discuss] "should be able to install mlr"
    //  Seems like testing a container functionality here. Also, I'm not super clear on even what user journey this test is supporting
    //  Am I misunderstanding something or is this testing the r kernel in a notebook?
  }
}
