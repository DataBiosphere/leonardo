package org.broadinstitute.dsde.workbench.leonardo.notebooks

import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.{LeonardoConfig, RuntimeFixtureSpec}
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

    // See https://github.com/DataBiosphere/leonardo/issues/398
    "should be able to install mlr" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
          // mlr: machine learning in R
          // https://github.com/mlr-org/mlr

          // it may take a little while to install
          val installTimeout = 5.minutes

          val installOutput = notebookPage.executeCell("""install.packages('mlr')""", installTimeout)
          installOutput shouldBe defined
          installOutput.get should include("Installing package into")
          installOutput.get should include("/home/jupyter/packages")
          installOutput.get should not include "Installation failed"

          // Make sure it was installed correctly; if not, this will return an error
          notebookPage.executeCell("library(mlr)").get should include("Loading required package: ParamHelpers")
          notebookPage.executeCell(""""mlr" %in% installed.packages()""") shouldBe Some("TRUE")
        }
      }
    }

    "should have tidyverse automatically installed" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
          notebookPage.executeCell(""""tidyverse" %in% installed.packages()""") shouldBe Some("TRUE")
          notebookPage.executeCell("find.package('tidyverse')").get should include("/usr/local/lib/R/site-library")
        }
      }
    }

    "should have Ronaldo automatically installed" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
          notebookPage.executeCell(""""Ronaldo" %in% installed.packages()""") shouldBe Some("TRUE")
        }
      }
    }

    // See https://github.com/DataBiosphere/leonardo/issues/710
    "should be able to install packages that depend on gfortran" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
          val installTimeout = 5.minutes

          val installOutput = notebookPage.executeCell("""install.packages('qwraps2')""", installTimeout)
          installOutput shouldBe defined
          installOutput.get should include("Installing package into")
          installOutput.get should include("/home/jupyter/packages")
          installOutput.get should not include "cannot find -lgfortran"
        }
      }
    }

    s"should have the workspace-related environment variables set" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        val expectedEVs =
          RuntimeFixtureSpec.getCustomEnvironmentVariables ++
            // variables implicitly set by Leo
            Map(
              "CLUSTER_NAME" -> runtimeFixture.runtime.clusterName.asString,
              "RUNTIME_NAME" -> runtimeFixture.runtime.clusterName.asString,
              "OWNER_EMAIL" -> runtimeFixture.runtime.creator.value,
              // TODO: remove when PPW is rolled out to all workspaces
              // and Leo removes the kernel_bootstrap logic.
              // See https://broadworkbench.atlassian.net/browse/IA-2936
              "WORKSPACE_NAME" -> "home"
            )
        withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
          expectedEVs.foreach { case (k, v) =>
            val res = notebookPage.executeCell(s"Sys.getenv('$k')")
            res shouldBe defined
            res.get shouldBe s"'$v'"
          }
        }
      }
    }

    "should have Java available" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
          val javaOutput = notebookPage.executeCell("""system('java --version', intern = TRUE)""")
          javaOutput shouldBe defined
          javaOutput.get should include("OpenJDK Runtime Environment")
          javaOutput.get should not include "not found"
        }
      }
    }
  }
}
