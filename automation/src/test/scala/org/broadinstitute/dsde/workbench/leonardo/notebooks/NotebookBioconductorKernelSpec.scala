/**
 *  1. Check if singleCellExp, DESeq2 etc %in% installed.packages()
 *  2. BiocManager::install() for
 *  3. BiocManager::version() == 3.6.1
 *  4. BiocManager::install('rsbml')
 *  5. BiocManager::install('RCurl') ## tests libcurl
 *  6. "XML" ##libxml
 *  7. "graphviz" RGraphviz
 *  8. "BiocSklearn"
 *  9. "rhhdf5"
 *  10. openbabel - ChemmineOB
 *  11. gsl - DirichletMultinomial
 *  12. gtk - RGtk2
 *  13. magick++ EBImage
 *  14. protobuf- protolite
 *  15. databse stuff - RMySQL
 *  16. jags - rjags
 */
package org.broadinstitute.dsde.workbench.leonardo.notebooks

import org.broadinstitute.dsde.workbench.leonardo.{LeonardoConfig, RuntimeFixtureSpec}
import org.scalatest.DoNotDiscover

import scala.concurrent.duration._

/**
 * This spec verifies notebook functionality specifically around the R-Bioconductor kernel.
 */
@DoNotDiscover
class NotebookBioconductorKernelSpec extends RuntimeFixtureSpec with NotebookTestUtils {
  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.bioconductorImageUrl)
  "NotebookBioconductorKernelSpec" - {
    "should be able to call installed Bioconductor libraries" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
          // it shouldn't take long to load libraries
          val callLibraryTimeout = 1.minutes
          // bioconductor libraries should install to appropriate directory
          notebookPage.executeCell("find.package('ShortRead')").get should include("/usr/local/lib/R/site-library")
        }
      }
    }

    "should have GenomicFeatures, SingleCellExperiment, GenomicAlignments, ShortRead automatically installed" in {
      runtimeFixture =>
        withWebDriver { implicit driver =>
          withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
            notebookPage.executeCell(""""GenomicFeatures" %in% installed.packages()""") shouldBe Some("TRUE")
            notebookPage.executeCell(""""SingleCellExperiment" %in% installed.packages()""") shouldBe Some("TRUE")
            notebookPage.executeCell(""""GenomicAlignments" %in% installed.packages()""") shouldBe Some("TRUE")
            notebookPage.executeCell(""""ShortRead" %in% installed.packages()""") shouldBe Some("TRUE")
          }
        }
    }

    // TODO: this is fixed in the new bioc (R 4.0) image
    "should have DESeq2 automatically installed" ignore { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
          notebookPage.executeCell(""""DESeq2" %in% installed.packages()""") shouldBe Some("TRUE")
        }
      }
    }

    "should be able to install packages that depend on jags, protobuf, Cairo and gtk" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
          val installTimeout = 15 minutes

          // new packages should install to directory where PD is mounted
          notebookPage.executeCell("find.package('RGtk2')").get should include("/home/jupyter-user/notebooks/packages")
        }
      }
    }

    "should have Java available" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
          val javaOutput = notebookPage.executeCell("""system('java --version', intern = TRUE)""")
          javaOutput shouldBe defined
          javaOutput.get should include("OpenJDK Runtime Environment")
          javaOutput.get should not include ("not found")
        }
      }
    }
  }
}
