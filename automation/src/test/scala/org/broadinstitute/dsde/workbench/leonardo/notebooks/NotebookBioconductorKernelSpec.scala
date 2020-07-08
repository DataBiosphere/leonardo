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
    "should create a notebook with a working R kernel and install package rsbml, RCurl" ignore { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
//          notebookPage.executeCell("""BiocManager::version() == "3.10"""")

          // Make sure unicode characters display correctly
          notebookPage.executeCell("""BiocManager::install("rsbml")""")
          notebookPage.executeCell("library(rsbml)")

          notebookPage.executeCell("""BiocManager::install("RCurl")""")
          notebookPage.executeCell("library(RCurl)")
        }
      }
    }

    "should be able to call installed Bioconductor libraries" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
          // it shouldn't take long to load libraries
          val callLibraryTimeout = 1.minutes

          notebookPage.executeCell("""library("SingleCellExperiment")""", callLibraryTimeout)
          notebookPage.executeCell("""library("DESeq2")""", callLibraryTimeout)
          notebookPage.executeCell("""library("ShortRead")""", callLibraryTimeout)
          notebookPage.executeCell("""library("GenomicAlignments")""", callLibraryTimeout)
          notebookPage.executeCell("""library("GenomicFeatures")""", callLibraryTimeout)

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

    "should be able to install packages that depend on libXML, graphviz" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
          val installTimeout = 5.minutes

          notebookPage.executeCell("""BiocManager::install('XML')""", installTimeout)
          notebookPage.executeCell("library(XML)")

          notebookPage.executeCell("""BiocManager::install('Rgraphviz')""", installTimeout)
          notebookPage.executeCell("library('Rgraphviz')")
        }
      }
    }

    "should be able to install packages that depend on scikit-learn, hdf5, openbabel, gsl, magick++, database package" in {
      runtimeFixture =>
        withWebDriver { implicit driver =>
          withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
            val installTimeout = 20 minutes

            notebookPage.executeCell("""BiocManager::install('BiocSklearn')""", installTimeout)
            notebookPage.executeCell("library('BiocSklearn')")

            notebookPage.executeCell("""BiocManager::install('rhdf5')""", installTimeout)
            notebookPage.executeCell("library(rhdf5)")

            notebookPage.executeCell("""BiocManager::install('ChemmineOB')""", installTimeout)
            notebookPage.executeCell("library(ChemmineOB)")

            notebookPage.executeCell("""BiocManager::install('EBImage')""", installTimeout)
            notebookPage.executeCell("library(EBImage)")

            notebookPage.executeCell("""BiocManager::install('RMySQL')""", installTimeout)
            notebookPage.executeCell("library(RMySQL)")

            notebookPage.executeCell("""BiocManager::install('DirichletMultinomial')""", installTimeout)
            notebookPage.executeCell("library(DirichletMultinomial)")
          }
        }
    }

    "should be able to install packages that depend on jags, protobuf, Cairo and gtk" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
          val installTimeout = 15 minutes

          notebookPage.executeCell("""BiocManager::install('rjags')""", installTimeout)
          notebookPage.executeCell("library('rjags')")

          notebookPage.executeCell("""BiocManager::install('protolite')""", installTimeout)
          notebookPage.executeCell("library(protolite)")

          notebookPage.executeCell("""BiocManager::install('RGtk2')""", installTimeout)
          notebookPage.executeCell("library(RGtk2)")
          // new packages should install to directory where PD is mounted
          notebookPage.executeCell("find.package('RGtk2')").get should include("/home/jupyter-user/notebooks/packages")
        }
      }
    }

    "should have Java available" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
          val javaOutput = notebookPage.executeCell("""system('java --version', intern = TRUE)""")
          javaOutput shouldBe 'defined
          javaOutput.get should include("OpenJDK Runtime Environment")
          javaOutput.get should not include ("not found")
        }
      }
    }
  }
}
