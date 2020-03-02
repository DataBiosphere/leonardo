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

import org.broadinstitute.dsde.workbench.leonardo.{ClusterFixtureSpec, LeonardoConfig}

import org.scalatest.DoNotDiscover

import scala.concurrent.duration._

/**
 * This spec verifies notebook functionality specifically around the R-Bioconductor kernel.
 */
@DoNotDiscover
class NotebookBioconductorKernelSpec extends ClusterFixtureSpec with NotebookTestUtils {
  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.bioconductorImageUrl)
  "NotebookBioconductorKernelSpec" - {

    "should use Bioconductor version 3.10" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          // Make sure BiocManager has the correct version of Bioconductor
          notebookPage.executeCell("""BiocManager::version() == "3.10"""")
        }
      }
    }

    "should create a notebook with a working R kernel and install package rsbml" ignore { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          // Make sure unicode characters display correctly
          notebookPage.executeCell("""BiocManager::install("rsbml")""")
          notebookPage.executeCell("library(rsbml)")

        }
      }
    }

    "should create a notebook with a working R kernel and install package RCurl" ignore { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          // Make sure unicode characters display correctly
          notebookPage.executeCell("""BiocManager::install("RCurl")""")
          notebookPage.executeCell("library(RCurl)")

        }
      }
    }

    "should be able to call installed Bioconductor libraries" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          // it shouldn't take long to load libraries
          val callLibraryTimeout = 1.minutes

          notebookPage.executeCell("""library("SingleCellExperiment")""", callLibraryTimeout)
          notebookPage.executeCell("""library("DESeq2")""", callLibraryTimeout)
          notebookPage.executeCell("""library("ShortRead")""", callLibraryTimeout)
          notebookPage.executeCell("""library("GenomicAlignments")""", callLibraryTimeout)
          notebookPage.executeCell("""library("GenomicFeatures")""", callLibraryTimeout)

        }
      }
    }

    "should have GenomicFeatures automatically installed" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          notebookPage.executeCell(""""GenomicFeatures" %in% installed.packages()""") shouldBe Some("TRUE")
        }
      }
    }

    "should have SingleCellExperiment automatically installed" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          notebookPage.executeCell(""""SingleCellExperiment" %in% installed.packages()""") shouldBe Some("TRUE")
        }
      }
    }

    "should have GenomicAlignments automatically installed" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          notebookPage.executeCell(""""GenomicAlignments" %in% installed.packages()""") shouldBe Some("TRUE")
        }
      }
    }

    "should have ShortRead automatically installed" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          notebookPage.executeCell(""""ShortRead" %in% installed.packages()""") shouldBe Some("TRUE")
        }
      }
    }

    "should have DESeq2 automatically installed" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          notebookPage.executeCell(""""DESeq2" %in% installed.packages()""") shouldBe Some("TRUE")
        }
      }
    }

    "should be able to install packages that depend on libXML" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          val installTimeout = 5.minutes

          val installOutput = notebookPage.executeCell("""BiocManager::install('XML')""", installTimeout)
          notebookPage.executeCell("library(XML)")

        }
      }
    }

    "should be able to install packages that depend on graphviz" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          val installTimeout = 5.minutes

          val installOutput = notebookPage.executeCell("""BiocManager::install('Rgraphviz')""", installTimeout)
          notebookPage.executeCell("library('Rgraphviz')")
        }
      }
    }

    "should be able to install packages that depend on scikit-learn python package" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          val installTimeout = 5.minutes

          val installOutput = notebookPage.executeCell("""BiocManager::install('BiocSklearn')""", installTimeout)
          notebookPage.executeCell("library('BiocSklearn')")

        }
      }
    }

    "should be able to install packages that depend on hdf5" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          val installTimeout = 5.minutes

          val installOutput = notebookPage.executeCell("""BiocManager::install('rhdf5')""", installTimeout)
          notebookPage.executeCell("library(rhdf5)")

        }
      }
    }

    "should be able to install packages that depend on openbabel" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          val installTimeout = 5.minutes

          val installOutput = notebookPage.executeCell("""BiocManager::install('ChemmineOB')""", installTimeout)
          notebookPage.executeCell("library(ChemmineOB)")

        }
      }
    }

    "should be able to install packages that depend on gsl" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          val installTimeout = 5.minutes

          val installOutput =
            notebookPage.executeCell("""BiocManager::install('DirichletMultinomial')""", installTimeout)
          notebookPage.executeCell("library(DirichletMultinomial)")

        }
      }
    }

    "should be able to install packages that depend on magick++" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          val installTimeout = 5.minutes

          val installOutput = notebookPage.executeCell("""BiocManager::install('EBImage')""", installTimeout)
          notebookPage.executeCell("library(EBImage)")

        }
      }
    }

    "should be able to install packages that depend on database packages" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          val installTimeout = 5.minutes

          val installOutput = notebookPage.executeCell("""BiocManager::install('RMySQL')""", installTimeout)
          notebookPage.executeCell("library(RMySQL)")

        }
      }
    }

    "should be able to install packages that depend on jags" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          val installTimeout = 5.minutes

          val installOutput = notebookPage.executeCell("""BiocManager::install('rjags')""", installTimeout)
          notebookPage.executeCell("library('rjags')")

        }
      }
    }

    "should be able to install packages that depend on protobuf" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          val installTimeout = 5.minutes

          val installOutput = notebookPage.executeCell("""BiocManager::install('protolite')""", installTimeout)
          notebookPage.executeCell("library(protolite)")

        }
      }
    }

    "should be able to install packages that depend on Cairo and gtk" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          val installTimeout = 5.minutes

          val installOutput = notebookPage.executeCell("""BiocManager::install('RGtk2')""", installTimeout)
          notebookPage.executeCell("library(RGtk2)")

        }
      }
    }

    "should have Java available" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          val javaOutput = notebookPage.executeCell("""system('java --version', intern = TRUE)""")
          javaOutput shouldBe 'defined
          javaOutput.get should include("OpenJDK Runtime Environment")
          javaOutput.get should not include ("not found")
        }
      }
    }
  }
}
