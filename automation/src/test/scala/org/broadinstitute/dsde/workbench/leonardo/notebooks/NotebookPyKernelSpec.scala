package org.broadinstitute.dsde.workbench.leonardo.notebooks

import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.{LeonardoConfig, RuntimeFixtureSpec, SSH}
import org.broadinstitute.dsde.workbench.leonardo.RuntimeFixtureSpec.runtimeFixtureZone
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

/**
 * This spec verifies notebook functionality specifically around the Python 3 kernel.
 */
@DoNotDiscover
class NotebookPyKernelSpec extends RuntimeFixtureSpec with NotebookTestUtils with ParallelTestExecution {
  implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()

  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.pythonImageUrl)

  "NotebookPyKernelSpec" - {

    "should create a notebook with a working Python 3 kernel, import installed packages and default to notebooks dir in terminal" in {
      runtimeFixture =>
        withWebDriver { implicit driver =>
          withNewNotebook(runtimeFixture.runtime, Python3) { notebookPage =>
            val getPythonVersion =
              """import platform
                |print(platform.python_version())""".stripMargin
            val getBxPython =
              """import bx.bitset
                |bx.bitset.sys.copyright""".stripMargin
            val getPandasLocation = "! pip3 show pandas"
            notebookPage.executeCell("1+1") shouldBe Some("2")
            notebookPage.executeCell(getPythonVersion).get should include("3.10")
            notebookPage.executeCell(getBxPython).get should include("Copyright (c)")
            notebookPage.executeCell(getPandasLocation).get should include("/opt/conda/lib/python3.10/site-packages")
            notebookPage.executeCell("! pwd").get shouldBe "/home/jupyter"
          }
        }
    }

    "should be able to install python libraries with C bindings" in { runtimeFixture =>
      for {
        cythonOutput <- SSH.executeGoogleCommand(
          runtimeFixture.runtime.googleProject,
          runtimeFixtureZone.value,
          runtimeFixture.runtime.clusterName,
          "sudo docker exec -it jupyter-server pip show Cython"
        )
        potOutput <- SSH.executeGoogleCommand(
          runtimeFixture.runtime.googleProject,
          runtimeFixtureZone.value,
          runtimeFixture.runtime.clusterName,
          "sudo docker exec -it jupyter-server pip install POT"
        )
      } yield {
        cythonOutput should include("Name: Cython")
        potOutput should include("Successfully installed POT")
      }
    }

  }
}
