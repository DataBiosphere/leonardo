package org.broadinstitute.dsde.workbench.leonardo.notebooks

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.TestUser.{getAuthTokenAndAuthorization, Ron}
import org.broadinstitute.dsde.workbench.leonardo.{
  LeonardoConfig,
  NewBillingProjectAndWorkspaceBeforeAndAfterAll,
  RuntimeFixtureSpec
}
import org.http4s.headers.Authorization
import org.scalatest.DoNotDiscover

/**
 * This spec verifies notebook functionality specifically around the Python 3 kernel.
 */
@DoNotDiscover
class NotebookPyKernelSpec
    extends RuntimeFixtureSpec
    with NotebookTestUtils
    with NewBillingProjectAndWorkspaceBeforeAndAfterAll {

  implicit override val (ronAuthToken: IO[AuthToken], ronAuthorization: IO[Authorization]) =
    getAuthTokenAndAuthorization(Ron)
  implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()

  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.pythonImageUrl)

  "NotebookPyKernelSpec" - {

    // TODO: [discuss] Debatable but I like a kernel smoke test... I could see getting rid of this entirely
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
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, Python3) { notebookPage =>
          notebookPage.executeCell("! pip show Cython").get should include("Name: Cython")
          notebookPage.executeCell("! pip install POT").get should include("Successfully installed POT")
        }
      }
    }

  }
}
