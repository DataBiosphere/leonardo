package org.broadinstitute.dsde.workbench.leonardo.notebooks

import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.runtimes.RuntimeGceSpecDependencies
import org.broadinstitute.dsde.workbench.leonardo.{Leonardo, LeonardoApiClient, LeonardoConfig, RuntimeFixtureSpec, SSH}
import org.scalatest.DoNotDiscover

import scala.concurrent.duration.DurationLong
import cats.effect.unsafe.implicits.global

/**
 * This spec verifies notebook functionality specifically around the Python 3 kernel.
 */
@DoNotDiscover
class NotebookPyKernelSpec extends RuntimeFixtureSpec with NotebookTestUtils {
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

    s"should have the workspace-related environment variables set in ${Python3.toString} kernel" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        val expectedEVs =
          RuntimeFixtureSpec.getCustomEnvironmentVariables ++
            // variables implicitly set by Leo -
            Map(
              "CLUSTER_NAME" -> runtimeFixture.runtime.clusterName.asString,
              "RUNTIME_NAME" -> runtimeFixture.runtime.clusterName.asString,
              "OWNER_EMAIL" -> runtimeFixture.runtime.creator.value,
              // TODO: remove when PPW is rolled out to all workspaces - // and Leo removes the kernel_bootstrap logic.
              // See https://broadworkbench.atlassian.net/browse/IA-2936
              "WORKSPACE_NAME" -> "home"
            )
        withNewNotebook(runtimeFixture.runtime, Python3) { notebookPage =>
          notebookPage.executeCell("import os")
          expectedEVs.foreach { case (k, v) =>
            val res = notebookPage.executeCell(s"os.getenv('$k')")
            res shouldBe defined
            res.get shouldBe s"'$v'"

          }

        }
      }

      // TODO: [discuss] it does not seem like automation tests should be verifying security policies

      // TODO: [discuss] This is an expensive test that waited on the command for 5min+ to complete...
      // seems like a smell to be waiting on an unreliable ssh connection/selenium socket for any longer than 10 seconds
    }
  }
}
