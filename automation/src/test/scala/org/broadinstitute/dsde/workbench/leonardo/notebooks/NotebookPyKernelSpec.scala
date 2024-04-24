package org.broadinstitute.dsde.workbench.leonardo.notebooks

import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.{LeonardoApiClient, LeonardoConfig, RuntimeFixtureSpec, SSH}
import org.broadinstitute.dsde.workbench.leonardo.RuntimeFixtureSpec.runtimeFixtureZone
import org.broadinstitute.dsde.workbench.leonardo.runtimes.RuntimeGceSpecDependencies
import org.scalatest.{DoNotDiscover, ParallelTestExecution}
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.leonardo.BillingProjectFixtureSpec.workspaceNameKey

/**
 * This spec verifies notebook functionality specifically around the Python 3 kernel.
 */
@DoNotDiscover
class NotebookPyKernelSpec extends RuntimeFixtureSpec with NotebookTestUtils with ParallelTestExecution {
  implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()
  override def runtimeSystemKey: Option[String] = Some(getClass.getSimpleName)

  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.pythonImageUrl)

  val dependencies = for {
    storage <- google2StorageResource
    httpClient <- LeonardoApiClient.client
  } yield RuntimeGceSpecDependencies(httpClient, storage)

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

  "RuntimeSystemSpec" - {
    s"should have the workspace-related environment variables set in jupyter image" in { runtimeFixture =>
      val expectedEnvironment = Map(
        "CLUSTER_NAME" -> runtimeFixture.runtime.clusterName.asString,
        "RUNTIME_NAME" -> runtimeFixture.runtime.clusterName.asString,
        "OWNER_EMAIL" -> runtimeFixture.runtime.creator.value,
        "WORKSPACE_NAME" -> sys.props.getOrElse(workspaceNameKey, "workspace")
      )

      val res = dependencies.use { deps =>
        implicit val httpClient = deps.httpClient
        for {
          runtime <- LeonardoApiClient.getRuntime(runtimeFixture.runtime.googleProject,
                                                  runtimeFixture.runtime.clusterName
          )
          outputs <- expectedEnvironment.keys.toList.traverse(envVar =>
            SSH.executeGoogleCommand(runtime.googleProject,
                                     RuntimeFixtureSpec.runtimeFixtureZone.value,
                                     runtime.runtimeName,
                                     s"sudo docker exec -it jupyter-server printenv $envVar"
            )
          )
        } yield outputs.map(_.trim).sorted shouldBe expectedEnvironment.values.toList.sorted
      }
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

    "should have Java available" in { runtimeFixture =>
      val res = dependencies.use { deps =>
        implicit val httpClient = deps.httpClient
        for {
          runtime <- LeonardoApiClient.getRuntime(runtimeFixture.runtime.googleProject,
                                                  runtimeFixture.runtime.clusterName
          )
          output <- SSH.executeGoogleCommand(runtime.googleProject,
                                             RuntimeFixtureSpec.runtimeFixtureZone.value,
                                             runtime.runtimeName,
                                             s"sudo docker exec -it jupyter-server java --version"
          )
        } yield output should include("OpenJDK Runtime Environment")
      }
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

    "should set up the users google identity on the vm" ignore { runtimeFixture =>
      val res = dependencies.use { deps =>
        implicit val httpClient = deps.httpClient
        withResourceFileInBucket(runtimeFixture.runtime.googleProject,
                                 ResourceFile("bucket-tests/gcsFile.ipynb"),
                                 "text/plain"
        ) { gcsPath =>
          for {
            runtime <- LeonardoApiClient.getRuntime(runtimeFixture.runtime.googleProject,
                                                    runtimeFixture.runtime.clusterName
            )
            gsUtilCommand <- SSH.executeGoogleCommand(
              runtime.googleProject,
              RuntimeFixtureSpec.runtimeFixtureZone.value,
              runtime.runtimeName,
              s"sudo docker exec -it -u root jupyter-server gsutil cp ${gcsPath.toUri} /tmp/gcsFile.ipynb"
            )
            cat <- SSH.executeGoogleCommand(
              runtime.googleProject,
              RuntimeFixtureSpec.runtimeFixtureZone.value,
              runtime.runtimeName,
              s"sudo docker exec -it -u root jupyter-server cat /tmp/gcsFile.ipynb"
            )
            _ <- loggerIO.info(s"cat and gsutil output: \n\t$gsUtilCommand\n\t$cat")
          } yield cat shouldBe "test"
        }
      }
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

  }
}
