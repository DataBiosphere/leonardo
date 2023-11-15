package org.broadinstitute.dsde.workbench.leonardo.notebooks

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.SSH.SSHRuntimeInfo
import org.broadinstitute.dsde.workbench.leonardo.TestUser.{getAuthTokenAndAuthorization, Ron}
import org.broadinstitute.dsde.workbench.leonardo.runtimes.RuntimeGceSpecDependencies
import org.broadinstitute.dsde.workbench.leonardo.{
  BillingProjectFixtureSpec,
  CloudContext,
  CloudProvider,
  LeonardoApiClient,
  SSH,
  UserJupyterExtensionConfig
}
import org.http4s.headers.Authorization
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

/**
 * This spec verifies different cluster creation options, such as extensions, scopes, environment variables.
 *
 * TODO consider removing this spec and moving test cases to RuntimeGceSpec.
 */
//@DoNotDiscover
final class NotebookGCECustomizationSpec
    extends BillingProjectFixtureSpec
    with ParallelTestExecution
    with NotebookTestUtils
    with LazyLogging {
  implicit val (ronAuthToken: IO[AuthToken], ronAuthorization: IO[Authorization]) = getAuthTokenAndAuthorization(Ron)
  implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()

  val dependencies = for {
    storage <- google2StorageResource
    httpClient <- LeonardoApiClient.client
  } yield RuntimeGceSpecDependencies(httpClient, storage)

  // TODO: add user start up script test?
  "NotebookGCECustomizationSpec" - {
    // Using nbtranslate extension from here:
    // https://github.com/ipython-contrib/jupyter_contrib_nbextensions/tree/master/src/jupyter_contrib_nbextensions/nbextensions/nbTranslate
    "should install user specified notebook extensions" ignore { billingProject =>
      val translateExtensionFile = ResourceFile("bucket-tests/translate_nbextension.tar.gz")
      withResourceFileInBucket(billingProject, translateExtensionFile, "application/x-gzip") {
        translateExtensionBucketPath =>
          val extensionConfig = UserJupyterExtensionConfig(
            nbExtensions = Map("translate" -> translateExtensionBucketPath.toUri)
          )
          withNewRuntime(
            billingProject,
            request =
              LeonardoApiClient.defaultCreateRuntime2Request.copy(userJupyterExtensionConfig = Some(extensionConfig))
          ) { runtime =>
            withWebDriver { implicit driver =>
              withNewNotebook(runtime, Python3) { notebookPage =>
                // Check the extensions were installed
                val nbExt = notebookPage.executeCell("! jupyter nbextension list")

                nbExt.get should include("translate_nbextension/main  enabled")
                // should be installed by default
                nbExt.get should include("toc2/main  enabled")

                val serverExt = notebookPage.executeCell("! jupyter serverextension list")
                serverExt.get should include("jupyterlab  enabled")
                // should be installed by default
                serverExt.get should include("jupyter_nbextensions_configurator  enabled")

                // Exercise the translate extensionfailure_screenshots/NotebookGCECustomizationSpec_18-34-37-017.png
                notebookPage.translateMarkup("Yes") should include("Oui")
              }
            }
          }
      }
    }

    "should populate user-specified environment variables" in { billingProject =>
      // Note: the R image includes R and Python 3 kernels
      val runtimeRequest =
        LeonardoApiClient.defaultCreateRuntime2Request.copy(customEnvironmentVariables = Map("KEY" -> "value"))

      val res = dependencies.use { deps =>
        implicit val httpClient = deps.httpClient
        withNewRuntime(billingProject, request = runtimeRequest, monitorCreate = true) { cluster =>
          for {
//            _ <- IO(Thread.sleep(1000 * 60 * 5))
            runtime <- LeonardoApiClient.getRuntime(cluster.googleProject, cluster.clusterName)
            _ <- loggerIO.info(
              s"about to start ssh client, using runtime ${cluster.googleProject}/${cluster.clusterName} with status ${cluster.status}"
            )

            output <- SSH.executeCommand(runtime.asyncRuntimeFields.get.hostIp.get.asString,
                                         22,
                                         "echo $KEY",
                                         SSHRuntimeInfo(Some(runtime.googleProject), CloudProvider.Gcp)
            )
          } yield output.outputLines.mkString shouldBe "value"
        }
      }
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }
}
