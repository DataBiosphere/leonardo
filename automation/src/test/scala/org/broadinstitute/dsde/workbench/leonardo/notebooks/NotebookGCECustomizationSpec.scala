package org.broadinstitute.dsde.workbench.leonardo.notebooks

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.TestUser.{getAuthTokenAndAuthorization, Ron}
import org.broadinstitute.dsde.workbench.leonardo.runtimes.RuntimeGceSpecDependencies
import org.broadinstitute.dsde.workbench.leonardo.{
  BillingProjectFixtureSpec,
  LeonardoApiClient,
  NewBillingProjectAndWorkspaceBeforeAndAfterAll,
  SSH,
  UserJupyterExtensionConfig
}
import org.http4s.headers.Authorization
import org.scalatest.{DoNotDiscover, ParallelTestExecution}
import org.scalatest.tagobjects.Retryable

/**
 * This spec verifies different cluster creation options, such as extensions, scopes, environment variables.
 */
@DoNotDiscover
final class NotebookGCECustomizationSpec
    extends BillingProjectFixtureSpec
    with ParallelTestExecution
    with NotebookTestUtils
    with LazyLogging
    with NewBillingProjectAndWorkspaceBeforeAndAfterAll {
  implicit val (ronAuthToken: IO[AuthToken], ronAuthorization: IO[Authorization]) = getAuthTokenAndAuthorization(Ron)
  implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()

  val dependencies = for {
    storage <- google2StorageResource
    httpClient <- LeonardoApiClient.client
  } yield RuntimeGceSpecDependencies(httpClient, storage)

  "NotebookGCECustomizationSpec" - {
    // Using nbtranslate extension from here:
    // https://github.com/ipython-contrib/jupyter_contrib_nbextensions/tree/master/src/jupyter_contrib_nbextensions/nbextensions/nbTranslate
    "should install user specified notebook extensions" in { billingProject =>
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
      val runtimeRequest =
        LeonardoApiClient.defaultCreateRuntime2Request.copy(customEnvironmentVariables = Map("KEY" -> "value"))

      val res = dependencies.use { deps =>
        implicit val httpClient = deps.httpClient

        val runtimeName = randomClusterName
        val runtime = createNewRuntime(billingProject, runtimeName, runtimeRequest, monitor = true)
        for {
          getRuntime <- LeonardoApiClient.getRuntime(runtime.googleProject, runtime.clusterName)

          output <- SSH.executeGoogleCommand(getRuntime.googleProject,
                                             LeonardoApiClient.defaultCreateRequestZone.value,
                                             getRuntime.runtimeName,
                                             "sudo docker exec -it jupyter-server printenv KEY"
          )
          _ <- LeonardoApiClient.deleteRuntimeWithWait(runtime.googleProject, runtimeName)
        } yield output.trim() shouldBe "value"
      }
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }
}
