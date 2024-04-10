package org.broadinstitute.dsde.workbench.leonardo.notebooks

import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.runtimes.RuntimeGceSpecDependencies
import org.broadinstitute.dsde.workbench.leonardo.{LeonardoApiClient, LeonardoConfig, RuntimeFixtureSpec, SSH}
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

/**
 * This spec verifies notebook functionality specifically around the R kernel.
 */
@DoNotDiscover
class NotebookRKernelSpec extends RuntimeFixtureSpec with ParallelTestExecution {
  implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()

  val dependencies = for {
    storage <- google2StorageResource
    httpClient <- LeonardoApiClient.client
  } yield RuntimeGceSpecDependencies(httpClient, storage)

  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.rImageUrl)

  "NotebookRKernelSpec" - {
    "should start up an r image and have utf encoding set up" in { runtimeFixture =>
      val res = dependencies.use { deps =>
        implicit val httpClient = deps.httpClient
        for {
          runtime <- LeonardoApiClient.getRuntime(runtimeFixture.runtime.googleProject,
                                                  runtimeFixture.runtime.clusterName
          )
          output <- SSH.executeGoogleCommand(runtime.googleProject,
                                             RuntimeFixtureSpec.runtimeFixtureZone.value,
                                             runtime.runtimeName,
                                             s"sudo docker exec -it jupyter-server printenv LC_ALL"
          )
        } yield output.trim shouldBe "en_US.UTF-8"
      }
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }
}
