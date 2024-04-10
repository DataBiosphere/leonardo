package org.broadinstitute.dsde.workbench.leonardo.runtimes

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.{LeonardoApiClient, LeonardoConfig, RuntimeFixtureSpec, SSH}
import org.scalatest.{DoNotDiscover, ParallelTestExecution}
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.leonardo.BillingProjectFixtureSpec.workspaceNameKey

@DoNotDiscover
class RuntimeSystemSpec extends RuntimeFixtureSpec with ParallelTestExecution {
  implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.pythonImageUrl)

  val dependencies = for {
    storage <- google2StorageResource
    httpClient <- LeonardoApiClient.client
  } yield RuntimeGceSpecDependencies(httpClient, storage)

  "RuntimeSystemSpec" - {
    s"should have the workspace-related environment variables set in jupyter image" in { runtimeFixture =>
      // TODO: any others?
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

    // TODO: remove selenium from this, needs re-write
    // see https://broadworkbench.atlassian.net/browse/IA-4723
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
