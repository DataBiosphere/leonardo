package org.broadinstitute.dsde.workbench.leonardo.runtimes

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.{BillingProjectFixtureSpec, CloudProvider, LeonardoApiClient, LeonardoConfig, RuntimeFixtureSpec, SSH}
import org.scalatest.DoNotDiscover
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.leonardo.SSH.SSHRuntimeInfo

import java.nio.file.{Files, Path}

@DoNotDiscover
class RuntimeSystemSpec extends RuntimeFixtureSpec {
  implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.rImageUrl)

  val dependencies = for {
    storage <- google2StorageResource
    httpClient <- LeonardoApiClient.client
  } yield RuntimeGceSpecDependencies(httpClient, storage)

  "RuntimeSystemSpec" - {
    s"should have the workspace-related environment variables set" in { runtimeFixture =>
      // TODO: any others?
      val expectedEnvironment = Map(
        "CLUSTER_NAME" -> runtimeFixture.runtime.clusterName.asString,
        "RUNTIME_NAME" -> runtimeFixture.runtime.clusterName.asString,
        "OWNER_EMAIL" -> runtimeFixture.runtime.creator.value,
        "WORKSPACE_NAME" -> "home"
      )

      val res = dependencies.use { deps =>
        implicit val httpClient = deps.httpClient
        for {
          runtime <- LeonardoApiClient.getRuntime(runtimeFixture.runtime.googleProject,
                                                  runtimeFixture.runtime.clusterName
          )
          outputs <- expectedEnvironment.keys.toList.traverse(envVar =>
            SSH.executeCommand(runtime.asyncRuntimeFields.get.hostIp.get.asString,
                               22,
                               s"echo $envVar",
              SSHRuntimeInfo(Some(runtime.googleProject), CloudProvider.Gcp)
            )
          )
        } yield outputs.map(_.outputLines.mkString).sorted shouldBe expectedEnvironment.values.toList.sorted
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
          output <- SSH.executeCommand(runtime.asyncRuntimeFields.get.hostIp.get.asString,
                                       22,
                                       s"java --version",
            SSHRuntimeInfo(Some(runtime.googleProject), CloudProvider.Gcp)
          )
        } yield output.outputLines.mkString should include("OpenJDK Runtime Environment")
      }
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

    "should set up the users google identity on the vm" in { runtimeFixture =>
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
            gsUtilCommand <- SSH.executeCommand(runtime.asyncRuntimeFields.get.hostIp.get.asString,
                                                22,
                                                s"gsutil cp ${gcsPath.toUri} /tmp/gcsFile.ipynb",
              SSHRuntimeInfo(Some(runtime.googleProject), CloudProvider.Gcp)
            )
            cat <- SSH.executeCommand(runtime.asyncRuntimeFields.get.hostIp.get.asString,
                                      22,
                                      s"cat /tmp/gcsFile.ipynb",
              SSHRuntimeInfo(Some(runtime.googleProject), CloudProvider.Gcp)
            )
          } yield {
            gsUtilCommand.exitCode shouldBe 0
            cat.exitCode shouldBe 0
          }
        }
      }
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

  }
}
