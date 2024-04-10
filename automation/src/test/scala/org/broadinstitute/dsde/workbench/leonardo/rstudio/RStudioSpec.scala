package org.broadinstitute.dsde.workbench.leonardo.rstudio

import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.leonardo.BillingProjectFixtureSpec.workspaceNameKey
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.notebooks.{NotebookTestUtils, Welder}
import org.broadinstitute.dsde.workbench.leonardo.runtimes.RuntimeGceSpecDependencies
import org.openqa.selenium.Keys
import org.scalatest.{DoNotDiscover, ParallelTestExecution}
import cats.syntax.all._

@DoNotDiscover
class RStudioSpec extends RuntimeFixtureSpec with RStudioTestUtils with NotebookTestUtils with ParallelTestExecution {
  implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()

  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.rstudioBioconductorImage.imageUrl)

  val dependencies = for {
    storage <- google2StorageResource
    httpClient <- LeonardoApiClient.client
  } yield RuntimeGceSpecDependencies(httpClient, storage)

  "RStudioSpec" - {

    "environment variables should be available in RStudio" in { runtimeFixture =>
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
                                     s"sudo docker exec -it rstudio-server printenv $envVar"
            )
          )
        } yield outputs.map(_.trim).sorted shouldBe expectedEnvironment.values.toList.sorted
      }
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

    "Welder should be up" in { runtimeFixture =>
      val resp = Welder.getWelderStatus(runtimeFixture.runtime)
      resp.attempt.unsafeRunSync().isRight shouldBe true
    }

    "test Rmd file syncing" in { runtimeFixture =>
      val sampleNotebook = ResourceFile("bucket-tests/gcsFile.Rmd")
      val isEditMode = false
      val isRStudio = true
      withResourceFileInBucket(runtimeFixture.runtime.googleProject, sampleNotebook, "text/plain") { gcsPath =>
        withWelderInitialized(runtimeFixture.runtime, gcsPath, ".+(.R|.Rmd)$", isEditMode, isRStudio) { localizedFile =>
          withWebDriver { implicit driver =>
            withNewRStudio(runtimeFixture.runtime) { rstudioPage =>
              rstudioPage.pressKeys("install.packages('markdown')")
              rstudioPage.pressKeys(Keys.ENTER.toString)

              // Test to make sure that a file created in RStudio syncs properly
              rstudioPage.pressKeys("system(\"touch tests.Rmd\")")
              rstudioPage.pressKeys(Keys.ENTER.toString)
              await visible cssSelector("[title~='tests.Rmd']")

              val oldCreatedContentSize: Int =
                getObjectSize(gcsPath.bucketName, GcsBlobName("tests.Rmd"))
                  .unsafeRunSync()
              logger.info(s"New Created File's original size is ${oldCreatedContentSize}")

              rstudioPage.variableExists("tests.Rmd") shouldBe true
              rstudioPage.pressKeys("system(\"echo 'teterererrdffsfsfsfafafafafafafafafaffwewew' >> tests.Rmd\")")
              rstudioPage.pressKeys(Keys.ENTER.toString)
              rstudioPage.pressKeys("system(\"cat tests.Rmd\")")
              rstudioPage.pressKeys(Keys.ENTER.toString)

              Thread.sleep(45000)

              val newCreatedContentSize: Int =
                getObjectSize(gcsPath.bucketName, GcsBlobName("tests.Rmd"))
                  .unsafeRunSync()
              logger.info(s"New Created File's new size is ${newCreatedContentSize}")

              newCreatedContentSize should be > oldCreatedContentSize

              // Verify that a File in a bucket can be selected, updated, and synced
              val oldRemoteContentSize: Int =
                getObjectSize(gcsPath.bucketName, GcsBlobName(gcsPath.objectName.value))
                  .unsafeRunSync()
              logger.info(s"Resource File's original size is ${oldRemoteContentSize}")

              rstudioPage.pressKeys("system(\"echo 'teterererrdffsfsfsfafafafafafafafafaffwewew' >> gcsFile.Rmd\")")
              rstudioPage.pressKeys(Keys.ENTER.toString)

              Thread.sleep(45000)
              val newRemoteContentSize: Int =
                getObjectSize(gcsPath.bucketName, GcsBlobName(gcsPath.objectName.value))
                  .unsafeRunSync()
              logger.info(s"Resource File's new size is ${newRemoteContentSize}")

              newRemoteContentSize should be > oldRemoteContentSize

              // Verify that a File with an incorrect extension is not picked up
              rstudioPage.pressKeys("system(\"touch tested.lmd\")")
              rstudioPage.pressKeys(Keys.ENTER.toString)
              await visible cssSelector("[title~='tested.lmd']")

              rstudioPage.pressKeys("system(\"echo 'teterererrdffsfsfsfafafafafafafafafaffwewew' >> tested.lmd\")")
              rstudioPage.pressKeys(Keys.ENTER.toString)

              Thread.sleep(45000)

              val incorrectFileEndingContentSize: Int =
                getObjectSize(gcsPath.bucketName, GcsBlobName("tested.lmd"))
                  .unsafeRunSync()
              logger.info(s"Incorrect File's new size is ${incorrectFileEndingContentSize}")

              incorrectFileEndingContentSize should be < newRemoteContentSize
            }
          }
        }
      }
    }
  }
}
