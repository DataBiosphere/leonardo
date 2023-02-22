package org.broadinstitute.dsde.workbench.leonardo.rstudio

import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.notebooks.{NotebookTestUtils, Welder}
import org.openqa.selenium.Keys
import org.scalatest.DoNotDiscover

@DoNotDiscover
class RStudioSpec extends RuntimeFixtureSpec with NotebookTestUtils with RStudioTestUtils {
  implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()

  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.rstudioBioconductorImage.imageUrl)

  "RStudioSpec" - {

    "should launch RStudio" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewRStudio(runtimeFixture.runtime) { rstudioPage =>
          rstudioPage.pressKeys("varA <- 1000")
          rstudioPage.pressKeys(Keys.ENTER.toString)
          await visible cssSelector("[title~='varA']")
          rstudioPage.variableExists("varA") shouldBe true
          rstudioPage.variableExists("1000") shouldBe true
        }
      }
    }

    "environment variables should be available in RStudio" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewRStudio(runtimeFixture.runtime) { rstudioPage =>
          val expectedEVs =
            RuntimeFixtureSpec.getCustomEnvironmentVariables ++
              // variables implicitly set by Leo
              Map(
                "CLUSTER_NAME" -> runtimeFixture.runtime.clusterName.asString,
                "RUNTIME_NAME" -> runtimeFixture.runtime.clusterName.asString,
                "OWNER_EMAIL" -> runtimeFixture.runtime.creator.value
              )

          expectedEVs.foreach { case (k, v) =>
            rstudioPage.pressKeys(s"""var_$k <- Sys.getenv("$k")""")
            rstudioPage.pressKeys(Keys.ENTER.toString)
            Thread.sleep(2000)
            await visible cssSelector(s"[title~='var_$k']")
            rstudioPage.variableExists(s"var_$k") shouldBe true
            rstudioPage.variableExists(s""""$v"""") shouldBe true
          }
        }
      }
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

    // Note this test should be last because the test infrastructure does not close the shiny app
    "should launch an RShiny app" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewRStudio(runtimeFixture.runtime) { rstudioPage =>
          rstudioPage.pressKeys(Keys.ENTER.toString)
          rstudioPage.withRShinyExample("01_hello")(rshinyPage =>
            rshinyPage.getExampleHeader shouldBe Some("Hello Shiny!")
          )
        }
      }
    }
  }
}
