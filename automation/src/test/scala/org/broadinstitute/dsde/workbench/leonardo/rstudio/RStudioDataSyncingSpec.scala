package org.broadinstitute.dsde.workbench.leonardo.rstudio

import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.notebooks._
import org.openqa.selenium.Keys
import org.scalatest.DoNotDiscover

/**
 * This spec verifies data syncing functionality, including notebook edit mode, playground mode,
 * and welder localization/delocalization.
 */
@DoNotDiscover
class RStudioDataSyncingSpec extends RuntimeFixtureSpec with NotebookTestUtils with RStudioTestUtils{
  override def enableWelder: Boolean = true

  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.rstudioBioconductorImage.imageUrl)

  implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()

  "RStudioDataSyncingSpec" - {

    "Welder should be up" in { runtimeFixture =>
      val resp = Welder.getWelderStatus(runtimeFixture.runtime)
      resp.attempt.unsafeRunSync().isRight shouldBe true
    }

 // Create file make sure it syncs to bucket
 // Update file with out saving, wait for auto save, and verify file has update (content size changes)

    "test Rmd file syncing" in { runtimeFixture =>
      val sampleNotebook = ResourceFile("bucket-tests/gcsFile.Rmd")
      val isEditMode= false
      val isRStudio = true
      withResourceFileInBucket(runtimeFixture.runtime.googleProject, sampleNotebook, "text/plain") { gcsPath =>
        withWelderInitialized(runtimeFixture.runtime, gcsPath, ".+(.R|.Rmd)$", isEditMode, isRStudio) { localizedFile =>
          withWebDriver { implicit driver =>
            withNewRStudio(runtimeFixture.runtime) { rstudioPage =>
              rstudioPage.pressKeys("install.packages('markdown')")
              rstudioPage.pressKeys(Keys.ENTER.toString)

              //Test to make sure that a file created in RStudio syncs properly
              rstudioPage.pressKeys("system(\"touch tests.Rmd\")")
              rstudioPage.pressKeys(Keys.ENTER.toString)
              await visible cssSelector("[title~='tests.Rmd']")

              //Sleep is used to give time for background syncing to take place
              Thread.sleep(30000)

              logger.info(s"gcsPath ${gcsPath}")

              val oldCreatedContentSize: Int =
                getObjectSize(gcsPath.bucketName, GcsBlobName("tests.Rmd"))
                  .unsafeRunSync()
              logger.info(s"New Created File's original size is ${oldCreatedContentSize}")

              rstudioPage.variableExists("tests.Rmd") shouldBe true
              rstudioPage.pressKeys("system(\"echo 'teterererrdffsfsfsfafafafafafafafafaffwewew' >> tests.Rmd\")")
              rstudioPage.pressKeys(Keys.ENTER.toString)
              rstudioPage.pressKeys("system(\"cat tests.Rmd\")")
              rstudioPage.pressKeys(Keys.ENTER.toString)


              Thread.sleep(30000)

              val newCreatedContentSize: Int =
                getObjectSize(gcsPath.bucketName, GcsBlobName("tests.Rmd"))
                  .unsafeRunSync()
              logger.info(s"New Created File's new size is ${newCreatedContentSize}")

              newCreatedContentSize should be > oldCreatedContentSize

              //Verify that a File in a bucket can be selected, updated, and synced
              val oldRemoteContentSize: Int =
                getObjectSize(gcsPath.bucketName, GcsBlobName(gcsPath.objectName.value))
                  .unsafeRunSync()
              logger.info(s"Resource File's original size is ${oldRemoteContentSize}")



              rstudioPage.pressKeys("system(\"echo 'teterererrdffsfsfsfafafafafafafafafaffwewew' >> gcsFile.Rmd\")")
              rstudioPage.pressKeys(Keys.ENTER.toString)

              Thread.sleep(30000)
              val newRemoteContentSize: Int =
                getObjectSize(gcsPath.bucketName, GcsBlobName(gcsPath.objectName.value))
                  .unsafeRunSync()
              logger.info(s"Resource File's new size is ${newRemoteContentSize}")

              newRemoteContentSize should be > oldRemoteContentSize

              //Verify that a File with an incorrect extension is not picked up
              rstudioPage.pressKeys("system(\"touch tested.lmd\")")
              rstudioPage.pressKeys(Keys.ENTER.toString)
              await visible cssSelector("[title~='tested.lmd']")

              Thread.sleep(30000)

              rstudioPage.pressKeys("system(\"echo 'teterererrdffsfsfsfafafafafafafafafaffwewew' >> tested.lmd\")")
              rstudioPage.pressKeys(Keys.ENTER.toString)

              Thread.sleep(30000)

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
