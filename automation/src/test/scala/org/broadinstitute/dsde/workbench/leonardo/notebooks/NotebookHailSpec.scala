package org.broadinstitute.dsde.workbench.leonardo.notebooks

import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.dao.Google.googleStorageDAO
import org.broadinstitute.dsde.workbench.leonardo.{CloudService, LeonardoConfig, RuntimeFixtureSpec}
import org.broadinstitute.dsde.workbench.model.google.{EmailGcsEntity, GcsEntityTypes, GcsObjectName, GcsRoles}
import org.broadinstitute.dsde.workbench.service.Sam
import org.scalatest.DoNotDiscover

import scala.concurrent.duration._

/**
 * This spec verifies Hail and Spark functionality.
 */
@DoNotDiscover
class NotebookHailSpec extends RuntimeFixtureSpec with NotebookTestUtils {

  // Should match the HAILHASH env var in the Jupyter Dockerfile
  val expectedHailVersion = "0.2.120"
  val hailTutorialUploadFile = ResourceFile(s"diff-tests/hail-tutorial.ipynb")
  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.hailImageUrl)
  override val cloudService: Option[CloudService] = Some(CloudService.Dataproc)

  "NotebookHailSpec" - {
    "should install the right Hail version" in { clusterFixture =>
      implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()
      Thread.sleep(30000) // Sleep 30 seconds to make tests more reliable hopefully
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.runtime, Python3, 10.minutes) { notebookPage =>
          // Verify we have the right hail version
          val importHail =
            """import hail as hl
              |hl.init()
            """.stripMargin

          val importHailOutput =
            s"""Welcome to
               |     __  __     <>__
               |    / /_/ /__  __/ /
               |   / __  / _ `/ / /
               |  /_/ /_/\\_,_/_/_/   version $expectedHailVersion""".stripMargin

          // Note: hl.init() displays several cell outputs. The 'Welcome to Hail' string should be the last output.
          notebookPage
            .executeCellWithCellOutput(importHail, timeout = 2.minutes, cellNumberOpt = Some(1))
            .map(_.output.last)
            .get should include(importHailOutput)

          // Run the Hail tutorial and verify
          // https://hail.is/docs/0.2/tutorials-landing.html
          // TO DO: fix this part of the tutorial , also edit the cellNumberOpts below when uncommenting
//          val tutorialToRun =
//            """
//              |hl.utils.get_movie_lens('data/')
//              |users = hl.read_table('data/users.ht')
//              |users.aggregate(hl.agg.count())""".stripMargin
//          val tutorialCellResult =
//            notebookPage.executeCellWithCellOutput(tutorialToRun, cellNumberOpt = Some(2)).map(_.output.tail.last).get
//          tutorialCellResult.toInt shouldBe (943)

          // Verify spark job is run in non local mode
          val getSparkContext =
            """
              |hl.spark_context()""".stripMargin
          val getSparkContextCellResult =
            notebookPage.executeCell(getSparkContext, cellNumberOpt = Some(2)).get
          getSparkContextCellResult.contains("yarn") shouldBe true

          // Verify spark job works
          val sparkJobToSucceed =
            """import random
              |hl.balding_nichols_model(3, 1000, 1000)._force_count_rows()""".stripMargin
          val sparkJobToSucceedcellResult =
            notebookPage
              .executeCellWithCellOutput(sparkJobToSucceed, cellNumberOpt = Some(3))
              .map(_.output.tail.last)
              .get
          sparkJobToSucceedcellResult.toInt shouldBe 1000
        }
      }
    }

    // Make sure we can import data from GCS into Hail.
    // See https://broadworkbench.atlassian.net/browse/IA-1558
    "should import data from GCS" in { clusterFixture =>

      implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()
      // Create a new bucket
      withNewGoogleBucket(clusterFixture.runtime.googleProject) { bucketName =>
        val ronPetServiceAccount =
          Sam.user.petServiceAccountEmail(clusterFixture.runtime.googleProject.value)(ronToken)
        googleStorageDAO.setBucketAccessControl(bucketName,
                                                EmailGcsEntity(GcsEntityTypes.User, ronPetServiceAccount),
                                                GcsRoles.Owner
        )

        // Add a sample TSV to the bucket
        val tsvString =
          """Sample     Height  Status  Age
            |PT-1234    154.1   ADHD    24
            |PT-1236    160.9   Control 19
            |PT-1238    NA      ADHD    89
            |PT-1239    170.3   Control 55""".stripMargin
        val tsvObjectName = GcsObjectName("samples.tsv")
        val tsvUri = s"gs://${bucketName.value}/${tsvObjectName.value}"

        withNewBucketObject(bucketName, tsvObjectName, tsvString, "text/plain") { objectName =>
          googleStorageDAO.setObjectAccessControl(bucketName,
                                                  objectName,
                                                  EmailGcsEntity(GcsEntityTypes.User, ronPetServiceAccount),
                                                  GcsRoles.Owner
          )

          withWebDriver { implicit driver =>
            withNewNotebook(clusterFixture.runtime, Python3) { notebookPage =>
              // Import hail
              val importHail =
                """import hail as hl
                  |hl.init()
                """.stripMargin
              notebookPage.executeCell(importHail, timeout = 2.minutes)

              // Import the TSV into a Hail table
              val importResult =
                notebookPage.executeCell(s"table = hl.import_table('${tsvUri}', impute=True)", timeout = 5.minutes)
              importResult shouldBe defined
              importResult.get should include("Finished type imputation")

              // Verify the Hail table
              val tableResult = notebookPage.executeCellWithCellOutput("table.count()").map(_.output.last)
              tableResult shouldBe Some("4")
            }
          }
        }
      }
    }

    // Make sure we can import a Hail table from a pandas dataframe.
    // See https://broadworkbench.atlassian.net/browse/IA-1637
    // This also simulates this featured workspace: https://app.terra.bio/#workspaces/fc-product-demo/2019_ASHG_Reproducible_GWAS
    "should import a pandas DataFrame into Hail" in { clusterFixture =>
      implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()
      withResourceFileInBucket(clusterFixture.runtime.googleProject,
                               ResourceFile("bucket-tests/hail_samples.csv"),
                               "text/plain"
      ) { gcsPath =>
        withWebDriver { implicit driver =>
          withNewNotebook(clusterFixture.runtime, Python3) { notebookPage =>
            // Localize the CSV
            val localizeResult = notebookPage.executeCell(s"! gsutil cp ${gcsPath.toUri} .")
            localizeResult shouldBe defined
            localizeResult.get should include("Operation completed")

            // Read the CSV into a pandas DataFrame
            val dataFrame =
              s"""import pandas as pd
                 |df = pd.read_csv('hail_samples.csv')
                 |df.shape""".stripMargin
            notebookPage.executeCell(dataFrame).get shouldBe "(2504, 15)" // (rows, cols)

            // Import hail
            val importHail =
              """import hail as hl
                |hl.init()
                """.stripMargin
            notebookPage.executeCell(importHail, timeout = 2.minutes)

            // Import the DataFrame into a Hail table
            val result =
              notebookPage.executeCell(s"samples = hl.Table.from_pandas(df, key = 'sample')", timeout = 5.minutes)
            result shouldBe defined
            result.get should not include "FatalError"
            result.get should not include "PythonException"
            // TODO: Uncomment if future hail version fixes this
            // result.get should include("Coerced sorted dataset")

            // Verify the Hail table
            val tableResult = notebookPage.executeCellWithCellOutput("samples.count()").map(_.output.last)
            tableResult shouldBe Some("2504") // rows
          }
        }
      }
    }
  }
}
