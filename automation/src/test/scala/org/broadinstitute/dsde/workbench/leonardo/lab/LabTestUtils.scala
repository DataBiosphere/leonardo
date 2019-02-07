package org.broadinstitute.dsde.workbench.leonardo.lab

import java.io.{File, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.util.Base64

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.dao.Google.googleStorageDAO
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.model.google.{EmailGcsEntity, GcsBucketName, GcsEntityTypes, GcsObjectName, GcsPath, GcsRoles}
import org.broadinstitute.dsde.workbench.service.Sam
import org.openqa.selenium.WebDriver
import org.scalatest.Suite

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

trait LabTestUtils extends LeonardoTestUtils {
  this: Suite with BillingFixtures =>

  private def whenKernelNotReady(t: Throwable): Boolean = t match {
    case e: KernelNotReadyException => true
    case _ => false
  }

  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  def withNewLabNotebook[T](cluster: Cluster, kernel: LabKernel = lab.Python2, timeout: FiniteDuration = 2.minutes)(testCode: LabNotebookPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    withLabLauncherPage(cluster) { labLauncherPage =>
      val result: Future[T] = retryUntilSuccessOrTimeout(whenKernelNotReady, failureLogMessage = s"Cannot make new notebook")(30 seconds, 2 minutes) {() =>
        Future(labLauncherPage.withNewLabNotebook(kernel, timeout) { labNotebookPage =>
          testCode(labNotebookPage)
        })
      }
      Await.result(result, 4 minutes)
    }
  }

  def withLocalizeDelocalizeFiles[T](cluster: Cluster, fileToLocalize: String, fileToLocalizeContents: String,
                                     fileToDelocalize: String, fileToDelocalizeContents: String,
                                     dataFileName: String, dataFileContents: String)(testCode: (Map[String, String], GcsBucketName, LabNotebookPage) => T)
                                    (implicit webDriver: WebDriver, token: AuthToken): T = {
    implicit val patienceConfig: PatienceConfig = storagePatience

    withNewGoogleBucket(cluster.googleProject) { bucketName =>
      // give the user's pet owner access to the bucket
      val petServiceAccount = Sam.user.petServiceAccountEmail(cluster.googleProject.value)
      googleStorageDAO.setBucketAccessControl(bucketName, EmailGcsEntity(GcsEntityTypes.User, petServiceAccount), GcsRoles.Owner).futureValue

      // create a bucket object to localize
      val bucketObjectToLocalize = GcsObjectName(fileToLocalize)
      withNewBucketObject(bucketName, bucketObjectToLocalize, fileToLocalizeContents, "text/plain") { objectName =>
        // give the user's pet read access to the object
        googleStorageDAO.setObjectAccessControl(bucketName, objectName, EmailGcsEntity(GcsEntityTypes.User, petServiceAccount), GcsRoles.Reader).futureValue

        // create a notebook file to delocalize
        withNewLabNotebook(cluster) { labNotebookPage =>
          labNotebookPage.runCodeInEmptyCell(s"""! echo -n "$fileToDelocalizeContents" > "$fileToDelocalize"""")

          val localizeRequest = Map(
            fileToLocalize -> GcsPath(bucketName, bucketObjectToLocalize).toUri,
            GcsPath(bucketName, GcsObjectName(fileToDelocalize)).toUri -> fileToDelocalize,
            dataFileName -> s"data:text/plain;base64,${Base64.getEncoder.encodeToString(dataFileContents.getBytes(StandardCharsets.UTF_8))}"
          )

          val testResult = Try(testCode(localizeRequest, bucketName, labNotebookPage))

          // Verify and save the localization.log file to test output to aid in debugging
          Try(verifyAndSaveLocalizationLog(cluster)) match {
            case Success(downloadFile) =>
              logger.info(s"Saved localization log for cluster ${cluster.projectNameString} to ${downloadFile.getAbsolutePath}")
            case Failure(e) =>
              logger.warn(s"Could not obtain localization log files for cluster ${cluster.projectNameString}: ${e.getMessage}")
          }

          //TODO:: the code below messes up the test somehow, figure out why that happens and fix.
          //TODO:: https://github.com/DataBiosphere/leonardo/issues/643
          // clean up files on the cluster
          // no need to clean up the bucket objects; that will happen as part of `withNewBucketObject`
          //notebookPage.executeCell(s"""! rm -f $fileToLocalize""")
          //notebookPage.executeCell(s"""! rm -f $fileToDelocalize""")

          testResult.get
        }
      }
    }
  }

  def verifyAndSaveLocalizationLog(cluster: Cluster)(implicit token: AuthToken): File = {
    // check localization.log for existence
    val localizationLog = Lab.getContentItem(cluster.googleProject, cluster.clusterName, "localization.log", includeContent = true)
    localizationLog.content shouldBe defined

    // Save localization.log to test output to aid in debugging
    val downloadFile = new File(logDir, s"${cluster.googleProject.value}-${cluster.clusterName.string}-localization.log")
    val fos = new FileOutputStream(downloadFile)
    fos.write(localizationLog.content.get.getBytes)
    fos.close()

    downloadFile
  }
}
