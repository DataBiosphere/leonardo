package org.broadinstitute.dsde.workbench.leonardo.notebooks

import cats.data.NonEmptyList
import cats.effect.IO
import com.google.cloud.Identity
import jdk.jshell.spi.ExecutionControl.NotImplementedException
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.google2.StorageRole.ObjectAdmin
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GetMetadataResponse, StorageRole}
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.service.Sam
import org.broadinstitute.dsde.workbench.util2.RemoveObjectResult
import org.http4s.headers.Authorization
import org.openqa.selenium.WebDriver
import org.scalatest.TestSuite

import java.io.File
import java.time.Instant
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

trait NotebookTestUtils extends LeonardoTestUtils {
  this: TestSuite =>
  private def whenKernelNotReady(t: Throwable): Boolean = t match {
    case _: KernelNotReadyException => true
    case _                          => false
  }

  def withNotebooksListPage[T](
    runtimeProjectAndName: RuntimeProjectAndName
  )(testCode: NotebooksListPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    val googleProject = runtimeProjectAndName.cloudContext match {
      case CloudContext.Gcp(v)   => v
      case CloudContext.Azure(_) => throw new NotImplementedException("Azure runtime is not supported yet")
    }
    val notebooksListPage = Notebook.get(googleProject, runtimeProjectAndName.runtimeName)
    testCode(notebooksListPage.open)
  }

  def withFileUpload[T](cluster: ClusterCopy, file: File)(
    testCode: NotebooksListPage => T
  )(implicit webDriver: WebDriver, token: AuthToken): T =
    withNotebooksListPage(RuntimeProjectAndName(CloudContext.Gcp(cluster.googleProject), cluster.clusterName)) {
      notebooksListPage =>
        notebooksListPage.upload(file)
        testCode(notebooksListPage)
    }

  def withNotebookUpload[T](cluster: ClusterCopy, file: File, timeout: FiniteDuration = 2.minutes)(
    testCode: NotebookPage => T
  )(implicit webDriver: WebDriver, token: AuthToken): T =
    withFileUpload(cluster, file) { notebooksListPage =>
      logger.info(
        s"Opening notebook ${file.getAbsolutePath} notebook on cluster ${cluster.googleProject.value} / ${cluster.clusterName.asString}..."
      )
      notebooksListPage.withOpenNotebook(file, timeout)(notebookPage => testCode(notebookPage))
    }

  def withNewNotebook[T](cluster: ClusterCopy, kernel: NotebookKernel = Python3, timeout: FiniteDuration = 3.minutes)(
    testCode: NotebookPage => T
  )(implicit webDriver: WebDriver, token: AuthToken): T =
    withNewNotebookWithCheck(RuntimeProjectAndName(CloudContext.Gcp(cluster.googleProject), cluster.clusterName),
                             kernel,
                             timeout
    )(testCode)

  def withNewNotebookWithCheck[T](runtimeProjectAndName: RuntimeProjectAndName,
                                  kernel: NotebookKernel = Python3,
                                  timeout: FiniteDuration = 3.minutes
  )(
    testCode: NotebookPage => T
  )(implicit webDriver: WebDriver, token: AuthToken): T = {
    // Note we retry the entire notebook creation when we encounter KernelNotReadyException
    val result = retryUntilSuccessOrTimeout(
      whenKernelNotReady,
      failureLogMessage =
        s"Cannot make new notebook on ${runtimeProjectAndName.cloudContext.asStringWithProvider} / ${runtimeProjectAndName.runtimeName.asString} for ${kernel}"
    )(30 seconds, 10 minutes) { () =>
      withNotebooksListPage(runtimeProjectAndName) { notebooksListPage =>
        logger.info(
          s"Creating new ${kernel.string} notebook on cluster ${runtimeProjectAndName.cloudContext.asStringWithProvider} / ${runtimeProjectAndName.runtimeName.asString}..."
        )
        Future(
          notebooksListPage.withNewNotebook(kernel, timeout) { notebookPage =>
            val res = testCode(notebookPage)
            notebookPage.saveAndCheckpoint()
            res
          }
        )
      }
    }
    Await.result(result, 10 minutes)
  }

  // Creates a notebook with the directory structure:
  //   ~jupyter-user/notebooks/Untitled Folder/Untitled Folder/Untitled.ipynb
  // This roughly simulates an real-life directory structure like:
  //   ~jupyter-user/notebooks/Workspace Name/edit/notebook.ipynb
  // Ideally we would name the directories similarly but the Jupyter UI doesn't make that easy.
  def withNewNotebookInSubfolder[T](
    cluster: ClusterCopy,
    kernel: NotebookKernel = Python3,
    timeout: FiniteDuration = 2.minutes
  )(testCode: NotebookPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    // Note we retry the entire notebook creation when we encounter KernelNotReadyException
    val result =
      retryUntilSuccessOrTimeout(whenKernelNotReady, failureLogMessage = s"Cannot make new notebook")(30 seconds,
                                                                                                      2 minutes
      ) { () =>
        withNotebooksListPage(RuntimeProjectAndName(CloudContext.Gcp(cluster.googleProject), cluster.clusterName)) {
          notebooksListPage =>
            notebooksListPage.withSubFolder(timeout) { notebooksListPage =>
              notebooksListPage.withSubFolder(timeout) { notebooksListPage =>
                logger.info(
                  s"Creating new ${kernel.string} notebook on cluster ${cluster.googleProject.value} / ${cluster.clusterName.asString}..."
                )
                Future(
                  notebooksListPage.withNewNotebook(kernel, timeout)(notebookPage => testCode(notebookPage))
                )
              }
            }
        }
      }
    Await.result(result, 10 minutes)
  }

  def withOpenNotebook[T](cluster: ClusterCopy, notebookPath: File, timeout: FiniteDuration = 2.minutes)(
    testCode: NotebookPage => T
  )(implicit webDriver: WebDriver, token: AuthToken): T =
    withNotebooksListPage(RuntimeProjectAndName(CloudContext.Gcp(cluster.googleProject), cluster.clusterName)) {
      notebooksListPage =>
        logger.info(
          s"Opening notebook ${notebookPath.getAbsolutePath} notebook on cluster ${cluster.googleProject.value} / ${cluster.clusterName.asString}..."
        )
        notebooksListPage.withOpenNotebook(notebookPath, timeout)(notebookPage => testCode(notebookPage))
    }

  def uploadDownloadTest(cluster: ClusterCopy, uploadFile: File, timeout: FiniteDuration, fileDownloadDir: String)(
    assertion: (File, File) => Any
  )(implicit webDriver: WebDriver, token: AuthToken): Any = {
    cluster.status shouldBe ClusterStatus.Running
    uploadFile.exists() shouldBe true

    withNotebookUpload(cluster, uploadFile) { notebook =>
      notebook.runAllCells(timeout)
      notebook.downloadAsIpynb()
    }

    // sanity check the file downloaded correctly
    val downloadFile = new File(fileDownloadDir, uploadFile.getName)
    downloadFile.exists() shouldBe true
    downloadFile.isFile() shouldBe true
    downloadFile.deleteOnExit()
  }

  def pipInstall(notebookPage: NotebookPage, kernel: NotebookKernel, packageName: String): Unit = {
    val pip = kernel match {
      case Python2 | PySpark2 => "pip2"
      case Python3 | PySpark3 => "pip3"
      case _                  => throw new IllegalArgumentException(s"Can't pip install in a ${kernel.string} kernel")
    }

    val installOutput = notebookPage.executeCell(s"!$pip install $packageName")
    installOutput shouldBe defined
    installOutput.get should include(s"Collecting $packageName")
    installOutput.get should include("Installing collected packages:")
    installOutput.get should include("Successfully installed")
    installOutput.get should not include "Exception:"
  }

  // https://github.com/aymericdamien/TensorFlow-Examples/blob/master/notebooks/1_Introduction/helloworld.ipynb
  def verifyTensorFlow(notebookPage: NotebookPage, kernel: NotebookKernel): Unit = {
    notebookPage.executeCell("import tensorflow as tf")
    notebookPage.executeCell("hello = tf.constant('Hello, TensorFlow!')") shouldBe None
    notebookPage.executeCell("sess = tf.Session()") shouldBe None
    val helloOutput = notebookPage.executeCell("print(sess.run(hello))")
    kernel match {
      case Python2 => helloOutput shouldBe Some("Hello, TensorFlow!")
      case Python3 => helloOutput shouldBe Some("b'Hello, TensorFlow!'")
      case other   => fail(s"Unexpected kernel: $other")
    }
  }

  // initializes storageLinks/ and localizes the file to the passed gcsPath
  def withWelderInitialized[T](cluster: ClusterCopy,
                               gcsPath: GcsPath,
                               pattern: String,
                               shouldLocalizeFileInEditMode: Boolean,
                               isRStudio: Boolean
  )(
    testCode: File => T
  )(implicit token: AuthToken): T = {
    Welder.postStorageLink(cluster, gcsPath, pattern, isRStudio)
    Welder.localize(cluster, gcsPath, shouldLocalizeFileInEditMode, isRStudio)

    val localPath: String = Welder.getLocalPath(gcsPath, shouldLocalizeFileInEditMode, isRStudio)
    val localFile: File = new File(localPath)

    logger.info("Initialized welder via /storageLinks and /localize")
    testCode(localFile)
  }

  def getLockedBy(workspaceBucketName: GcsBucketName, notebookName: GcsBlobName): IO[Option[String]] =
    google2StorageResource.use { google2StorageDAO =>
      for {
        metadata <- google2StorageDAO.getObjectMetadata(workspaceBucketName, notebookName, None).compile.last
        lockExpiresAt = metadata match {
          case Some(GetMetadataResponse.Metadata(_, metadataMap, _)) if metadataMap.contains("lockExpiresAt") =>
            Some(metadataMap("lockExpiresAt"))
          case _ => None
        }
        currentlyLocked = lockExpiresAt match {
          case Some(instantStr) => Instant.ofEpochMilli(instantStr.toLong).compareTo(Instant.now()) == 1
          case None             => false
        }
        lastLockedBy =
          if (currentlyLocked) {
            metadata match {
              case Some(GetMetadataResponse.Metadata(_, metadataMap, _)) if metadataMap.contains("lastLockedBy") =>
                Some(metadataMap("lastLockedBy"))
              case _ => None
            }
          } else None
      } yield lastLockedBy
    }

  def getObjectAsString(workspaceBucketName: GcsBucketName, notebookName: GcsBlobName): IO[Option[String]] =
    google2StorageResource.use { google2StorageDAO =>
      google2StorageDAO.unsafeGetBlobBody(workspaceBucketName, notebookName, None)
    }

  def getObjectSize(workspaceBucketName: GcsBucketName, notebookName: GcsBlobName): IO[Int] =
    google2StorageResource.use { google2StorageDAO =>
      google2StorageDAO.getBlobBody(workspaceBucketName, notebookName).compile.toList.map(_.size)
    }

  def deleteObject(workspaceBucketName: GcsBucketName, notebookName: GcsBlobName): IO[RemoveObjectResult] =
    google2StorageResource.use { google2StorageDAO =>
      google2StorageDAO.removeObject(workspaceBucketName, notebookName).compile.lastOrError
    }

  def setObjectMetadata(workspaceBucketName: GcsBucketName,
                        notebookName: GcsBlobName,
                        metadata: Map[String, String]
  ): IO[Unit] =
    // lockExpiresAt, lastLockedBy
    google2StorageResource.use { google2StorageDAO =>
      google2StorageDAO.setObjectMetadata(workspaceBucketName, notebookName, metadata, None).compile.drain
    }

  def setObjectContents(googleProject: GoogleProject,
                        workspaceBucketName: GcsBucketName,
                        notebookName: GcsBlobName,
                        contents: String
  )(implicit token: AuthToken): IO[Unit] = {
    val petServiceAccount = Sam.user.petServiceAccountEmail(googleProject.value)
    val userID = Identity.serviceAccount(petServiceAccount.value)

    google2StorageResource.use { google2StorageDAO =>
      for {
        _ <- google2StorageDAO
          .createBlob(workspaceBucketName, notebookName, contents.toCharArray.map(_.toByte))
          .compile
          .drain
        _ <- google2StorageDAO
          .setIamPolicy(workspaceBucketName,
                        Map(ObjectAdmin.asInstanceOf[StorageRole] -> NonEmptyList[Identity](userID, List()))
          )
          .compile
          .drain
      } yield ()
    }
  }

  def startAndMonitorRuntime(googleProject: GoogleProject, runtimeName: RuntimeName, checkJupyterSetup: Boolean)(
    implicit
    token: AuthToken,
    authorization: IO[Authorization]
  ): Unit = {
    // verify with get()
    val waitForRunning = LeonardoApiClient.client.use { implicit c =>
      LeonardoApiClient.startRuntimeWithWait(googleProject, runtimeName)
    }

    waitForRunning.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    // TODO: PR comment: we probably don't need to hit the proxy every time we monitor starting, only one test around life cycle should handle
    logger.info(s"Checking if ${googleProject.value}/${runtimeName.asString} is proxyable yet")
    val getResult = Try(Notebook.getApi(googleProject, runtimeName))
    getResult.isSuccess shouldBe true
    getResult.get should not include "ProxyException"

    // TODO: PR comment, we don't want to be using selenium to run notebook code as part of verifying a runtime is started, the proxy check should be sufficient

    // Grab the jupyter.log and welder.log files for debugging.
    saveClusterLogFiles(googleProject, runtimeName, List("jupyter.log", "welder.log"), "start")
  }
}
