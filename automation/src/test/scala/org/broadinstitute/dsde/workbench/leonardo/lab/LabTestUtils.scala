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

  def withLabLauncherPage[T](cluster: Cluster)(testCode: LabLauncherPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    val labLauncherPage = lab.Lab.get(cluster.googleProject, cluster.clusterName)
    testCode(labLauncherPage.open)
  }

  def withNewLabNotebook[T](cluster: Cluster, kernel: LabKernel = lab.Python2, timeout: FiniteDuration = 2.minutes)(testCode: LabNotebookPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    withLabLauncherPage(cluster) { labLauncherPage =>
      val result: Future[T] = retryUntilSuccessOrTimeout(whenKernelNotReady, failureLogMessage = s"Cannot make new notebook")(30 seconds, 5 minutes) {() =>
        Future(labLauncherPage.withNewLabNotebook(kernel, timeout) { labNotebookPage =>
          testCode(labNotebookPage)
        })
      }
      Await.result(result, 5 minutes)
    }
  }
}
