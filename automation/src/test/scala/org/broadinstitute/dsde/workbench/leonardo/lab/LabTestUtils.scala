package org.broadinstitute.dsde.workbench.leonardo.lab


import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo._
import org.openqa.selenium.WebDriver
import org.scalatest.Suite

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

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
        Future(labLauncherPage.withNewNotebook(kernel, timeout) { labNotebookPage =>
          testCode(labNotebookPage)
        })
      }
      Await.result(result, 10 minutes)
    }
  }

}
