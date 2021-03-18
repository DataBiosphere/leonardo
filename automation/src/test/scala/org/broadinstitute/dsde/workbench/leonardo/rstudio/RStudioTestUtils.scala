package org.broadinstitute.dsde.workbench.leonardo.rstudio

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.notebooks.ProxyRedirectClient
import org.openqa.selenium.WebDriver
import org.scalatest.Suite
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Try

trait RStudioTestUtils extends LeonardoTestUtils {
  this: Suite =>

  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  def withRStudioPage[T](cluster: ClusterCopy)(testCode: RStudioPage => T)(implicit webDriver: WebDriver,
                                                                           token: AuthToken): T = {
    val rstudioMainPage = RStudio.get(cluster.googleProject, cluster.clusterName)
    logger.info(s"rstudio ${rstudioMainPage.url}")
    val bindingFuture = ProxyRedirectClient.startServer
    val testResult = Try {
      val proxyRedirectPage = ProxyRedirectClient.get(cluster.googleProject, cluster.clusterName, "rstudio")
      proxyRedirectPage.open
      testCode(rstudioMainPage)
    }

    ProxyRedirectClient.stopServer(bindingFuture)
    testResult.get
  }

  def withNewRStudio[T](cluster: ClusterCopy, timeout: FiniteDuration = 5.minutes)(
    testCode: RStudioPage => T
  )(implicit webDriver: WebDriver, token: AuthToken): T =
    withRStudioPage(cluster) { rstudioMainPage =>
      logger.info(
        s"Creating new rstudio on cluster ${cluster.googleProject.value} / ${cluster.clusterName.asString}..."
      )
      rstudioMainPage.withNewRStudio()(rstudioPage => testCode(rstudioPage))
    }

}
