package org.broadinstitute.dsde.workbench.leonardo.rstudio

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo._
import org.openqa.selenium.WebDriver
import org.scalatest.TestSuite

import scala.concurrent._
import scala.concurrent.duration._

trait RStudioTestUtils extends LeonardoTestUtils {
  this: TestSuite =>

  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  def withRStudioPage[T](
    cluster: ClusterCopy
  )(testCode: RStudioPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    val rstudioMainPage = RStudio.get(cluster.googleProject, cluster.clusterName)
    testCode(rstudioMainPage.open)
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
