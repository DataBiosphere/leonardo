package org.broadinstitute.dsde.workbench.leonardo.rstudio

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.openqa.selenium.WebDriver
import org.broadinstitute.dsde.workbench.leonardo.notebooks.{JupyterPage, NotebooksListPage}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.util.Try

class RStudioPage(override val url: String)(implicit override val authToken: AuthToken,
                                            implicit override val webDriver: WebDriver)
    extends JupyterPage
    with LazyLogging {

  override def open(implicit webDriver: WebDriver): RStudioPage =
    super.open.asInstanceOf[RStudioPage]

  override val renderedApp: Query = cssSelector("[id='rstudio_rstudio_logo']")

  override def awaitLoaded(): JupyterPage = {
    await enabled renderedApp
    this
  }

  def checkGlobalVariable(variable: String): Query = cssSelector(s"[title~='${variable}']")

  def withNewRStudio[T](timeout: FiniteDuration = 2.minutes)(testCode: RStudioPage => T): T = {

    // Not calling NotebookPage.open() as it should already be opened
    val rstudioPage = new RStudioPage(currentUrl)
    val result = Try(testCode(rstudioPage))
    result.get
  }

  def variableExists(variable: String): Boolean =
    find(checkGlobalVariable(variable)).size > 0

}
