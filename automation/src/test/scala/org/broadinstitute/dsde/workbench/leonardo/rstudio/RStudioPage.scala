package org.broadinstitute.dsde.workbench.leonardo.rstudio

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.openqa.selenium.WebDriver
import org.broadinstitute.dsde.workbench.leonardo.notebooks.{JupyterPage, NotebooksListPage}

class RStudioPage(override val url: String)(implicit override val authToken: AuthToken,
                                            implicit override val webDriver: WebDriver)
    extends JupyterPage
    with LazyLogging {

  override def open(implicit webDriver: WebDriver): RStudioPage =
    super.open.asInstanceOf[RStudioPage]

  def checkGlobalVariable(variable: String): Query = cssSelector(s"[title~='${variable}']")

  def variableExists(variable: String): Boolean =
    find(checkGlobalVariable(variable)).size > 0

}
