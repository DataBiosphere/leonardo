package org.broadinstitute.dsde.workbench.leonardo.notebooks

import org.broadinstitute.dsde.workbench.page.CookieAuthedPage
import org.openqa.selenium.WebDriver

trait JupyterPage extends CookieAuthedPage[JupyterPage] {
  implicit val webDriver: WebDriver

  val renderedApp: Query = cssSelector("[id='ipython-main-app']")

  override def awaitLoaded(): JupyterPage = {
    await enabled renderedApp
    this
  }
}
