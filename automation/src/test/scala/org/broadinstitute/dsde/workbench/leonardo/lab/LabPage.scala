package org.broadinstitute.dsde.workbench.leonardo.lab

import org.broadinstitute.dsde.workbench.page.ProxyRedirectPage
import org.openqa.selenium.WebDriver

trait LabPage extends ProxyRedirectPage[LabPage] {
  implicit val webDriver: WebDriver

  val renderedApp: Query = cssSelector("[id='jp-top-panel']")

  override def awaitLoaded(): LabPage = {
    await enabled renderedApp
    this
  }
}
