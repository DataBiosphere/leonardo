package org.broadinstitute.dsde.workbench.leonardo.lab

import org.broadinstitute.dsde.workbench.page.CookieAuthedPage
import org.openqa.selenium.WebDriver
import org.scalatestplus.selenium.WebBrowser._

trait LabPage extends CookieAuthedPage[LabPage] {
  implicit val webDriver: WebDriver

  val renderedApp: Query = cssSelector("[id='jp-top-panel']")

  override def awaitLoaded(): LabPage = {
    await enabled renderedApp
    this
  }
}
