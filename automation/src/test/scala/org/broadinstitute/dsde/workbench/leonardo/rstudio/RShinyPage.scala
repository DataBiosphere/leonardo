package org.broadinstitute.dsde.workbench.leonardo.rstudio

import org.broadinstitute.dsde.workbench.page.PageUtil
import org.broadinstitute.dsde.workbench.service.test.WebBrowserUtil
import org.openqa.selenium.WebDriver
import org.scalatestplus.selenium.Page

import scala.concurrent.duration._

class RShinyPage(val url: String)(implicit val webDriver: WebDriver)
    extends Page
    with PageUtil[RShinyPage]
    with WebBrowserUtil {

  private val exampleHeader: Query = cssSelector("#showcase-app-container > div > h2")

  override def awaitLoaded(): RShinyPage = {
    await visible (exampleHeader, 1.minute.toSeconds)
    this
  }

  def getExampleHeader: Option[String] =
    find(exampleHeader).map(_.text)

  // TODO add methods to interact with shiny apps in other ways
}
