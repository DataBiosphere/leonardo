package org.broadinstitute.dsde.workbench.leonardo.rstudio

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.page.CookieAuthedPage
import org.openqa.selenium.WebDriver

import scala.concurrent.duration._

class RShinyPage(val url: String)(implicit override val authToken: AuthToken, implicit val webDriver: WebDriver)
    extends CookieAuthedPage[RShinyPage] {
  private val exampleHeader: Query = cssSelector("#showcase-app-container > div > h2")

  override def awaitLoaded(): RShinyPage = {
    await visible (exampleHeader, 1.minute.toSeconds)
    this
  }

  def getExampleHeader: Option[String] =
    find(exampleHeader).map(_.text)

  // TODO add methods to interact with shiny apps in other ways
}
