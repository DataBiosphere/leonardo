package org.broadinstitute.dsde.workbench.leonardo.rstudio

import org.broadinstitute.dsde.workbench.auth.AuthToken
import scala.concurrent.duration._
import org.broadinstitute.dsde.workbench.page.ProxyRedirectPage
import org.openqa.selenium.{Keys, WebDriver}

import scala.util.Try

class RStudioPage(override val url: String)(implicit
  val webDriver: WebDriver,
  override val authToken: AuthToken
) extends ProxyRedirectPage[RStudioPage] {

  val renderedApp: Query = cssSelector("[id='rstudio_rstudio_logo']")

  val rstudioContainer: Query = cssSelector("[id='rstudio_container']")

  val popupPanel: Query = cssSelector("[class*='themedPopupPanel']")

  override def awaitLoaded(): RStudioPage = {
    await enabled renderedApp
    this
  }

  def checkGlobalVariable(variable: String): Query = cssSelector(s"[title~='${variable}']")

  def withNewRStudio[T](timeout: FiniteDuration = 2.minutes)(testCode: RStudioPage => T): T = {

    val rstudioPage = new RStudioPage(currentUrl)
    val result = Try(testCode(rstudioPage))
    result.get
  }

  def variableExists(variable: String): Boolean =
    find(checkGlobalVariable(variable)).isDefined

  def dismissPopupPanel(): Unit = {
    Thread.sleep(5000)
    // Press ESC if the popup panel is present to dismiss it
    if (find(popupPanel).isDefined) {
      pressKeys(Keys.ESCAPE.toString)
      await notVisible popupPanel
    }
  }
}
