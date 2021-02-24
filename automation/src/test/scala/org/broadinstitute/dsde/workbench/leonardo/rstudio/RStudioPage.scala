package org.broadinstitute.dsde.workbench.leonardo.rstudio

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.page.CookieAuthedPage
import org.openqa.selenium.{Keys, WebDriver}

import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.Try

class RStudioPage(override val url: String)(implicit override val authToken: AuthToken,
                                            implicit val webDriver: WebDriver)
    extends CookieAuthedPage[RStudioPage]
    with LazyLogging {

  override def open(implicit webDriver: WebDriver): RStudioPage =
    super.open.asInstanceOf[RStudioPage]

  val renderedApp: Query = cssSelector("[id='rstudio_rstudio_logo']")

  val rstudioContainer: Query = cssSelector("[id='rstudio_container']")

  override def awaitLoaded(): RStudioPage = {
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

  // Opens an example app from the shiny package.
  // Valid examples are:
  //   "01_hello", "02_text", "03_reactivity", "04_mpg", "05_sliders", "06_tabsets",
  //   "07_widgets", "08_html", "09_upload", "10_download", "11_timer"
  def withRShinyExample[T](exampleName: String)(testCode: RShinyPage => T): T = {
    // Enter commands to launch the shiny app
    switchToNewTab {
      val launchCommand = s"""shiny::runExample("$exampleName", launch.browser = T)"""

      pressKeys(launchCommand)

      await notVisible cssSelector("[class*='themedPopupPanel']")

      pressKeys(Keys.ENTER.toString)
      await condition windowHandles.size == 2
    }

    // Do verifications
    val rshinyPage = new RShinyPage(currentUrl).awaitLoaded
    val result = Try(testCode(rshinyPage))
    result.get
  }
}
