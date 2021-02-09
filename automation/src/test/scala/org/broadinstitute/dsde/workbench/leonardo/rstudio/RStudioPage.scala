package org.broadinstitute.dsde.workbench.leonardo.rstudio

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.notebooks.JupyterPage
import org.openqa.selenium.{Keys, WebDriver}

import scala.concurrent.duration.{FiniteDuration, _}
import scala.jdk.CollectionConverters._
import scala.util.Try

class RStudioPage(override val url: String)(implicit override val authToken: AuthToken,
                                            implicit override val webDriver: WebDriver)
    extends JupyterPage
    with LazyLogging {

  override def open(implicit webDriver: WebDriver): RStudioPage =
    super.open.asInstanceOf[RStudioPage]

  override val renderedApp: Query = cssSelector("[id='rstudio_rstudio_logo']")

  val rstudioContainer: Query = cssSelector("[id='rstudio_container']")

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

  // Opens an example app from the shiny package.
  // Valid examples are:
  //   "01_hello", "02_text", "03_reactivity", "04_mpg", "05_sliders", "06_tabsets",
  //   "07_widgets", "08_html", "09_upload", "10_download", "11_timer"
  def withRShinyExample[T](exampleName: String)(testCode: RShinyPage => T): T =
    withRShiny(s"""runExample("$exampleName")""")(testCode)

  // Opens a shiny app from a specified directory.
  // For example:
  //   "/usr/local/lib/R/site-library/shiny/examples/01_hello"
  def withRShinyApp[T](appDir: String)(testCode: RShinyPage => T): T =
    withRShiny(s"""runApp("$appDir")""")(testCode)

  private def withRShiny[T](launchCommand: String)(testCode: RShinyPage => T): T = {
    // Get the original window handle
    val winHandleBefore = webDriver.getWindowHandle

    // Enter commands to launch the shiny app
    pressKeys(s"shiny::$launchCommand")
    pressKeys(Keys.ENTER.toString)

    // Switch to the pop-up window
    webDriver.getWindowHandles.asScala.filterNot(_ == winHandleBefore).headOption match {
      case Some(newHandle) =>
        switch to window(newHandle)
      case None =>
        throw RShinyLaunchException(launchCommand)
    }

    // Do verifications
    val result = Try(testCode(new RShinyPage))

    // Close the pop-up window and switch back to the original window
    close()
    switch to window(winHandleBefore)

    result.get
  }
}
