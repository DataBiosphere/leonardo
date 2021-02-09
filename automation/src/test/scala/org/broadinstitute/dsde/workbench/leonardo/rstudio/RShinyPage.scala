package org.broadinstitute.dsde.workbench.leonardo.rstudio

import org.broadinstitute.dsde.workbench.service.test.WebBrowserUtil
import org.openqa.selenium.WebDriver

class RShinyPage(implicit val webDriver: WebDriver) extends WebBrowserUtil {
  val header: Query = cssSelector("h2")

  def hasHeaderText(text: String): Boolean =
    find(header).exists(_.text == text)

  // TODO add methods to interact with shiny apps in other ways
}

case class RShinyLaunchException(command: String)
    extends Exception(s"RShiny app failed to launch with command '$command''")
