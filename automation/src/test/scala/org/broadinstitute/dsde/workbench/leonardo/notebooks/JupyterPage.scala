package org.broadinstitute.dsde.workbench.leonardo.notebooks

import org.broadinstitute.dsde.workbench.page.CookieAuthedPage
import org.openqa.selenium.{JavascriptExecutor, WebDriver}

trait JupyterPage extends CookieAuthedPage[JupyterPage] {
  implicit val webDriver: WebDriver

  val renderedApp: Query = cssSelector("[id='ipython-main-app']")

  override def awaitLoaded(): JupyterPage = {
    await enabled renderedApp
    this
  }

  def storeInBrowser(messages: Map[String, String]): Unit = {
    messages.foreach(pair => {
      val q = "\""
      val script = s"window.sessionStorage.setItem(${q + pair._1 + q},${q + pair._2 + q})"
      executeScript(script)
    })
  }

  def executeScript(script: String): Unit = {
    val executor: JavascriptExecutor = webDriver.asInstanceOf[JavascriptExecutor]
    executor.executeScript(script)
  }

//  def isValidEntry(entry: (String, String)): Unit = {
//    val invalidChars = ["/"]
//  }

}
