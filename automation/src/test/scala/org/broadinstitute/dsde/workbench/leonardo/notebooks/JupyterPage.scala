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

  //Useful utility to print client-side information.
  //Will be located in the driver window's session storage.
  //To view messages stored in this fashion, enter `window.sessionStorage` into the browser console
  //Currently, the user is responsible for ensuring their strings are valid javascript strings
  def storeInBrowser(messages: Map[String, String]): Unit = {
    messages.foreach(pair => {
      val q = "\""
      val script = s"window.sessionStorage.setItem(${q + pair._1 + q},${q + pair._2 + q})"
      executeScript(script)
    })
  }

  //executes arbitrary javascript client-side
  def executeScript(script: String): Unit = {
    val executor: JavascriptExecutor = webDriver.asInstanceOf[JavascriptExecutor]
    executor.executeScript(script)
  }
}
