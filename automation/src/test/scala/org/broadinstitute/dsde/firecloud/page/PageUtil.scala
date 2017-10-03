package org.broadinstitute.dsde.firecloud.page

import org.openqa.selenium.WebDriver
import org.scalatest.selenium.Page
import org.scalatest.selenium.WebBrowser.go

/**
  * Mix-in utilities for ScalaTest's Page.
  */
trait PageUtil[P <: Page] { self: P =>

  /**
    * Sends the browser to the URL for this Page object. Returns the page
    * object to facilitate call chaining.
    *
    * @param webDriver implicit WebDriver for the WebDriverWait
    * @return the Page object
    */
  def open(implicit webDriver: WebDriver): P = {
    go to this
    awaitLoaded()
  }

  /**
    * Override this to wait for something on the page that shows up after it
    * has been loaded. Returns the page object to facilitate call chaining.
    */
  def awaitLoaded(): P = { this }

}
