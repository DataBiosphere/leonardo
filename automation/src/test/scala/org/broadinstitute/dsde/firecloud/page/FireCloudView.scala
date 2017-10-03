package org.broadinstitute.dsde.firecloud.page

import org.broadinstitute.dsde.firecloud.test.WebBrowserUtil
import org.openqa.selenium.WebDriver
import org.scalatest.selenium.WebBrowser

/**
  * Convenience parent class for FireCloud pages and other views such as modal
  * dialogs.
  */
abstract class FireCloudView(implicit webDriver: WebDriver)
  extends WebBrowser with WebBrowserUtil {

  /** Query for the FireCloud Spinner component */
  val spinner = testId("spinner")

  def readText(q: Query): String = {
    find(q) map { _.text } getOrElse ""
  }
}
