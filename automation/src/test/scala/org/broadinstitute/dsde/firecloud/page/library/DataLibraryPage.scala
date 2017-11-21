package org.broadinstitute.dsde.firecloud.page.library

import org.broadinstitute.dsde.firecloud.config.FireCloudConfig
import org.broadinstitute.dsde.firecloud.page.AuthenticatedPage
import org.broadinstitute.dsde.workbench.page.PageUtil
import org.openqa.selenium.WebDriver
import org.scalatest.selenium.Page

/**
  * Page class for the Data Library page.
  */
class DataLibraryPage(implicit webDriver: WebDriver) extends AuthenticatedPage with Page with PageUtil[DataLibraryPage] {
  override val url: String = s"${FireCloudConfig.FireCloud.baseUrl}#library"

  override def awaitLoaded(): DataLibraryPage = {
    await text "Data Use Limitation"
    this
  }

  def validateLocation(): Unit = {
    // TODO: Use something more reliable to validate that the browser is on the right page.
    await text "Matching Cohorts"
  }

  trait UI extends super.UI {
    private def datasetTestId(n: String) = { s"dataset-$n" }
    def hasDataset(name: String): Boolean = {
      find(testId(datasetTestId(name))).isDefined
    }
  }
  object ui extends UI
}
