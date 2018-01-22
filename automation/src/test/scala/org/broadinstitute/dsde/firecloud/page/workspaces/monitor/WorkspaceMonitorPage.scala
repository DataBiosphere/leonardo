package org.broadinstitute.dsde.firecloud.page.workspaces.monitor

import org.broadinstitute.dsde.workbench.config.{Config => FireCloudConfig}
import org.broadinstitute.dsde.firecloud.page.workspaces.WorkspacePage
import org.broadinstitute.dsde.workbench.page.PageUtil
import org.openqa.selenium.WebDriver
import org.scalatest.selenium.Page


class WorkspaceMonitorPage(namespace: String, name: String)(implicit webDriver: WebDriver) extends WorkspacePage with Page with PageUtil[WorkspaceMonitorPage] {

  override val url: String = s"${FireCloudConfig.FireCloud.baseUrl}#workspaces/$namespace/$name/monitor"

  /**
    * Opens a submission
    */
  def openSubmission(submissionName: String): SubmissionDetailsPage = {
    ui.openSubmission(submissionName)
    new SubmissionDetailsPage(namespace, name)
  }

  def filter(searchText: String): Unit = {
    ui.filter(searchText)
  }


  trait UI extends super.UI {
    private val filterInput = testId("-input")
    private val submissionLinkId = "submission-%s"

    def filter(searchText: String) = {
      await enabled filterInput
      searchField(filterInput).value = searchText
      pressKeys("\n")
    }

    def openSubmission(submissionName: String) = {
      val linkId = submissionLinkId.format(submissionName)
      val link = testId(linkId)
      click on (await enabled link)
    }
  }
  object ui extends UI
}
