package org.broadinstitute.dsde.firecloud.page.workspaces.monitor

import org.broadinstitute.dsde.firecloud.config.FireCloudConfig
import org.broadinstitute.dsde.firecloud.page.workspaces.WorkspacePage
import org.broadinstitute.dsde.workbench.page.PageUtil
import org.openqa.selenium.WebDriver
import org.scalatest.selenium.Page

class SubmissionDetailsPage(namespace: String, name: String)(implicit webDriver: WebDriver) extends WorkspacePage with Page with PageUtil[SubmissionDetailsPage] {

  private val submissionId = getSubmissionId()
  override val url: String = s"${FireCloudConfig.FireCloud.baseUrl}#workspaces/$namespace/$name/monitor/$submissionId"

  private val WAITING_STATS = Array("Queued","Launching")
  private val WORKING_STATS = Array("Submitted", "Running", "Aborting")
  private val SUCCESS_STATS = Array("Succeeded")
  private val FAILED_STATS  = Array("Failed")
  private val ABORTED_STATS  = Array("Aborted")

  private val SUBMISSION_COMPLETE_STATS = Array("Done") ++ SUCCESS_STATS ++ FAILED_STATS ++ ABORTED_STATS

  def isSubmissionDone():Boolean = {
    val status = ui.getSubmissionStatus()
    (SUBMISSION_COMPLETE_STATS.contains(status))
  }

  def getSubmissionId(): String = {
    ui.getSubmissionId()
  }

  def verifyWorkflowSucceeded(): Boolean = {
    val status = ui.getWorkflowStatus()
    SUCCESS_STATS.contains(status)
  }

  def verifyWorkflowFailed(): Boolean = {
    val status = ui.getWorkflowStatus()
    FAILED_STATS.contains(status)
  }

  def verifyWorkflowAborted(): Boolean = {
    val status = ui.getWorkflowStatus()
    ABORTED_STATS.contains(status)
  }

  def waitUntilSubmissionCompletes() = {
    while (!isSubmissionDone()) {
      open
    }
  }

  def abortSubmission() = {
    ui.abortSubmission()
  }

  trait UI extends super.UI {
    private val submissionStatusQuery: Query = testId("submission-status")
    private val workflowStatusQuery: Query = testId("workflow-status")
    private val submissionIdQuery: Query = testId("submission-id")
    private val submissionAbortButtonQuery: Query = testId("submission-abort-button")
    private val submissionAbortModalConfirmButtonQuery: Query = testId("submission-abort-modal-confirm-button")

    def getSubmissionStatus(): String = {
      (await enabled submissionStatusQuery).text
    }

    //This currently only works for 1 workflow!!!
    def getWorkflowStatus(): String = {
      await enabled workflowStatusQuery
      val workflowStatusElement = find(workflowStatusQuery)
      workflowStatusElement.get.text
    }

    def getSubmissionId(): String = {
      await enabled submissionIdQuery
      val submissionIdElement = find(submissionIdQuery)
      submissionIdElement.get.text
    }

    def abortSubmission() = {
      await enabled submissionAbortButtonQuery
      click on submissionAbortButtonQuery
      click on submissionAbortModalConfirmButtonQuery
    }
  }
  object ui extends UI
}
