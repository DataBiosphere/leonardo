package org.broadinstitute.dsde.firecloud.page

import org.openqa.selenium.WebDriver

abstract class OKCancelModal(implicit webDriver: WebDriver) extends FireCloudView {

  def clickOk()(implicit webDriver: WebDriver): Unit = {
    ui.clickOkButton()
  }

  def clickCancel()(implicit webDriver: WebDriver): Unit = {
    ui.clickCancelButton()
  }

  object ui {
    private val okButton: Query = testId("ok-button")
    private val cancelButton: Query = testId("cancel-button")
    def clickOkButton(): Unit = {
      click on (await enabled okButton)
    }
    def clickCancelButton(): Unit = {
      click on (await enabled cancelButton)
    }
  }
}

case class ErrorModal(implicit webDriver: WebDriver) extends OKCancelModal {
  def validateLocation(implicit webDriver: WebDriver): Boolean = {
    testId("error-modal").element != null
  }

  def getErrorText(): String = {
    readText(testId("message-modal-content"))
  }

}

case class MessageModal(implicit webDriver: WebDriver) extends OKCancelModal {
  def validateLocation: Boolean = {
    testId("message-modal").element != null
  }
}

