package org.broadinstitute.dsde.workbench.leonardo

import java.io.File

import org.broadinstitute.dsde.workbench.config.AuthToken
import org.openqa.selenium.WebDriver

class NotebooksListPage(override val url: String)(override implicit val authToken: AuthToken, override implicit val webDriver: WebDriver)
  extends JupyterPage {

  override def open(implicit webDriver: WebDriver): NotebooksListPage = super.open.asInstanceOf[NotebooksListPage]

  val uploadNewButton: Query = cssSelector("[title='Click to browse for a file to upload.']")
  val finishUploadButton: Query = cssSelector("[class='btn btn-primary btn-xs upload_button']")
  val newButton: Query = cssSelector("[id='new-buttons']")
  val python2Link: Query = cssSelector("[title='Create a new notebook with Python 2']")

  def upload(file: File): Unit = {
    uploadNewButton.findElement.get.underlying.sendKeys(file.getAbsolutePath)
    click on (await enabled finishUploadButton)
  }

  def openNotebook(file: File): NotebookPage = {
    await enabled text(file.getName)
    new NotebookPage(url + "/notebooks/" + file.getName).open
  }

  def newNotebook: NotebookPage = {
    click on newButton
    click on (await enabled python2Link)
    Thread.sleep(5000)
    new NotebookPage(url + "/notebooks/Untitled.ipynb").open
  }
}