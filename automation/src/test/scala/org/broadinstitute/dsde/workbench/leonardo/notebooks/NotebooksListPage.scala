package org.broadinstitute.dsde.workbench.leonardo.notebooks

import java.io.File

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.openqa.selenium.WebDriver

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.util.Try


sealed trait NotebookKernel {
  def string: String
  def cssSelectorString: String = "ul#new-menu > li[id] > a"
}

case object Python2 extends NotebookKernel {
  def string: String = "Python 2"
  override def cssSelectorString: String = super.cssSelectorString + "[title='Create a new notebook with Python 2']"
}

case object Python3 extends NotebookKernel {
  def string: String = "Python 3"
  override def cssSelectorString: String = super.cssSelectorString + "[title='Create a new notebook with Python 3']"
}

case object PySpark2 extends NotebookKernel {
  def string: String = "PySpark 2"
  override def cssSelectorString: String = super.cssSelectorString + "[title='Create a new notebook with PySpark 2']"
}

case object PySpark3 extends NotebookKernel {
  def string: String = "PySpark 3"
  override def cssSelectorString: String = super.cssSelectorString + "[title='Create a new notebook with PySpark 3']"
}

case object RKernel extends NotebookKernel {
  def string: String = "R"
  override def cssSelectorString: String = super.cssSelectorString + "[title='Create a new notebook with R']"
}

class NotebooksListPage(override val url: String)(override implicit val authToken: AuthToken, override implicit val webDriver: WebDriver)
  extends JupyterPage {

  override def open(implicit webDriver: WebDriver): NotebooksListPage = super.open.asInstanceOf[NotebooksListPage]

  val uploadNewButton: Query = cssSelector("[title='Click to browse for a file to upload.']")
  val finishUploadButton: Query = cssSelector("[class='btn btn-primary btn-xs upload_button']")
  val newButton: Query = cssSelector("[id='new-buttons']")

  def upload(file: File): Unit = {
    uploadNewButton.findElement.get.underlying.sendKeys(file.getAbsolutePath)
    click on (await enabled finishUploadButton)
  }

  def withOpenNotebook[T](file: File, timeout: FiniteDuration = 2.minutes)(testCode: NotebookPage => T): T = {
    storeInBrowser(Map("fileExists" -> file.getPath))
    await enabled (text(file.getPath), timeout.toSeconds)
    val notebookPage = new NotebookPage(url + "/notebooks/" + file.getPath).open
    notebookPage.awaitReadyKernel(timeout)
    val result = Try { testCode(notebookPage) }
//    notebookPage.shutdownKernel()
    result.get
  }

  def withNewNotebook[T](kernel: NotebookKernel = Python2, timeout: FiniteDuration = 2.minutes)(testCode: NotebookPage => T): T = {
    switchToNewTab {
      await visible (newButton, timeout.toSeconds)
      click on newButton
      await visible (cssSelector(kernel.cssSelectorString), timeout.toSeconds)
      click on cssSelector(kernel.cssSelectorString)
    }
    // Not calling NotebookPage.open() as it should already be opened
    val notebookPage = new NotebookPage(currentUrl)
    notebookPage.awaitReadyKernel(timeout)
    val result = Try { testCode(notebookPage) }
    notebookPage.shutdownKernel()
    result.get
  }

}
