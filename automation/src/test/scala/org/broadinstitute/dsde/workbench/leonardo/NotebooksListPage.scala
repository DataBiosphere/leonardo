package org.broadinstitute.dsde.workbench.leonardo

import java.io.File

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.openqa.selenium.WebDriver

import scala.util.Try

sealed trait Kernel {
  def string: String
  def cssSelectorString: String
}

case object Python2 extends Kernel {
  def string: String = "Python 2"
  def cssSelectorString: String = "[title='Create a new notebook with Python 2']"

}

case object Python3 extends Kernel {
  def string: String = "Python 3"
  def cssSelectorString: String = "[title='Create a new notebook with Python 3']"
}

case object RKernel extends Kernel {
  def string: String = "R"
  def cssSelectorString: String = "[title='Create a new notebook with R']"
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

  def withOpenNotebook[T](file: File)(testCode: NotebookPage => T): T = {
    await enabled text(file.getName)
    val notebookPage = new NotebookPage(url + "/notebooks/" + file.getName).open
    val result = Try { testCode(notebookPage) }
    notebookPage.shutdownKernel()
    result.get
  }

  def withNewNotebook[T](kernel: Kernel = Python2)(testCode: NotebookPage => T): T = {
    switchToNewTab {
      click on (await enabled newButton)
      click on (await enabled cssSelector(kernel.cssSelectorString))
    }
    // Not calling NotebookPage.open() as it should already be opened
    val notebookPage = new NotebookPage(currentUrl)
    val result = Try { testCode(notebookPage) }
    notebookPage.shutdownKernel()
    result.get
  }

}
