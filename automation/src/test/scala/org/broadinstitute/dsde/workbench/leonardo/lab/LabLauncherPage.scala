package org.broadinstitute.dsde.workbench.leonardo.lab

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.openqa.selenium.WebDriver

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

sealed trait LabKernel {
  def string: String
  def cssSelectorString: String
}

case object Python2 extends LabKernel {
  def string: String = "Python 2"
  override def cssSelectorString: String = "[title='Python 2'][data-category='Notebook']"
}

case object Python3 extends LabKernel {
  def string: String = "Python 3"
  override def cssSelectorString: String = "[title='Python 3'][data-category='Notebook']"
}

case object PySpark2 extends LabKernel {
  def string: String = "PySpark 2"
  override def cssSelectorString: String = "[title='PySpark 2'][data-category='Notebook']"
}

case object PySpark3 extends LabKernel {
  def string: String = "PySpark 3"
  override def cssSelectorString: String = "[title='PySpark 3'][data-category='Notebook']"
}

case object RKernel extends LabKernel {
  def string: String = "R"
  override def cssSelectorString: String = "[title='R'][data-category='Notebook']"
}


class LabLauncherPage(override val url: String)(override implicit val authToken: AuthToken, override implicit val webDriver: WebDriver)
  extends LabPage {

  override def open(implicit webDriver: WebDriver): LabLauncherPage = super.open.asInstanceOf[LabLauncherPage]


  def withNewNotebook[T](kernel: LabKernel = Python3, timeout: FiniteDuration = 2.minutes)(testCode: LabNotebookPage => T): T = {
    await notVisible (cssSelector("#main-logo"))
    await visible (cssSelector(kernel.cssSelectorString), timeout.toSeconds)
    click on cssSelector(kernel.cssSelectorString)
    // Not calling NotebookPage.open() as it should already be opened
    val labNotebookPage = new LabNotebookPage(currentUrl)
    labNotebookPage.awaitReadyKernel(timeout)
    val result = Try {
      testCode(labNotebookPage)
    } match {
      case Failure(f) => throw f
      case Success(value) => value
    }
    labNotebookPage.shutdownKernel()
    result
  }
}