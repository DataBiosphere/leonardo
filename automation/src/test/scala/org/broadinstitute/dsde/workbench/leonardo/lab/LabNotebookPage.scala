package org.broadinstitute.dsde.workbench.leonardo.lab

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.text.StringEscapeUtils
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.scalatest.Matchers.convertToAnyShouldWrapper
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout}
import org.scalatest.exceptions.TestFailedDueToTimeoutException
import org.scalatest.time.{Seconds, Span}
import org.broadinstitute.dsde.workbench.leonardo.KernelNotReadyException
import org.openqa.selenium.{By, TimeoutException, WebDriver, WebElement}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}


class LabNotebookPage(override val url: String)(override implicit val authToken: AuthToken, override implicit val webDriver: WebDriver)
  extends LabPage with Toolbar with NotebookCell with Eventually with LazyLogging {


  override def open(implicit webDriver: WebDriver): LabNotebookPage = {
    super.open.asInstanceOf[LabNotebookPage]
  }

  // menu elements

  lazy val fileMenu: Element = {
    findAll(cssSelector(menus)).filter { e => e.text == "File" }.toList.head
  }

  lazy val runMenu: Element = {
    findAll(cssSelector(menus)).filter { e => e.text == "Run" }.toList.head
  }

  lazy val kernelMenu: Element = {
    findAll(cssSelector(menus)).filter { e => e.text == "Kernel" }.toList.head
  }

  // is at least one cell currently executing?
  def cellsAreRunning: Boolean = {
    findAll(prompts).exists { e => e.text == "In [*]:" }
  }

  // has the specified cell completed execution? Cell number will appear in prompt once execution is complete
  def cellIsRendered(cellNumber: Int): Boolean = {
    findAll(cssSelector(prompts)).exists { e => e.text == s"[$cellNumber]:"}
  }

  lazy val cells: Query = cssSelector(cellSelector)

  def lastCell: WebElement = {
    webDriver.findElements(cells.by).asScala.toList.last
  }

  def firstCell: WebElement = {
    webDriver.findElements(cells.by).asScala.toList.head
  }

  def numCellsOnPage: Int = {
    webDriver.findElements(cells.by).asScala.toList.length
  }

  def cellOutput(cell: WebElement): Option[String] = {
    val outputs = cell.findElements(By.cssSelector(cellOutputSelector))
    outputs.asScala.headOption.map(_.getText)
  }


  def shutdownKernel(): Unit = {
    click on kernelMenu
    click on (await enabled cssSelector(shutdownKernelSelection))
    await condition isKernelDisconnected
  }

  def clickRunCell(timeout: FiniteDuration = 2.minutes): Unit = {
    click on cssSelector(runCellButton)
    awaitReadyKernel(timeout)
  }

  def awaitReadyKernel(timeout: FiniteDuration): Unit = {
    val time = Timeout(scaled(Span(timeout.toSeconds, Seconds)))
    val pollInterval = Interval(scaled(Span(5, Seconds)))
    try {
      val t0 = System.nanoTime()

      eventually(time, pollInterval) {
        val ready = !cellsAreRunning && isKernelReady
        ready shouldBe true
      }

      val t1 = System.nanoTime()
      val timediff = FiniteDuration(t1 - t0, NANOSECONDS)

      logger.info(s"kernel was ready after ${timediff.toSeconds} seconds. Timeout was ${timeout.toSeconds}")
    } catch {
      case _: TestFailedDueToTimeoutException => throw KernelNotReadyException(time)
    }
  }

  def isKernelDisconnected: Boolean = {
    find(className(kernelStatus)).exists(_.underlying.getAttribute("title") == "Kernel Dead")
  }

  def isKernelReady: Boolean = {
    find(className(kernelStatus)).exists(_.underlying.getAttribute("title") == "Kernel Idle")
  }

  def runCodeInEmptyCell(code: String, timeout: FiniteDuration = 1 minute): Option[String] = {
    Try {
      await enabled cells
    } match {
      case Success(_) => ()
      case Failure(_) => throw new NoSuchElementException(s"Failed to find Notebook cell. css = [${cells.by}]")
    }

    val cell = lastCell
    val cellNumber = numCellsOnPage
    click on cell.findElement(By.cssSelector(".CodeMirror"))
    val jsEscapedCode = StringEscapeUtils.escapeEcmaScript(code)
    executeScript(s"""arguments[0].CodeMirror.setValue("$jsEscapedCode");""", cell.findElement(By.cssSelector(".CodeMirror")))

    clickRunCell(timeout)

    try {
      await condition(cellIsRendered(cellNumber), timeout.toSeconds)
    } catch {
      case e: Exception => throw new TimeoutException(s"cellIsRendered($cellNumber) failed.", e)
    }

    cellOutput(cell)
  }

}
