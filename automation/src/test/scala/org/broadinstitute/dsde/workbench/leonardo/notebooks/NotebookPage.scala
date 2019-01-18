package org.broadinstitute.dsde.workbench.leonardo.notebooks

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.text.StringEscapeUtils
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.openqa.selenium.interactions.Actions
import org.openqa.selenium.{By, WebDriver, WebElement}
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout}
import org.scalatest.exceptions.TestFailedDueToTimeoutException
import org.scalatest.time.{Seconds, Span}

import org.broadinstitute.dsde.workbench.leonardo.KernelNotReadyException


import scala.collection.JavaConverters._
import org.scalatest.Matchers.convertToAnyShouldWrapper

import scala.concurrent.duration._

class NotebookPage(override val url: String)(override implicit val authToken: AuthToken, override implicit val webDriver: WebDriver)
  extends JupyterPage with Eventually with LazyLogging {

  override def open(implicit webDriver: WebDriver): NotebookPage = {
    super.open.asInstanceOf[NotebookPage]
  }

  // selects all menus from the header bar
  lazy val menus: Query = cssSelector("[class='dropdown-toggle']")

  // menu elements

  lazy val fileMenu: Element = {
    findAll(menus).filter { e => e.text == "File" }.toList.head
  }

  lazy val cellMenu: Element = {
    findAll(menus).filter { e => e.text == "Cell" }.toList.head
  }

  lazy val kernelMenu: Element = {
    findAll(menus).filter { e => e.text == "Kernel" }.toList.head
  }

  // selects all submenus which appear in dropdowns after clicking a main menu header
  lazy val submenus: Query = cssSelector("[class='dropdown-submenu']")

  // File -> Download as
  lazy val downloadSubMenu: Element = {
    findAll(submenus).filter { e => e.text == "Download as" }.toList.head
  }

  // Cell -> Cell type
  lazy val cellTypeSubMenu: Element = {
    findAll(submenus).filter { e => e.text == "Cell Type" }.toList.head
  }
  // File -> Download as -> ipynb
  lazy val downloadSelectionAsIpynb: Query = cssSelector("[id='download_ipynb']")

  // File -> Download as -> pdf
  lazy val downloadSelectionAsPdf: Query = cssSelector("[id='download_pdf']")

  // File -> Save and Checkpoint
  lazy val saveAndCheckpointSelection: Query = cssSelector("[id='save_checkpoint']")

  // Cell -> Run All Cells
  lazy val runAllCellsSelection: Query = cssSelector("[id='run_all_cells']")

  // Run Cell toolbar button
  lazy val runCellButton: Query = cssSelector("[title='Run']")

  // Kernel -> Shutdown
  lazy val shutdownKernelSelection: Query = cssSelector("[id='shutdown_kernel']")

  // Kernel -> Restart
  lazy val restartKernelSelection: Query = cssSelector("[id='restart_kernel']")

  // Jupyter asks: Are you sure you want to shutdown the kernel?
  lazy val shutdownKernelConfirmationSelection: Query = cssSelector("[class='btn btn-default btn-sm btn-danger']")

  // Jupyter asks: Are you sure you want to restart the kernel?
  lazy val restartKernelConfirmationSelection: Query = cssSelector("[class='btn btn-default btn-sm btn-danger']")

  // selects the numbered left-side cell prompts
  lazy val prompts: Query = cssSelector("[class='prompt input_prompt']")

  lazy val toMarkdownCell:Query = cssSelector("[title='Markdown']")

  lazy val translateCell: Query = cssSelector("[title='Translate current cell']")

  // is at least one cell currently executing?
  def cellsAreRunning: Boolean = {
    findAll(prompts).exists { e => e.text == "In [*]:" }
  }

  // has the specified cell completed execution?
  // since we execute cells one by one, the Nth cell (counting from 1) will also have Cell Number N
  def cellIsRendered(cellNumber: Int): Boolean = {
    findAll(prompts).exists { e => e.text == s"In [$cellNumber]:" }
  }

  lazy val kernelNotification: Query = cssSelector("[id='notification_kernel']")

  // can we see that the kernel connection has terminated?
  def isKernelShutdown: Boolean = {
    find(kernelNotification).exists { e => e.text == "No kernel" }
  }

  def runAllCells(timeout: FiniteDuration = 60 seconds): Unit = {
    click on cellMenu
    click on (await enabled runAllCellsSelection)
    awaitReadyKernel(timeout)
  }

  def downloadAsIpynb(): Unit = {
    click on fileMenu
    // TODO move to WebBrowser in automation lib so we can instead do:
    // hover over downloadSubMenu
    new Actions(webDriver).moveToElement(downloadSubMenu.underlying).perform()
    click on (await enabled downloadSelectionAsIpynb)
  }

  def downloadAsPdf(): Unit = {
    click on fileMenu
    // TODO move to WebBrowser in automation lib so we can instead do:
    // hover over downloadSubMenu
    new Actions(webDriver).moveToElement(downloadSubMenu.underlying).perform()
    click on (await enabled downloadSelectionAsPdf)
  }

  def saveAndCheckpoint(): Unit = {
    click on fileMenu
    click on (await enabled saveAndCheckpointSelection)
  }

  lazy val cells: Query = cssSelector(".CodeMirror")

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
    val outputs = cell.findElements(By.xpath("../../../..//div[contains(@class,'output_subarea')]"))
    outputs.asScala.headOption.map(_.getText)
  }

  def executeCell(code: String, timeout: FiniteDuration = 1 minute): Option[String] = {
    await enabled cells
    val cell = lastCell
    val cellNumber = numCellsOnPage
    click on cell
    val jsEscapedCode = StringEscapeUtils.escapeEcmaScript(code)
    executeScript(s"""arguments[0].CodeMirror.setValue("$jsEscapedCode");""", cell)
    clickRunCell(timeout)
    await condition (cellIsRendered(cellNumber), timeout.toSeconds)
    cellOutput(cell)
  }

  def translateMarkup(code: String, timeout: FiniteDuration = 1 minute): String = {
    await enabled cells
    await enabled translateCell
    val inputCell = lastCell
    val jsEscapedCode = StringEscapeUtils.escapeEcmaScript(code)
    executeScript(s"""arguments[0].CodeMirror.setValue("$jsEscapedCode");""", inputCell)
    changeCodeToMarkdown
    await enabled cells
    click on translateCell
    Thread.sleep(3000) // To fix, this is not good
    val outputCell = lastCell
    outputCell.getText
  }

  private def changeCodeToMarkdown(): Unit = {
    click on cellMenu
    new Actions(webDriver).moveToElement(cellTypeSubMenu.underlying).perform()
    await visible translateCell
  }

  def shutdownKernel(): Unit = {
    click on kernelMenu
    click on (await enabled shutdownKernelSelection)
    click on (await enabled shutdownKernelConfirmationSelection)
    await condition isKernelShutdown
  }

  /**
    * Throw TimeoutException if Kernel is not ready after restart when timeout is reached
    *
    * @param timeout
    */
  def restartKernel(timeout: FiniteDuration = 1 minute): Unit = {
    logger.info("restarting kernel ...")
    click on kernelMenu
    click on (await enabled restartKernelSelection)
    click on (await enabled restartKernelConfirmationSelection)
    await notVisible restartKernelConfirmationSelection
    await condition (isKernelReady && kernelNotificationText == "none", timeout.toSeconds)
  }

  def clickRunCell(timeout: FiniteDuration = 2.minutes): Unit = {
    click on runCellButton
    awaitReadyKernel(timeout)
  }

  def awaitReadyKernel(timeout: FiniteDuration): Unit = {
    val time = Timeout(scaled(Span(timeout.toSeconds, Seconds)))
    val pollInterval = Interval(scaled(Span(5, Seconds)))
    try {
      val t0 = System.nanoTime()

      eventually(time, pollInterval) {
        val ready = (!cellsAreRunning && isKernelReady && kernelNotificationText == "none")
        ready shouldBe true
      }

      val t1 = System.nanoTime()
      val timediff = FiniteDuration(t1 - t0, NANOSECONDS)

      logger.info(s"kernel was ready after ${timediff.toSeconds} seconds. Timeout was ${timeout.toSeconds}")
    } catch {
      case e: TestFailedDueToTimeoutException => throw KernelNotReadyException(time)
    }
  }

  def isKernelDisconnected: Boolean = {
    find(id("kernel_indicator_icon")).exists(_.underlying.getAttribute("class") == "kernel_disconnected_icon")
  }

  def isKernelReady: Boolean = {
    find(id("kernel_indicator_icon")).exists(_.underlying.getAttribute("class") == "kernel_idle_icon")
  }

  def kernelNotificationText: String = {
    find(id("notification_kernel")).map(_.underlying.getCssValue("display")).getOrElse("")
  }

}
