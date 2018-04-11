package org.broadinstitute.dsde.workbench.leonardo

import org.apache.commons.text.StringEscapeUtils
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.openqa.selenium.{By, WebDriver, WebElement}
import org.openqa.selenium.interactions.Actions

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

class NotebookPage(override val url: String)(override implicit val authToken: AuthToken, override implicit val webDriver: WebDriver)
  extends JupyterPage {

  override def open(implicit webDriver: WebDriver): NotebookPage = super.open.asInstanceOf[NotebookPage]

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
  lazy val downloadSelection: Query = cssSelector("[id='download_ipynb']")

  // File -> Save and Checkpoint
  lazy val saveAndCheckpointSelection: Query = cssSelector("[id='save_checkpoint']")

  // Cell -> Run All Cells
  lazy val runAllCellsSelection: Query = cssSelector("[id='run_all_cells']")

  // Run Cell toolbar button
  lazy val runCellButton: Query = cssSelector("[title='Run']")

  // Kernel -> Shutdown
  lazy val shutdownKernelSelection: Query = cssSelector("[id='shutdown_kernel']")

  // Jupyter asks: Are you sure you want to shutdown the kernel?
  lazy val shutdownKernelConfirmationSelection: Query = cssSelector("[class='btn btn-default btn-sm btn-danger']")

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

  def runAllCells(timeoutSeconds: Long): Unit = {
    click on cellMenu
    click on (await enabled runAllCellsSelection)
    await condition (!cellsAreRunning, timeoutSeconds)
  }

  def download(): Unit = {
    click on fileMenu
    // TODO move to WebBrowser in automation lib so we can instead do:
    // hover over downloadSubMenu
    new Actions(webDriver).moveToElement(downloadSubMenu.underlying).perform()
    click on (await enabled downloadSelection)
  }

  def saveAndCheckpoint(): Unit = {
    click on fileMenu
    click on (await enabled saveAndCheckpointSelection)
  }

  lazy val cells: Query = cssSelector(".CodeMirror")

  def lastCell: (Int, WebElement) = {
    val asList = webDriver.findElements(cells.by).asScala.toList
    (asList.length, asList.last)
  }

  def firstCell: WebElement = {
    webDriver.findElements(cells.by).asScala.toList.head
  }

  def cellOutput(cell: WebElement): Option[String] = {
    val outputs = cell.findElements(By.xpath("../../../..//div[contains(@class, 'output_subarea')]"))
    outputs.asScala.headOption.map(_.getText)
  }
  
  def executeCell(code: String, timeout: FiniteDuration = 1 minute): Option[String] = {
    await enabled cells
    val (cellNumber, cell) = lastCell
    click on cell
    val jsEscapedCode = StringEscapeUtils.escapeEcmaScript(code)
    executeScript(s"""arguments[0].CodeMirror.setValue("$jsEscapedCode");""", cell)
    click on runCellButton
    await condition (!cellsAreRunning, timeout.toSeconds)
    await condition (cellIsRendered(cellNumber), timeout.toSeconds)
    cellOutput(cell)
  }

  def translateMarkup(code: String, timeout: FiniteDuration = 1 minute): String = {
    await enabled cells
    await enabled translateCell
    val (_, inputCell) = lastCell
    val jsEscapedCode = StringEscapeUtils.escapeEcmaScript(code)
    executeScript(s"""arguments[0].CodeMirror.setValue("$jsEscapedCode");""", inputCell)
    changeCodeToMarkdown
    await enabled cells
    click on translateCell
    Thread.sleep(3000)
    val (_, outputCell) = lastCell
    outputCell.getText
  }

  private def changeCodeToMarkdown = {
    click on cellMenu
    new Actions(webDriver).moveToElement(cellTypeSubMenu.underlying).perform()
    await visible(translateCell)
  }

  def shutdownKernel(): Unit = {
    click on kernelMenu
    click on (await enabled shutdownKernelSelection)
    click on (await enabled shutdownKernelConfirmationSelection)
    await condition isKernelShutdown
  }
}
