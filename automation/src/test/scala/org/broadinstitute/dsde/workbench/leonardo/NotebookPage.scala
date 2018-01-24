package org.broadinstitute.dsde.workbench.leonardo

import org.apache.commons.text.StringEscapeUtils
import org.broadinstitute.dsde.workbench.config.AuthToken
import org.openqa.selenium.{By, WebDriver, WebElement}
import org.openqa.selenium.interactions.Actions

import scala.collection.JavaConverters._

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

  // File -> Download as -> ipynb
  lazy val downloadSelection: Query = cssSelector("[id='download_ipynb']")

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

  // is at least one cell currently executing?
  def cellsAreRunning: Boolean = {
    findAll(prompts).exists { e => e.text == "In [*]:" }
  }

  // can we see that the kernel connection has terminated?
  lazy val kernelTerminatedModalBody: Query = cssSelector("[class='modal-body']")

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

  lazy val cells: Query = cssSelector(".CodeMirror")

  def lastCell: WebElement = {
    webDriver.findElements(cells.by).asScala.toList.last
  }

  def cellOutput(cell: WebElement): Option[String] = {
    val outputs = cell.findElements(By.xpath("../../../..//div[contains(@class, 'output_subarea')]"))
    outputs.asScala.headOption.map(_.getText)
  }

  def executeCell(code: String, timeoutSeconds: Long = 60): Option[String] = {
    await enabled cells
    val cell = lastCell
    val jsEscapedCode = StringEscapeUtils.escapeEcmaScript(code)
    executeScript(s"""arguments[0].CodeMirror.setValue("$jsEscapedCode");""", cell)
    click on runCellButton
    await condition (!cellsAreRunning, timeoutSeconds)
    cellOutput(cell)
  }

  def shutdownKernel(): Unit = {
    click on kernelMenu
    click on (await enabled shutdownKernelSelection)
    click on (await enabled shutdownKernelConfirmationSelection)
    await enabled kernelTerminatedModalBody
  }
}