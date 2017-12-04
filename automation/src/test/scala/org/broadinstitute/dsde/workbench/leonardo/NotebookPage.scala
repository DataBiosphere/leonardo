package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.config.AuthToken
import org.openqa.selenium.{WebDriver, WebElement}
import org.openqa.selenium.interactions.Actions
import scala.collection.JavaConverters._

class NotebookPage(override val url: String)(override implicit val authToken: AuthToken, override implicit val webDriver: WebDriver)
  extends JupyterPage {

  override def open(implicit webDriver: WebDriver): NotebookPage = super.open.asInstanceOf[NotebookPage]

  // selects all menus from the header bar
  lazy val menus: Query = cssSelector("[class='dropdown-toggle']")

  // "File" and "Cell" menus

  lazy val fileMenu: Element = {
    findAll(menus).filter { e => e.text == "File" }.toList.head
  }

  lazy val cellMenu: Element = {
    findAll(menus).filter { e => e.text == "Cell" }.toList.head
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

  // selects the numbered left-side cell prompts
  lazy val prompts: Query = cssSelector("[class='prompt input_prompt']")

  // is at least one cell currently executing?
  def cellsAreRunning: Boolean = {
    findAll(prompts).exists { e => e.text == "In [*]:" }
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

  lazy val cells: Query = cssSelector(".CodeMirror")
  lazy val outputs: Query = cssSelector(".output_subarea")

  def lastCell: WebElement = {
    webDriver.findElements(cells.by).asScala.toList.last
  }

  def lastOutput: Option[Element] = {
    outputs.findAllElements.toList.lastOption
  }

  def executeCell(code: String, timeoutSeconds: Long = 60): Option[String] = {
    await enabled cells
    executeScript(s"""arguments[0].CodeMirror.setValue("$code");""", lastCell)
    click on runCellButton
    await condition (!cellsAreRunning, timeoutSeconds)
    Thread.sleep(1000)
    lastOutput.map(_.text)
  }
}