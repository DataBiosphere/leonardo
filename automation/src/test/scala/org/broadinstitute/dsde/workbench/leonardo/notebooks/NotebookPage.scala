package org.broadinstitute.dsde.workbench.leonardo.notebooks

import cats.effect.{IO, Timer}
import org.apache.commons.text.StringEscapeUtils
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.KernelNotReadyException
import org.broadinstitute.dsde.workbench.leonardo.notebooks.Notebook.NotebookMode
import org.openqa.selenium.interactions.Actions
import org.openqa.selenium.{By, WebDriver, WebElement}
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout}
import org.scalatest.exceptions.TestFailedDueToTimeoutException
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Try

class NotebookPage(override val url: String)(implicit override val webDriver: WebDriver,
                                             override val authToken: AuthToken,
                                             override val timer: Timer[IO])
    extends JupyterPage
    with Eventually {

  override def open(implicit webDriver: WebDriver): NotebookPage = super.open.asInstanceOf[NotebookPage]

  // selects all menus from the header bar
  lazy val menus: Query = cssSelector("[class='dropdown-toggle']")

  // menu elements

  lazy val fileMenu: Element = {
    findAll(menus)
      .filter(e => e.text == "File")
      .toList
      .head
  }

  lazy val cellMenu: Element = {
    findAll(menus)
      .filter(e => e.text == "Cell")
      .toList
      .head
  }

  lazy val kernelMenu: Element = {
    findAll(menus)
      .filter(e => e.text == "Kernel")
      .toList
      .head
  }

  // selects all submenus which appear in dropdowns after clicking a main menu header
  lazy val submenus: Query = cssSelector("[class='menu_focus_highlight dropdown-submenu']")

  // File -> Download as
  lazy val downloadSubMenu: Element = {
    findAll(submenus)
      .filter(e => e.text == "Download as")
      .toList
      .head
  }

  // Cell -> Cell type
  lazy val cellTypeSubMenu: Element = {
    findAll(submenus)
      .filter(e => e.text == "Cell Type")
      .toList
      .head
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
  lazy val runCellButton: Query = cssSelector("[title='run cell, select below']")

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

  lazy val toMarkdownCell: Query = cssSelector("[title='Markdown']")

  lazy val translateCell: Query = cssSelector("[title='Translate current cell']")

  // banner for edit or playground mode
  lazy val modeBanner: Query = cssSelector("[id='notification_mode']")

  lazy val kernelNotification: Query = cssSelector("[id='notification_kernel']")

  lazy val notebookNotification: Query = cssSelector("[id='notification_notebook']")

  lazy val saveButton: Query = cssSelector(s"[id='${saveButtonId}']")

  lazy val jupyterModal: Query = cssSelector("[class='modal-title']")

  lazy val button: Query = cssSelector("button")

  lazy val secondJupyterModal: Query = cssSelector("[class='modal-title']:nth-child(2)")

  lazy val confirmNotebookSaveButton: Query = cssSelector(
    "[class='btn btn-default btn-sm btn-danger save-confirm-btn']"
  )

  lazy val confirmKernelDiedButton: Query = cssSelector(
    "[class='btn btn-default btn-sm btn-primary']"
  )

  //intentionally misspelled
  val saveButtonId = "save-notbook"

  val modalId = "leoUserModal"
  val syncCopyButton = "modal-copy-1"
  val syncReloadButton = "modal-reload"
  val lockPlaygroundButton = "modal-playground"
  val lockCopyButton = "modal-copy-2"
  val noModeBannerId = "notification_not_saving"

  def getSelectorFrom(id: String): Query =
    cssSelector(s"[id='${id}']")

  // is at least one cell currently executing?
  def cellsAreRunning: Boolean =
    findAll(prompts).exists(e => e.text == "In [*]:")

  // has the specified cell completed execution?
  // since we execute cells one by one, the Nth cell (counting from 1) will also have Cell Number N
  def cellIsRendered(cellNumber: Int): Boolean =
    findAll(prompts).exists(e => e.text == s"In [$cellNumber]:")

  // can we see that the kernel connection has terminated?
  def isKernelShutdown: Boolean =
    find(kernelNotification).exists(e => e.text == "No kernel")

  // has the notebook just been saved?
  def isNotebookSaved: Boolean =
    find(notebookNotification).exists(e => e.text.contains("Notebook saved") || e.text.contains("Checkpoint created"))

  def runAllCells(timeout: FiniteDuration = 60 seconds): Unit = {
    dismissNotebookChanged()
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
    // The 'notebook saved' element is only briefly visible, so don't throw an exception if this
    // check times out. The notebook is auto-saved every 5 seconds anyway.
    Try(await.condition(isNotebookSaved, 10))
  }

  lazy val cells: Query = cssSelector(".CodeMirror")

  def lastCell: WebElement =
    webDriver.findElements(cells.by).asScala.toList.last

  def firstCell: WebElement =
    webDriver.findElements(cells.by).asScala.toList.head

  def numCellsOnPage: Int =
    webDriver.findElements(cells.by).asScala.toList.length

  def cellOutput(cell: WebElement): Option[CellOutput] = {
    val outputs = cell.findElements(By.xpath("../../../..//div[contains(@class,'output_subarea')]"))
    outputs.asScala.toList match {
      case Nil => None
      case renderResult :: tail =>
        Some(CellOutput(renderResult.getText, tail.headOption.map(_.getText)))
    }
  }

  //TODO: This function is buggy because the cell numbers are kernel specific not notebook specific
  //It is possible to have a notebook with two cells, numbered 1,1 or even 1, 9
  def executeCell(code: String,
                  timeout: FiniteDuration = 1 minute,
                  cellNumberOpt: Option[Int] = None): Option[String] = {
    dismissNotebookChanged()
    await enabled cells
    val cell = lastCell
    val cellNumber = cellNumberOpt.getOrElse(numCellsOnPage)
    val action = new Actions(webDriver)
    action.moveToElement(cell)
    click on cell
    val jsEscapedCode = StringEscapeUtils.escapeEcmaScript(code)
    executeScript(s"""arguments[0].CodeMirror.setValue("$jsEscapedCode");""", cell)
    clickRunCell(timeout)
    await.condition(cellIsRendered(cellNumber), timeout.toSeconds)
    cellOutput(cell).map(_.renderResult)
  }

  def executeCellWithCellOutput(code: String,
                                timeout: FiniteDuration = 1 minute,
                                cellNumberOpt: Option[Int] = None): Option[CellOutput] = {
    dismissNotebookChanged()
    await enabled cells
    val cell = lastCell
    val cellNumber = cellNumberOpt.getOrElse(numCellsOnPage)
    click on cell
    val jsEscapedCode = StringEscapeUtils.escapeEcmaScript(code)
    executeScript(s"""arguments[0].CodeMirror.setValue("$jsEscapedCode");""", cell)
    clickRunCell(timeout)
    await condition (cellIsRendered(cellNumber), timeout.toSeconds)
    cellOutput(cell)
  }

  //TODO: this function is duplicative of the above but does not have the bug
  def addCodeAndExecute(code: String, wait: Boolean = true, timeout: FiniteDuration = 1 minute): Unit = {
    dismissNotebookChanged()
    await enabled cells
    val cell = lastCell
    click on cell
    val jsEscapedCode = StringEscapeUtils.escapeEcmaScript(code)
    executeScript(s"""arguments[0].CodeMirror.setValue("$jsEscapedCode");""", cell)
    clickRunCell(timeout, wait)
    if (wait) await condition (!cellsAreRunning, timeout.toSeconds)
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
    dismissNotebookChanged()
    awaitReadyKernel(
      1.minutes
    ) //can cause failures with fast tests as it is called in clean-up, and it will timeout if it is called before the kernel is active
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
    dismissNotebookChanged()
    click on kernelMenu
    click on (await enabled restartKernelSelection)
    click on (await enabled restartKernelConfirmationSelection)
    await notVisible restartKernelConfirmationSelection
    await condition (isKernelReady && kernelNotificationText == "none", timeout.toSeconds)
  }

  def clickRunCell(timeout: FiniteDuration = 2.minutes, wait: Boolean = true): Unit = {
    click on runCellButton
    if (wait) awaitReadyKernel(timeout)
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
      case _: TestFailedDueToTimeoutException => throw KernelNotReadyException(time)
    }
  }

  def isKernelDisconnected: Boolean =
    find(id("kernel_indicator_icon")).exists(_.underlying.getAttribute("class") == "kernel_disconnected_icon")

  def isKernelReady: Boolean =
    find(id("kernel_indicator_icon")).exists(_.underlying.getAttribute("class") == "kernel_idle_icon")

  def kernelNotificationText: String =
    find(id("notification_kernel")).map(_.underlying.getCssValue("display")).getOrElse("")

  def isKernelDead: Boolean =
    find(jupyterModal).exists(_.text == "Kernel Restarting")

  def modeExists(): Boolean =
    find(modeBanner).size > 0

  def secondModalExists(): Boolean =
    find(secondJupyterModal).size > 0

  def getMode(): NotebookMode =
    if (modeExists()) {
      NotebookMode.getModeFromString(find(modeBanner).head.text)
    } else {
      NotebookMode.NoMode
    }

  def saveNotebook(timeout: FiniteDuration = 1 minute): Unit = {
    val isSafeMode = find(saveButton).exists(_.underlying.getAttribute("style") == "display: none;")

    if (isSafeMode) toggleSaveButtonHidden(false)
    await visible saveButton
    click on saveButton
    if (isSafeMode) toggleSaveButtonHidden(true)
    // The 'notebook saved' element is only briefly visible, so don't throw an exception if this
    // check times out. The notebook is auto-saved every 5 seconds anyway.
    Try(await.condition(isNotebookSaved, 10))
  }

  def toggleSaveButtonHidden(shouldHide: Boolean) = {
    val functionToRun = if (shouldHide) "hide()" else "show()"
    executeJavaScript(s"$$('#${saveButtonId}').${functionToRun}")
  }

  //checks if elements have the style "display: none;"
  def areElementsHidden(elementIds: List[String]): Boolean =
    elementIds
      .map(elementId => find(id(elementId)).exists(_.underlying.getAttribute("style") == "display: none;"))
      .forall(identity)

  //checks if IDs are present in DOM and the elements with those IDs are displayed. Not the negation of the above, because javascript has many ways to hide elements
  def areElementsPresent(elementIds: List[String]): Boolean =
    elementIds
      .map(elementId =>
        find(id(elementId)) match {
          case Some(el) => el.underlying.isDisplayed
          case None     => false
        }
      )
      .forall(identity)

  def clickOverrideNotebookChanged(): Unit =
    if (findOverwriteButton.isDefined) {
      click on findOverwriteButton.get
    }

  //will cause an exception if no modal exists - check existence of below ID before calling
  def makeACopyFromSyncIssue(): Unit =
    if (find(syncCopyButton).size > 0) {
      click on getSelectorFrom(syncCopyButton)
    }

  //will cause an exception if expected modal does not exist - check existence of below ID before calling
  def goToPlaygroundModeFromLockIssue(): Unit =
    click on getSelectorFrom(lockPlaygroundButton)

  def waitForSecondModal(): Unit =
    await condition (secondModalExists, 7.minutes.toSeconds)

  def findOverwriteButton: Option[Element] =
    findAll(button).find(_.text == "Overwrite")

  /**
   * In some situations Jupyter UI will display a "Notebook changed" modal if it detects
   * the Jupyter last save date is out of sync with the filesystem last_modified timestamp.
   *
   * It's unclear why it sporadically happens in tests. It's not reproducible manually. This
   * is a big hammer approach to dismiss the modal when it comes up. It resolves the issue,
   * but we risk hiding a potentially legitimate bug.
   *
   * See https://broadworkbench.atlassian.net/browse/IA-1228
   */
  def dismissNotebookChanged(): Unit =
    if (find(jupyterModal).exists(_.text == "Notebook changed")) {
      click on confirmNotebookSaveButton
      await notVisible jupyterModal
    }

  def validateKernelDiedAndDismiss(timeout: FiniteDuration = 5.minutes): Unit = {
    await condition (isKernelDead, timeout.toSeconds)
    click on (await enabled confirmKernelDiedButton)
    await notVisible jupyterModal
  }

}

final case class CellOutput(renderResult: String, output: Option[String])
