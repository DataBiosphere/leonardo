package org.broadinstitute.dsde.workbench.leonardo.lab

import org.scalatest.selenium.WebBrowser._
import org.openqa.selenium.WebElement

trait Toolbar {

  // selects all menus from the header bar
  lazy val menus: String = "[class='p-MenuBar-item']"

  // Run Cell toolbar button
  lazy val runCellButton: String = "[title='Run the selected cells and advance']"

  // Kernel -> Shutdown
  lazy val shutdownKernelSelection: String = "[data-command='kernelmenu:shutdown']"

  lazy val kernelStatus: String = "jp-Toolbar-kernelStatus"

}

