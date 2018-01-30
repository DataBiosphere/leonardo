package org.broadinstitute.dsde.workbench.util

import java.io.File
import java.nio.file.Files
import org.broadinstitute.dsde.workbench.service.test.WebBrowserUtil

import org.openqa.selenium.WebDriver

trait LocalFileUtil extends WebBrowserUtil {
  def moveFile(source: File, dest: File)(implicit webDriver: WebDriver): Unit = {
    await condition source.exists()

    if (!dest.getParentFile.exists()) {
      dest.getParentFile.mkdirs()
    }
    Files.move(source.toPath, dest.toPath)
  }
}
