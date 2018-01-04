package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.config.AuthToken
import org.broadinstitute.dsde.workbench.page.PageUtil
import org.broadinstitute.dsde.workbench.util.WebBrowserUtil
import org.openqa.selenium.WebDriver
import org.scalatest.selenium.Page

/**
  * Created by rtitle on 1/4/18.
  */
class DummyClientPage(override val url: String) extends Page with PageUtil[DummyClientPage] with WebBrowserUtil {

  val notebookLink: Query = cssSelector("[id='notebook']")

  def openNotebook(implicit authToken: AuthToken, driver: WebDriver): NotebooksListPage = {
    switchToNewTab {
      click on notebookLink
    }
    // NOT calling NotebooksListPage.open() which sets a cookie.
    // The notebooks list page should be already opened by the dummy client.
    new NotebooksListPage(currentUrl)
  }

}