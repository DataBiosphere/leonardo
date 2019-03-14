package org.broadinstitute.dsde.workbench.leonardo.notebooks

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.page.PageUtil
import org.broadinstitute.dsde.workbench.service.test.WebBrowserUtil
import org.openqa.selenium.WebDriver
import org.scalatest.selenium.Page

/**
  * Created by rtitle on 1/4/18.
  */
class DummyClientPage(override val url: String)(implicit val authToken: AuthToken, implicit val webDriver: WebDriver)
  extends Page with PageUtil[DummyClientPage] with WebBrowserUtil {

  val notebookLink: Query = cssSelector("[id='notebook']")

  def openNotebook: NotebooksListPage = {
    switchToNewTab {
      click on notebookLink
    }
    // NOT calling NotebooksListPage.open() which sets a cookie.
    // The notebooks list page should be already opened by the dummy client.
    new NotebooksListPage(currentUrl)
  }

  override def awaitLoaded(): DummyClientPage = {
    await enabled (notebookLink, 120)
    this
  }

}