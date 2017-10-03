package org.broadinstitute.dsde.firecloud.page.methodrepo

import org.broadinstitute.dsde.firecloud.config.FireCloudConfig
import org.broadinstitute.dsde.firecloud.page.{AuthenticatedPage, MessageModal, PageUtil}
import org.openqa.selenium.WebDriver
import org.scalatest.selenium.Page

class MethodDetailPage(namespace: String, name: String)(implicit webDriver: WebDriver) extends AuthenticatedPage
  with Page with PageUtil[MethodDetailPage] {

  override val url = s"${FireCloudConfig.FireCloud.baseUrl}#methods2/$namespace/$name"

  override def awaitLoaded(): MethodDetailPage = {
    await enabled ui.redactButtonQuery
    this
  }

  trait UI extends super.UI {
    private[MethodDetailPage] val redactButtonQuery = testId("redact-button")

    def redact(): Unit = {
      click on redactButtonQuery
      MessageModal().clickOk()
      // redact takes us back to the table:
      await notVisible redactButtonQuery
    }
  }

  object ui extends UI
}
