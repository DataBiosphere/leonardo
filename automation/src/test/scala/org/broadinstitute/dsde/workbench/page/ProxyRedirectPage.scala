package org.broadinstitute.dsde.workbench.page

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.ProxyRedirectClient
import org.broadinstitute.dsde.workbench.service.test.WebBrowserUtil
import org.openqa.selenium.WebDriver
import org.scalatestplus.selenium.Page

trait ProxyRedirectPage[P <: Page] extends Page with PageUtil[P] with WebBrowserUtil with LazyLogging { self: P =>
  implicit val authToken: AuthToken

  override def open(implicit webDriver: WebDriver): P = {
    // 1. Go to the target page. This will initially return 401.
    go.to(this)

    // 2. Set the LeoToken cookie. Note this needs to be done from the same domain as the target page.
    addCookie("LeoToken", authToken.value)

    // 3. Go to the proxy redirect page, specifying the target page as the `rurl`. This will automatically
    //    redirect to the target page. This is done so the Referer is set correctly.
    go.to(ProxyRedirectClient.get(url))

    this
  }

}
