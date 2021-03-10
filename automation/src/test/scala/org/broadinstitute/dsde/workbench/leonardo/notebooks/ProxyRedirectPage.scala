package org.broadinstitute.dsde.workbench.leonardo.notebooks

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.page.{CookieAuthedPage, PageUtil}
import org.broadinstitute.dsde.workbench.service.test.WebBrowserUtil
import org.openqa.selenium.WebDriver
import org.scalatestplus.selenium.Page

/**
 * Created by rtitle on 1/4/18.
 */
class ProxyRedirectPage(override val url: String)(implicit val webDriver: WebDriver,
                                                  implicit override val authToken: AuthToken)
    extends CookieAuthedPage[ProxyRedirectPage]
    with PageUtil[ProxyRedirectPage]
    with WebBrowserUtil {}
