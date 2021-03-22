package org.broadinstitute.dsde.workbench.page

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.service.test.WebBrowserUtil
import org.openqa.selenium.WebDriver

class ProxyRedirectPage(override val url: String)(implicit val webDriver: WebDriver,
                                                  implicit override val authToken: AuthToken)
    extends CookieAuthedPage[ProxyRedirectPage]
    with PageUtil[ProxyRedirectPage]
    with WebBrowserUtil {}
