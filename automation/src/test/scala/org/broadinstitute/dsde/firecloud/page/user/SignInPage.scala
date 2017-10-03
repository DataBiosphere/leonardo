package org.broadinstitute.dsde.firecloud.page.user

import org.broadinstitute.dsde.firecloud.page.{FireCloudView, PageUtil}
import org.broadinstitute.dsde.firecloud.test.WebBrowserUtil
import org.openqa.selenium.WebDriver
import org.scalatest.selenium.{Page, WebBrowser}

/**
  * Page class for the page displayed when accessing FireCloud when not signed in.
  */
class SignInPage(val baseUrl: String)(implicit webDriver: WebDriver) extends FireCloudView with Page with PageUtil[SignInPage] {

  override val url: String = baseUrl

  /**
    * Sign in to FireCloud. Returns when control is handed back to FireCloud after Google sign-in is done.
    */
  def signIn(email: String, password: String): Unit = {
    val popup = beginSignIn()
    popup.signIn(email, password)
    await enabled testId("account-dropdown")
  }

  /**
    * Handles the pre-sign-in dance of switching Selenium's focus to Google's
    * sign-in pop-up window.
    *
    * @return a new GoogleSignInPopup
    */
  private def beginSignIn(): GoogleSignInPopup = {
    val initialWindowHandles = windowHandles

    ui.clickSignIn()
    await condition (windowHandles.size > 1)

    val popupWindowHandle = (windowHandles -- initialWindowHandles).head

    switch to window(popupWindowHandle)
    new GoogleSignInPopup().awaitLoaded()
  }

  object ui {

    private val signInButton = testId("sign-in-button")

    def clickSignIn(): Unit = {
      click on (await enabled signInButton)
    }
  }
}

class GoogleSignInPopup(implicit webDriver: WebDriver) extends WebBrowser with WebBrowserUtil {

  def awaitLoaded(): GoogleSignInPopup = {
    await text "to continue to"
    this
  }

  /**
    * Signs in to Google to authenticate for FireCloud.
    */
  def signIn(email: String, password: String): Unit = {
    val chooseAccount = find(id("identifierLink")).filter(_.isDisplayed) foreach { click on _ }

    await enabled id("identifierId")
    emailField(id("identifierId")).value = email
    pressKeys("\n")

    await enabled id("passwordNext")
    await enabled name("password")
    /*
     * The log-in pane animation is sometimes delayed or takes while during which the password field flips between
     * enabled and disabled. While it's not great to sleep for a fixed amount of time, we have very little ability to
     * do anything better. From experimentation, 300ms isn't quite enough but 500ms seems to do it.
     */
    Thread sleep 500
    pwdField(name("password")).value = password
    pressKeys("\n")

    returnFromSignIn()
  }

  /**
    * Handles the post-sign-in dance of switching Selenium's focus back to the
    * main FireCloud window.
    * TODO: make this work when there is more than one window
    */
  def returnFromSignIn(): Unit = {
    /*
     * The sign-in popup may go away at any time which could cause any calls
     * such as findElement to fail with NullPointerException. Therefore, the
     * only safe check we can make is on the number of windows.
     */
    await condition (windowHandles.size == 1, 30)

    /*
     * If there is still more than 1 window after 30 seconds, we most likely
     * need to approve access to continue.
     */
    if (windowHandles.size > 1) {
      click on id("submit_approve_access")
      await condition(windowHandles.size == 1)
    }

    switch to window(windowHandles.head)
  }
}