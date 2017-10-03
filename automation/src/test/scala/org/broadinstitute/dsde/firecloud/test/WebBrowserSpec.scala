package org.broadinstitute.dsde.firecloud.test

import java.io.{File, FileInputStream, FileOutputStream}
import java.net.URL
import java.text.SimpleDateFormat
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.firecloud.api.Orchestration
import org.broadinstitute.dsde.firecloud.config.FireCloudConfig
import org.broadinstitute.dsde.firecloud.page.user.SignInPage
import org.broadinstitute.dsde.firecloud.page.workspaces.WorkspaceListPage
import org.broadinstitute.dsde.firecloud.util.ExceptionHandling
import org.broadinstitute.dsde.workbench.config.Credentials
import org.openqa.selenium.chrome.ChromeDriverService
import org.openqa.selenium.remote.{Augmenter, DesiredCapabilities, LocalFileDetector, RemoteWebDriver}
import org.openqa.selenium.{OutputType, TakesScreenshot, WebDriver}
import org.scalatest.Suite

import scala.sys.SystemProperties
import scala.util.Random

/**
  * Base spec for writing FireCloud web browser tests.
  */
trait WebBrowserSpec extends WebBrowserUtil with ExceptionHandling with LazyLogging { self: Suite =>

  val api = Orchestration

  /**
    * Executes a test in a fixture with a managed WebDriver. A test that uses
    * this will get its own WebDriver instance will be destroyed when the test
    * is complete. This encourages test case isolation.
    *
    * @param testCode the test code to run
    */
  def withWebDriver(testCode: (WebDriver) => Any): Unit = {
    val headless = new SystemProperties().get("headless")
    headless match {
      case Some("false") => runLocalChrome(testCode)
      case _ => runHeadless(testCode)
    }
  }

  private def runLocalChrome(testCode: (WebDriver) => Any) = {
    val service = new ChromeDriverService.Builder().usingDriverExecutable(new File(FireCloudConfig.ChromeSettings.chromDriverPath)).usingAnyFreePort().build()
    service.start()
    implicit val driver = new RemoteWebDriver(service.getUrl, DesiredCapabilities.chrome())
    driver.setFileDetector(new LocalFileDetector())
    try {
      withScreenshot {
        testCode(driver)
      }
    } finally {
      try driver.quit() catch nonFatalAndLog
      try service.stop() catch nonFatalAndLog
    }
  }

  private def runHeadless(testCode: (WebDriver) => Any) = {
    val defaultChrome = FireCloudConfig.ChromeSettings.chromedriverHost
    implicit val driver = new RemoteWebDriver(new URL(defaultChrome), DesiredCapabilities.chrome())
    driver.manage.window.setSize(new org.openqa.selenium.Dimension(1600, 2400))
    driver.setFileDetector(new LocalFileDetector())
    try {
      withScreenshot {
        testCode(driver)
      }
    } finally {
      try driver.quit() catch nonFatalAndLog
    }
  }


  /**
    * Make a random alpha-numeric (lowercase) string to be used as a semi-unique
    * identifier.
    *
    * @param length the number of characters in the string
    * @return a random string
    */
  def makeRandomId(length: Int = 7): String = {
    Random.alphanumeric.take(length).mkString.toLowerCase
  }

  def randomUuid: String = {
    UUID.randomUUID().toString
  }

  /**
    * Convenience method for sign-in to the configured FireCloud URL.
    */
  def signIn(email: String, password: String)(implicit webDriver: WebDriver): Unit = {
    new SignInPage(FireCloudConfig.FireCloud.baseUrl).open.signIn(email, password)
  }

  /**
    * Convenience method for sign-in to the configured FireCloud URL. Assumes
    * that the user has previously registered and will therefore be taken to
    * the workspace list page.
    */
  def signIn(credentials: Credentials)(implicit webDriver: WebDriver): WorkspaceListPage = {
    signIn(credentials.email, credentials.password)
    new WorkspaceListPage
  }

  /**
    * Override of withScreenshot that works with a remote Chrome driver and
    * lets us control the image file name.
    */
  override def withScreenshot(f: => Unit)(implicit driver: WebDriver): Unit = {
    try {
      f
    } catch {
      case t: Throwable =>
        val date = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss-SSS").format(new java.util.Date())
        val fileName = s"failure_screenshots/${date}_$suiteName.png"
        val htmlSourceFileName = s"failure_screenshots/${date}_$suiteName.html"
        try {
          val directory = new File("failure_screenshots")
          if (!directory.exists()) {
            directory.mkdir()
          }
          val tmpFile = new Augmenter().augment(driver).asInstanceOf[TakesScreenshot].getScreenshotAs(OutputType.FILE)
          logger.error(s"Failure screenshot saved to $fileName")
          new FileOutputStream(new File(fileName)).getChannel.transferFrom(
            new FileInputStream(tmpFile).getChannel, 0, Long.MaxValue)

          val html = tagName("html").element.underlying.getAttribute("outerHTML")
          new FileOutputStream(new File(htmlSourceFileName)).write(html.getBytes)
        } catch nonFatalAndLog(s"FAILED TO SAVE SCREENSHOT $fileName")
        throw t
    }
  }
}
