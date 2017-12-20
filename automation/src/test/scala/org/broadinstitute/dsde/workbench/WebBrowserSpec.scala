package org.broadinstitute.dsde.workbench

import java.io.{File, FileInputStream, FileOutputStream}
import java.net.URL
import java.text.SimpleDateFormat
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.config.WorkbenchConfig
import org.broadinstitute.dsde.workbench.util.{ExceptionHandling, WebBrowserUtil}
import org.openqa.selenium.chrome.{ChromeDriverService, ChromeOptions}
import org.openqa.selenium.remote.{Augmenter, DesiredCapabilities, LocalFileDetector, RemoteWebDriver}
import org.openqa.selenium.{OutputType, TakesScreenshot, WebDriver}
import org.scalatest.Suite

import scala.collection.JavaConverters._
import scala.sys.SystemProperties
import scala.util.Random

/**
  * Base spec for writing web browser tests.
  */
trait WebBrowserSpec extends WebBrowserUtil with ExceptionHandling with LazyLogging { self: Suite =>

  /**
    * Executes a test in a fixture with a managed WebDriver. A test that uses
    * this will get its own WebDriver instance will be destroyed when the test
    * is complete. This encourages test case isolation.
    *
    * @param testCode the test code to run
    */
  def withWebDriver(testCode: (WebDriver) => Any): Unit = {
    withWebDriver(System.getProperty("java.io.tmpdir"))(testCode)
  }

  /**
    * Executes a test in a fixture with a managed WebDriver. A test that uses
    * this will get its own WebDriver instance will be destroyed when the test
    * is complete. This encourages test case isolation.
    *
    * @param downloadPath a directory where downloads should be saved
    * @param testCode the test code to run
    */
  def withWebDriver(downloadPath: String)(testCode: (WebDriver) => Any): Unit = {
    val capabilities = getChromeIncognitoOption(downloadPath)
    val headless = new SystemProperties().get("headless")
    headless match {
      case Some("false") => runLocalChrome(capabilities, testCode)
      case _ => runHeadless(capabilities, testCode)
    }
  }

  private def getChromeIncognitoOption(downloadPath: String): DesiredCapabilities = {
    val options = new ChromeOptions
    options.addArguments("--incognito")
    // Note that download.prompt_for_download will be ignored if download.default_directory is invalid or doesn't exist
    options.setExperimentalOption("prefs", Map(
      "download.default_directory" -> downloadPath,
      "download.prompt_for_download" -> "false").asJava)
    val capabilities = DesiredCapabilities.chrome
    capabilities.setCapability(ChromeOptions.CAPABILITY, options)
    capabilities
  }

  private def runLocalChrome(capabilities: DesiredCapabilities, testCode: (WebDriver) => Any): Unit = {
    val service = new ChromeDriverService.Builder().usingDriverExecutable(new File(WorkbenchConfig.ChromeSettings.chromDriverPath)).usingAnyFreePort().build()
    service.start()
    implicit val driver: RemoteWebDriver = new RemoteWebDriver(service.getUrl, capabilities)
//    driver.manage.window.setSize(new org.openqa.selenium.Dimension(1600, 2400))
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

  private def runHeadless(capabilities: DesiredCapabilities, testCode: (WebDriver) => Any): Unit = {
    val defaultChrome = WorkbenchConfig.ChromeSettings.chromedriverHost
    implicit val driver: RemoteWebDriver = new RemoteWebDriver(new URL(defaultChrome), capabilities)
//    driver.manage.window.setSize(new org.openqa.selenium.Dimension(1600, 2400))
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
