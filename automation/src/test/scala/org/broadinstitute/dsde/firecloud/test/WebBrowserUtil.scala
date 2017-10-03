package org.broadinstitute.dsde.firecloud.test

import org.openqa.selenium.support.ui.{ExpectedCondition, WebDriverWait}
import org.openqa.selenium.{StaleElementReferenceException, WebDriver}
import org.scalatest.selenium.WebBrowser

/**
  * Mix-in utilities for ScalaTest's WebBrowser.
  */
trait WebBrowserUtil extends WebBrowser {
  val defaultTimeOutInSeconds: Long = 30

  /**
    * Override of the base find() method to retry in the case of a
    * StaleElementReferenceException.
    */
  abstract override def find(query: Query)(implicit driver: WebDriver): Option[Element] = {
    try {
      super.find(query)
    } catch {
      case _: StaleElementReferenceException => this.find(query)
    }
  }

  abstract override def findAll(query: Query)(implicit driver: WebDriver): Iterator[Element] = {
    try {
      super.findAll(query)
    } catch {
      case _: StaleElementReferenceException => this.findAll(query)
    }
  }

  /**
    * Extension to ScalaTest's Selenium DSL for waiting on changes in browser
    * state. Example:
    *
    * <pre>
    * await enabled id("myButton")
    * </pre>
    */
  object await {

    /**
      * Waits for a condition to be met.
      *
      * @param condition function returning the Boolean result of the condition check
      * @param timeOutInSeconds number of seconds to wait for the condition to be true
      * @param webDriver implicit WebDriver for the WebDriverWait
      */
    def condition(condition: => Boolean, timeOutInSeconds: Long = defaultTimeOutInSeconds)(implicit webDriver: WebDriver): Unit = {
      withWaitForCondition(timeOutInSeconds) {
        condition
      }
    }

    /**
      * Waits for an element to be enabled. Returns the element found by the
      * query to facilitate call chaining, e.g.:
      *
      *   click on (await enabled id("my-button"))
      *
      * Returns null if the element is not found. Why null instead of None? It
      * would be too easy for callers to map/flatmap an Option which is likely
      * to delay failure of the test if it's None. For example, if trying to
      * click on a button, nothing would happen instead of the test failing.
      * The test would (hopefully) fail only when the next action it tries to
      * do is not available. Understanding the cause of the failure would
      * require more work, including probably looking at the failure
      * screenshot. A NullPointerException when trying to interact with the
      * element will result in a more immediate failure with a more obvious
      * cause.
      *
      * @param query Query to locate the element
      * @param timeOutInSeconds number of seconds to wait for the enabled element
      * @param webDriver implicit WebDriver for the WebDriverWait
      * @return the found element
      */
    def enabled(query: Query, timeOutInSeconds: Long = defaultTimeOutInSeconds)
               (implicit webDriver: WebDriver): Element = {
      withWaitForElement(timeOutInSeconds) {
        find(query).filter(_.isEnabled).orNull
      }
    }

    def writable(query: Query, timeoutInSeconds: Long = defaultTimeOutInSeconds)
                (implicit webDriver: WebDriver): Element = {
      withWaitForElement(timeoutInSeconds) {
        findAll(query).find(_.attribute("readonly").isEmpty).orNull
      }
    }

    def notVisible(query: Query, timeOutInSeconds: Long = defaultTimeOutInSeconds)
                  (implicit webDriver: WebDriver): Unit = {
      withWaitForCondition(timeOutInSeconds) {
        !findAll(query).exists(_.isDisplayed)
      }
    }

    def forState(element: Element, state: String, timeOutInSeconds: Long = defaultTimeOutInSeconds)
                    (implicit webDriver: WebDriver): Element = {
      withWaitForCondition(timeOutInSeconds) {
        element.attribute("data-test-state").getOrElse("") == state
      }
      element
    }

    def spinner(text: String, timeOutInSeconds: Long = defaultTimeOutInSeconds)
               (implicit webDriver: WebDriver): Unit = {
      // Micro-sleep to make sure the spinner has had a chance to appear before waiting for it to disappear.
      Thread sleep 100
      notVisible(xpath(s"//*[@data-test-id='spinner-text' and contains(text(),'$text')]"))
    }

    /**
      * Waits for an element to be enabled, then clicks it.
      *
      * @param query Query to locate the element
      * @param timeOutInSeconds number of seconds to wait for the enabled element
      * @param webDriver implicit WebDriver for the WebDriverWait
      */
    def thenClick(query: Query, timeOutInSeconds: Long = defaultTimeOutInSeconds)(implicit webDriver: WebDriver): Unit = {
      val element = await enabled query
      click on element
    }

    /**
      * Waits for an element containing the given text.
      * TODO: this is currently untested
      *
      * @param text the text to search for
      * @param timeOutInSeconds number of seconds to wait for the text
      * @param webDriver implicit WebDriver for the WebDriverWait
      */
    def text(text: String, timeOutInSeconds: Long = defaultTimeOutInSeconds)(implicit webDriver: WebDriver): Unit = {
      await condition (find(withText(text)).isDefined, timeOutInSeconds)
    }

    def visible(query: Query, timeOutInSeconds: Long = defaultTimeOutInSeconds)
               (implicit webDriver: WebDriver): Unit = {
      withWaitForCondition(timeOutInSeconds) {
        find(query).exists(_.isDisplayed)
      }
    }

    private def withWaitForCondition(timeOutInSeconds: Long)(f: => Boolean)(implicit webDriver: WebDriver): Boolean = {
      val wait = new WebDriverWait(webDriver, timeOutInSeconds)
      wait until new ExpectedCondition[Boolean] {
        override def apply(d: WebDriver): Boolean = {
          try {
            f
          } catch {
            case _: StaleElementReferenceException => false
          }
        }
      }
    }

    private def withWaitForElement(timeOutInSeconds: Long)(f: => Element)(implicit webDriver: WebDriver): Element = {
      val wait = new WebDriverWait(webDriver, timeOutInSeconds)
      wait until new ExpectedCondition[Element] {
        override def apply(d: WebDriver): Element = {
          try {
            f
          } catch {
            case _: StaleElementReferenceException => null
          }
        }
      }
    }
  }

  def enabled(q: Query)(implicit webDriver: WebDriver): Boolean = {
    find(q).exists(_.isEnabled)
  }

  /**
    * Extension to ScalaTest's Selenium DSL for working with option elements.
    */
  object option {

    /**
      * Determines the value of an option based on its text. Example:
      *
      * <pre>
      * singleSel("choices").value = option value "Choice 1"
      * </pre>
      *
      * @param text text label of the option
      * @param webDriver implicit WebDriver for the WebDriverWait
      * @return the value of the option
      */
    def value(text: String)(implicit webDriver: WebDriver): String = {
      find(xpath(s"//option[text()='$text']")).get.underlying.getAttribute("value")
    }
  }

  /**
    * Creates a Query for an element with a data-test-id attribute.
    *
    * @param id the expected data-test-id
    * @param webDriver implicit WebDriver for the WebDriverWait
    * @return a Query for the data-test-id
    */
  def testId(id: String)(implicit webDriver: WebDriver): CssSelectorQuery = {
    cssSelector(s"[data-test-id='$id']")
  }

  def testState(state: String)(implicit webDriver: WebDriver): CssSelectorQuery = {
    cssSelector(s"[data-test-state='$state']")
  }

  def typeSelector(selector: String)(implicit webDriver: WebDriver): CssSelectorQuery = {
    cssSelector(s"[type='$selector']")
  }

  implicit class RichCssSelectorQuery(child: CssSelectorQuery) {
    def inside(parent: CssSelectorQuery): CssSelectorQuery = {
      CssSelectorQuery(parent.queryString + " " + child.queryString)
    }
  }

  def withText(text: String)(implicit webDriver: WebDriver): Query = {
    xpath(s"//*[contains(text(),'$text')]")
  }

  /**
    * Creates a query for an element containing the given text.
    * TODO: this is currently untested
    *
    * @param text the text to search for
    * @param webDriver implicit WebDriver for the WebDriverWait
    * @return a Query for the text
    */
  def text(text: String)(implicit webDriver: WebDriver): Query = {
    xpath(s"//*[contains(text(),'$text')]")
  }

  /**
    * Creates a Query for an element with a title attribute.
    *
    * @param title the expected title
    * @param webDriver implicit WebDriver for the WebDriverWait
    * @return a Query for the title
    */
  def title(title: String)(implicit webDriver: WebDriver): Query = {
    cssSelector(s"[title='$title']")
  }
}
