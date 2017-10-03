package org.broadinstitute.dsde.firecloud.page

import org.openqa.selenium.WebDriver

class Table(rootId: String)(implicit webDriver: WebDriver) extends FireCloudView {

  private val tableElement = testId(rootId)

  def findInner(id: String): CssSelectorQuery = testId(id) inside tableElement

  private val filterField = findInner("filter-input")
  private val filterButton = findInner("filter-button")

  private def tab(name: String) = findInner(s"$name-tab")

  private val prevPageButton = findInner("prev-page")
  private val nextPageButton = findInner("next-page")
  private def pageButton(page: Int) = findInner(s"page-$page")
  private val perPageSelector = findInner("per-page")

  def awaitReady(): Unit = {
    await enabled tableElement
    await condition { tableElement.element.attribute("data-test-state").getOrElse("unknown") == "ready" }
  }

  def filter(text: String): Unit = {
    awaitReady()
    searchField(filterField).value = text
    click on filterButton
  }

  def goToTab(tabName: String): Unit = {
    awaitReady()
    click on tab(tabName)
  }

  def readDisplayedTabCount(tabName: String): Int = {
    awaitReady()
    readText(tab(tabName)).replaceAll("\\D+","").toInt
  }

  def goToPreviousPage(): Unit = {
    awaitReady()
    click on prevPageButton
  }

  def goToNextPage(): Unit = {
    awaitReady()
    click on nextPageButton
  }

  def goToPage(page: Int): Unit = {
    awaitReady()
    click on pageButton(page)
  }

  def selectPerPage(perPage: Int): Unit = {
    awaitReady()
    singleSel(perPageSelector).value = perPage.toString
  }
}
