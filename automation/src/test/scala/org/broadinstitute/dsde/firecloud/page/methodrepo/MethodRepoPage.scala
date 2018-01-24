package org.broadinstitute.dsde.firecloud.page.methodrepo

import org.broadinstitute.dsde.workbench.config.{Config => FireCloudConfig}
import org.broadinstitute.dsde.firecloud.page.{AuthenticatedPage, FireCloudView, Table}
import org.broadinstitute.dsde.workbench.page.PageUtil
import org.openqa.selenium.WebDriver
import org.scalatest.selenium.Page

class MethodRepoPage(implicit webDriver: WebDriver) extends AuthenticatedPage with Page with PageUtil[MethodRepoPage] {

  override val url: String = s"${FireCloudConfig.FireCloud.baseUrl}#methods2"

  override def awaitLoaded(): MethodRepoPage = {
    ui.MethodRepoTable.awaitReady()
    this
  }

  trait UI extends super.UI {
    private val newMethodButtonQuery = testId("create-method-button")

    def clickNewMethodButton(): Unit = {
      click on newMethodButtonQuery
    }

    object MethodRepoTable extends Table("methods-table") {
      private def methodLink(namespace: String, name: String) = findInner(s"method-link-$namespace-$name")

      def hasMethod(namespace: String, name: String): Boolean = {
        find(methodLink(namespace, name)).isDefined
      }

      def enterMethod(namespace: String, name: String): MethodDetailPage = {
        click on methodLink(namespace, name)
        new MethodDetailPage(namespace, name)
      }
    }
  }

  object ui extends UI
}

class CreateMethodModal(implicit webDriver: WebDriver) extends FireCloudView {

  def createMethod(attributes: Map[String, String]): Unit = {
    ui.fillNamespaceField(attributes("namespace"))
    ui.fillNameField(attributes("name"))
    ui.fillSynopsisField(attributes("synopsis"))
    ui.fillDocumentationField(attributes("documentation"))
    ui.fillWDLField(attributes("payload"))

    ui.clickUploadButton()
  }

  def awaitDismissed(): Unit =
    await notVisible ui.uploadButtonQuery

  object ui {
    private val namespaceFieldQuery = testId("namespace-field")
    private val nameFieldQuery = testId("name-field")
    private val synopsisFieldQuery = testId("synopsis-field")
    private val documentationFieldQuery = testId("documentation-field")
    private val wdlFieldQuery = cssSelector("[data-test-id='wdl-field'] .CodeMirror")
    private[CreateMethodModal] val uploadButtonQuery = testId("upload-button")

    def fillNamespaceField(namespace: String): Unit =
      textField(namespaceFieldQuery).value = namespace

    def fillNameField(name: String): Unit =
      textField(nameFieldQuery).value = name

    def fillSynopsisField(synopsis: String): Unit =
      textField(synopsisFieldQuery).value = synopsis

    def fillDocumentationField(documentation: String): Unit =
      textArea(documentationFieldQuery).value = documentation

    def fillWDLField(wdl: String): Unit = {
      val sanitized = wdl.replaceAll("\"" ,"\\\\\"").replaceAll("\n", "\\\\n")
      executeScript("arguments[0].CodeMirror.setValue(\"" + sanitized + "\");", wdlFieldQuery.webElement)
    }

    def clickUploadButton(): Unit =
      click on uploadButtonQuery
  }
}
