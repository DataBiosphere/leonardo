package org.broadinstitute.dsde.firecloud.page.workspaces.methodconfigs

import org.broadinstitute.dsde.firecloud.config.FireCloudConfig
import org.broadinstitute.dsde.firecloud.page.workspaces.WorkspacePage
import org.broadinstitute.dsde.firecloud.page.workspaces.monitor.SubmissionDetailsPage
import org.broadinstitute.dsde.firecloud.page.{ErrorModal, FireCloudView, PageUtil, Table}
import org.openqa.selenium.{JavascriptExecutor, WebDriver}
import org.scalatest.selenium.Page

class WorkspaceMethodConfigDetailsPage(namespace: String, name: String, methodConfigNamespace: String, methodConfigName: String)(implicit webDriver: WebDriver) extends WorkspacePage with Page with PageUtil[WorkspaceMethodConfigDetailsPage] {

  override val url: String = s"${FireCloudConfig.FireCloud.baseUrl}#workspaces/$namespace/$name/method-configs/$methodConfigNamespace/$methodConfigName"

  def launchAnalysis(rootEntityType: String, entityId: String, expression: String = "", enableCallCaching: Boolean = true) = {
    val launchModal = ui.openLaunchAnalysisModal()
    launchModal.launchAnalysis(rootEntityType, entityId, expression, enableCallCaching)
    new SubmissionDetailsPage(namespace, name)
  }

  def editMethodConfig(newName: Option[String] = None, newSnapshotId: Option[Int] = None, newRootEntityType: Option[String] = None,
                       inputs: Option[Map[String, String]] = None, outputs: Option[Map[String, String]] = None) = {
    ui.openEditMode()
    await spinner "Loading attributes..."

    if (newName != None) { ui.changeMethodConfigName(newName.get) }
    if (newSnapshotId != None) { ui.changeSnapshotId(newSnapshotId.get) }
    if (newRootEntityType != None) { ui.changeRootEntityType(newRootEntityType.get)}
    if (inputs != None) { ui.changeInputsOutputs(inputs.get)  }
    if (outputs != None) { ui.changeInputsOutputs(outputs.get)}
    ui.saveEdits()

  }

  def openlaunchModal() = {
    ui.openLaunchAnalysisModal()
  }

  def isLoaded: Boolean = {
    ui.isLaunchAnalysisButtonPresent()
  }

  override def awaitLoaded(): WorkspaceMethodConfigDetailsPage = {
    await condition isLoaded
    await spinner "Checking permissions..."
    this
  }

  def deleteMethodConfig(): WorkspaceMethodConfigListPage = {
    ui.deleteMethodConfig()
    new WorkspaceMethodConfigListPage(namespace, name)
  }

  trait UI extends super.UI {
    private val methodConfigNameTextQuery: Query = testId("method-config-name")
    private val openLaunchAnalysisModalButtonQuery: Query = testId("open-launch-analysis-modal-button")
    private val openEditModeQuery: Query = testId("edit-method-config-button")
    private val editMethodConfigNameInputQuery: Query = testId("edit-method-config-name-input")
    private val saveEditedMethodConfigButtonQuery: Query = testId("save-edited-method-config-button")
    private val cancelEditMethodConfigModeButtonQuery: Query = testId("cancel-edit-method-config-button")
    private val editMethodConfigSnapshotIdSelectQuery: Query = testId("edit-method-config-snapshot-id-select")
    private val editMethodConfigRootEntityTypeInputQuery: Query = testId("edit-method-config-root-entity-type-select")
    private val deleteMethodConfigButtonQuery: Query = testId("delete-method-config-button")
    private val modalConfirmDeleteButtonQuery: Query = testId("modal-confirm-delete-button")
    private val snapshotRedactedTitleQuery: Query = testId("snapshot-redacted-title")
    private val snapshotIdLabelQuery: Query = testId("method-label-Snapshot ID")

    def openLaunchAnalysisModal(): LaunchAnalysisModal = {
      await enabled methodConfigNameTextQuery
      click on (await enabled openLaunchAnalysisModalButtonQuery)
      new LaunchAnalysisModal
    }

    def openEditMode() = {
      click on (await enabled openEditModeQuery)
    }

    def changeMethodConfigName(newName: String) = {
      await enabled editMethodConfigNameInputQuery
      textField(editMethodConfigNameInputQuery).value = newName
    }

    def changeSnapshotId(newSnapshotId: Int) = {
      await enabled editMethodConfigSnapshotIdSelectQuery
      singleSel(editMethodConfigSnapshotIdSelectQuery).value = newSnapshotId.toString
    }

    def changeRootEntityType(newRootEntityType: String) = {
      await enabled editMethodConfigRootEntityTypeInputQuery
      singleSel(editMethodConfigRootEntityTypeInputQuery).value = newRootEntityType
    }

    def changeInputsOutputs(fields: Map[String, String]) = {
      for ((field, expression) <- fields) {
        val fieldInputQuery: Query = xpath(s"//*[@data-test-id='$field-text-input']/..//input")
        searchField(fieldInputQuery).value = expression
      }
    }

    def saveEdits(state: String = "enabled") = {
      val button = await enabled saveEditedMethodConfigButtonQuery
      await forState(button, state)
      // The button can sometimes scroll off the page and become unclickable. Therefore we need to scroll it into view.
      webDriver.asInstanceOf[JavascriptExecutor].executeScript("arguments[0].scrollIntoView(true)", button.underlying)
      click on button
    }

    def cancelEdits() = {
      click on (await enabled cancelEditMethodConfigModeButtonQuery)
    }

    def isLaunchAnalysisButtonPresent() = {
      await enabled openLaunchAnalysisModalButtonQuery
      find(openLaunchAnalysisModalButtonQuery).size == 1
    }

    def clickLaunchAnalysisButtonError(): ErrorModal = {
      click on (await enabled openLaunchAnalysisModalButtonQuery)
      new ErrorModal
    }

    def verifyMethodConfigurationName(methodConfigName: String) = {
      await enabled methodConfigNameTextQuery

      val methodConfigNameElement = find(methodConfigNameTextQuery)
      methodConfigNameElement.get.text == methodConfigName

    }

    def isSnapshotRedacted() = {
      await enabled snapshotIdLabelQuery
      find(snapshotRedactedTitleQuery).isDefined
    }

    def deleteMethodConfig() = {
      click on (await enabled deleteMethodConfigButtonQuery)
      click on (await enabled modalConfirmDeleteButtonQuery)
    }

  }
  object ui extends UI

}



/**
  * Page class for the launch analysis modal.
  */
class LaunchAnalysisModal(implicit webDriver: WebDriver) extends FireCloudView {

  /**
    *
    */
  def launchAnalysis(rootEntityType: String, entityId: String, expression: String = "", enableCallCaching: Boolean): Unit = { //Use Option(String) for expression?
    ui.filterRootEntityType(rootEntityType)
    ui.searchEntity(entityId)
    ui.selectEntity(entityId)
    if (!expression.isEmpty()) { ui.fillExpression(expression) }
    if (!enableCallCaching) { ui.clickCallCachingCheckbox() }
    ui.clickLaunchButton()
  }

  def validateLocation(implicit webDriver: WebDriver): Boolean = {
    testId("launch-analysis-modal").element != null
  }

  def filterRootEntityType(rootEntityType: String) = {
    ui.filterRootEntityType(rootEntityType)
  }

  def searchAndSelectEntity(entityId: String) = {
    ui.searchEntity(entityId)
    ui.selectEntity(entityId)
  }

  def fillExpressionField(expression: String) = {
    ui.fillExpression(expression)
  }

  def clickLaunchButton() = {
    ui.clickLaunchButton()
  }

  def verifyNoDefaultEntityMessage(): Boolean = {
    ui.isNoDefaultEntitiesMessagePresent()
  }

  def verifyWorkflowsWarning(): Boolean = {
    ui.isNumberOfWorkflowWarningPresent()
  }

  def verifyWrongEntityError(errorText: String): Boolean = {
    ui.isErrorTextPresent(errorText)
  }

  def verifyMissingInputsError(errorText: String): Boolean = {
    ui.isErrorTextPresent(errorText)
  }

  def closeModal() = {
    ui.closeModal()
  }

  object ui {
    private val entityTable = new Table("entity-table")
    private val expressionInputQuery: Query = testId("define-expression-input")
    private val emptyDefaultEntitiesMessageQuery: Query = testId("message-well")
    private val launchAnalysisButtonQuery: Query = testId("launch-button")
    private val closeModalXButtonQuery: Query = testId("x-button")
    private val numberOfWorkflowsWarningQuery: Query = testId("number-of-workflows-warning")
    private val callCachingCheckboxQuery: Query = testId("call-cache-checkbox")

    private val emptyDefaultMessage = "There are no entities to display."

    def filterRootEntityType(rootEntityType: String) = {
      entityTable.goToTab(rootEntityType)
    }

    def filterParticipantSetType() = {
      entityTable.goToTab("participant_set")
    }

    def searchEntity(entityId: String) = {
      entityTable.filter(entityId)
    }

    def selectEntity(entityId: String) = {
      entityTable.awaitReady()
      click on testId(entityId + "-link")
    }

    def fillExpression(expression: String) = {
      await enabled expressionInputQuery
      searchField(expressionInputQuery).value = expression
    }

    def clickLaunchButton() = {
      click on (await enabled launchAnalysisButtonQuery)
    }

    def isNoDefaultEntitiesMessagePresent(): Boolean = {
      await enabled emptyDefaultEntitiesMessageQuery
      find(emptyDefaultEntitiesMessageQuery).size == 1
    }

    def closeModal() = {
      click on (await enabled closeModalXButtonQuery)
    }

    def isNumberOfWorkflowWarningPresent(): Boolean = {
      await enabled numberOfWorkflowsWarningQuery
      find(numberOfWorkflowsWarningQuery).size == 1
    }

    def isErrorTextPresent(errorText: String): Boolean = {
      val errorTextQuery: Query = text(errorText)
      await enabled errorTextQuery
      val error = find(errorTextQuery)
      error.size == 1
    }

    def clickCallCachingCheckbox() = {
      click on (await enabled callCachingCheckboxQuery)
    }

  }
}





