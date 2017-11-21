package org.broadinstitute.dsde.firecloud.page.billing

import org.broadinstitute.dsde.firecloud.config.FireCloudConfig
import org.broadinstitute.dsde.firecloud.page.{AuthenticatedPage, FireCloudView, Table}
import org.broadinstitute.dsde.firecloud.util.Retry.retry
import org.broadinstitute.dsde.workbench.page.PageUtil
import org.openqa.selenium.WebDriver
import org.scalatest.selenium.Page

import scala.concurrent.duration.DurationLong

/**
  * Page class for managing billing projects.
  */
class BillingManagementPage(implicit webDriver: WebDriver) extends AuthenticatedPage
  with Page with PageUtil[BillingManagementPage] {
  override val url: String = s"${FireCloudConfig.FireCloud.baseUrl}#billing"

  override def awaitLoaded(): BillingManagementPage = {
    await condition ui.hasCreateBillingProjectButton
    this
  }

  /**
    * Creates a new billing project. Returns after creation has started, though
    * it may not yet be complete. Creation status, including success or
    * failure, can be queried using ui.creationStatusForProject().
    *
    * @param projectName the name for the new project
    * @param billingAccountName the billing account for the new project
    */
  def createBillingProject(projectName: String, billingAccountName: String): Unit = {
    val modal = ui.clickCreateBillingProjectButton()
    modal.createBillingProject(projectName, billingAccountName)
  }

  /**
    * Filters the list of billing projects.
    *
    * @param text the text to filter by
    */
  def filter(text: String): Unit = {
    ui.filter(text)
  }

  /**
    * Waits until creation of a billing project is complete. The result can be
    * "success", "failure", or "unknown".
    *
    * Note: this has a side effect of filtering the billing project list in
    * order to make sure the status of the requested project is visible.
    *
    * @param projectName the billing project name
    * @return a status of "success", "failure", or "unknown"
    */
  def waitForCreateCompleted(projectName: String): String = {
    filter(projectName)
    retry(10.seconds, 5.minutes)({
          ui.readCreationStatusForProject(projectName).filterNot(_ equals "running")
        }) match {
      case None => throw new Exception("Billing project creation did not complete")
      case Some(s) => s
    }
  }


  def openBillingProject(projectName: String) = {
    filter(projectName)
    ui.openBillingProject(projectName)
  }


  def addUserToBillingProject(userEmail: String, role: String) = {
    val modal = ui.openAddUserDialog()
    modal.addUserToBillingProject(userEmail, role)
  }

  def isUserInBillingProject(userEmail: String): Boolean = {
    userEmail == ui.findUser(userEmail)
  }


  trait UI extends super.UI {
    private val billingProjectTable = new Table("billing-project-table")

    private val createBillingProjectButton: Query = testId("begin-create-billing-project")
    private val addUserButton = testId("billing-project-add-user-button")

    def clickCreateBillingProjectButton(): CreateBillingProjectModal = {
      click on createBillingProjectButton
      new CreateBillingProjectModal
    }

    def filter(text: String): Unit = {
      billingProjectTable.filter(text)
    }

    def hasCreateBillingProjectButton: Boolean = {
      find(createBillingProjectButton).isDefined
    }

    def readCreationStatusForProject(projectName: String): Option[String] = {
      for {
        e <- find(xpath(s"//div[@data-test-id='$projectName-row']//span[@data-test-id='status-icon']"))
        v <- e.attribute("data-test-value")
      } yield v
    }

    def openBillingProject(projectName: String) = {
      val billingProjectLink = testId(projectName + "-link")
      click on (await enabled billingProjectLink)
    }

    def openAddUserDialog() = {
      click on (await enabled addUserButton)
      new AddUserToBillingProjectModal
    }

    def findUser(userEmail: String): String = {
      val emailQuery = testId(userEmail)
      await enabled emailQuery
      val userEmailElement = find(emailQuery)
      userEmailElement.get.text
    }
  }
  object ui extends UI
}


/**
  * Page class for the modal for creating a billing project.
  */
class CreateBillingProjectModal(implicit webDriver: WebDriver) extends FireCloudView {

  def createBillingProject(projectName: String, billingAccountName: String): Unit = {
    ui.fillProjectName(projectName)
    ui.selectBillingAccount(billingAccountName)
    ui.clickCreateButton()
    ui.clickCreateButtonWait()
  }


  object ui {
    private val createBillingProjectModal: Query = testId("create-billing-project-modal")
    private val createButton: Query = testId("create-project-button")
    private val projectNameInput = testId("project-name-input")

    def clickCreateButton(): Unit = {
      click on createButton
    }

    def clickCreateButtonWait(): Unit = {
      await notVisible createBillingProjectModal
    }

    def fillProjectName(name: String): Unit = {
      await enabled projectNameInput
      textField(projectNameInput).value = name
    }

    def selectBillingAccount(name: String): Unit = {
      click on testId(name)
    }
  }
}



/**
  * Page class for the modal for adding users to a billing project.
  */
class AddUserToBillingProjectModal(implicit webDriver: WebDriver) extends FireCloudView {

  def addUserToBillingProject(userEmail: String, role: String): Unit = {
    ui.fillUserEmail(userEmail)
    ui.selectRole(role)
    ui.confirmAddUserDialog()
  }

  object ui {
    private val addUserModalEmailInput = testId("billing-project-add-user-modal-user-email-input")
    private val addUserModalRoleSelect = testId("billing-project-add-user-modal-user-role-select")
    private val addUserModalConfirmButton = testId("billing-project-add-user-modal-confirm-button")

    def fillUserEmail(email: String) = {
      await enabled addUserModalEmailInput
      textField(addUserModalEmailInput).value = email
    }

    def selectRole(role: String) = {
      singleSel(addUserModalRoleSelect).value = option value role
    }

    def confirmAddUserDialog() = {
      click on addUserModalConfirmButton
    }
  }
}
