package org.broadinstitute.dsde.firecloud.page.workspaces

import org.broadinstitute.dsde.workbench.service.WorkspaceAccessLevel
import org.broadinstitute.dsde.workbench.service.WorkspaceAccessLevel.WorkspaceAccessLevel
import org.broadinstitute.dsde.workbench.config.{Config => FireCloudConfig}
import org.broadinstitute.dsde.firecloud.page.workspaces.methodconfigs.WorkspaceMethodConfigListPage
import org.broadinstitute.dsde.firecloud.page._
import org.broadinstitute.dsde.workbench.page.PageUtil
import org.openqa.selenium.WebDriver
import org.scalatest.selenium.Page

/**
  * Page class for the Workspace Detail page.
  */
class WorkspaceSummaryPage(namespace: String, name: String)(implicit webDriver: WebDriver) extends WorkspacePage with Page with PageUtil[WorkspaceSummaryPage] {

  override val url: String = s"${FireCloudConfig.FireCloud.baseUrl}#workspaces/$namespace/$name"

  override def awaitLoaded(): WorkspaceSummaryPage = {
    await condition { enabled(testId("submission-status")) || enabled(testId("workspace-details-error")) }
    await spinner "Loading..."
    this
  }

  /**
    * Dictionary of access level labels displayed in the web UI.
    */
  object AccessLevel extends Enumeration {
    type AccessLevel = Value
    val NoAccess = Value("NO ACCESS")
    val Owner = Value("OWNER")
    val Reader = Value("READER")
    val Writer = Value("WRITER")
  }
  import AccessLevel._

  /**
    * Clones the workspace currently being viewed. Returns when the clone
    * operation is complete.
    *
    * @param billingProjectName the billing project for the workspace (aka namespace)
    * @param workspaceName the name for the new workspace
    * @param authDomain the authorization domain for the new workspace
    * @return a WorkspaceSummaryPage for the created workspace
    */
  def cloneWorkspace(billingProjectName: String, workspaceName: String, authDomain: Set[String] = Set.empty): WorkspaceSummaryPage = {
    await notVisible spinner
    val cloneModal = ui.clickCloneButton()
    cloneModal.cloneWorkspace(billingProjectName, workspaceName, authDomain)
    cloneModal.cloneWorkspaceWait()
    cloneWorkspaceWait()
    new WorkspaceSummaryPage(billingProjectName, workspaceName)
  }

  /**
    * Wait for workspace clone to complete.
    *
    * Clone is initiated from the workspace summary page for the source
    * workspace and ends on the workspace summary page for the cloned
    * workspace. WorkspaceSummaryPage.awaitLoaded() will complete even if the
    * browser has not yet navigated to the cloned workspace which could cause
    * subsequent assertions to fail. This extra wait makes sure that the
    * browser has navigated somewhere else.
    */
  def cloneWorkspaceWait(): Unit = {
    await condition { currentUrl != url }
  }

  def unpublishWorkspace(): Unit = {
    ui.clickUnpublishButton()
    val msgModal = new MessageModal
    msgModal.clickOk()
  }

  /**
    * Deletes the workspace currently being viewed. Returns while transitioning
    * to the resulting view after successful deletion.
    */
  def deleteWorkspace(): WorkspaceListPage = {
    val workspaceDeleteModal = ui.clickDeleteWorkspaceButton()
    workspaceDeleteModal.confirmDelete()
    workspaceDeleteModal.confirmDeleteWait()
    new WorkspaceListPage
  }

  /**
    * shares workspace currently being viewed with user email
    * @param email
    * @param accessLevel
    * @param share
    * @param compute
    * @return
    */

  def share(email: String, accessLevel: String, share: Boolean = false, compute: Boolean = false): WorkspaceSummaryPage = {
    await notVisible spinner
    val aclEditor = ui.clickShareWorkspaceButton()
    aclEditor.shareWorkspace(email, WorkspaceAccessLevel.withName(accessLevel), share, compute)
    new WorkspaceSummaryPage(namespace, name)
  }

  def openShareDialog(email: String, accessLevel: String): AclEditor = {
    await notVisible spinner
    val aclEditor = ui.clickShareWorkspaceButton()
    aclEditor.fillEmailAndAccess(email, WorkspaceAccessLevel.withName(accessLevel))
    aclEditor
  }

  trait UI extends super.UI {
    private val authDomainGroups = testId("auth-domain-groups")
    private val authDomainRestrictionMessage = testId("auth-domain-restriction-message")
    private val cloneButton = testId("open-clone-workspace-modal-button")
    private val deleteWorkspaceButtonQuery = testId("delete-workspace-button")
    private val nameHeader = testId("header-name")
    private val publishButtonQuery = testId("publish-button")
    private val unpublishButtonQuery = testId("unpublish-button")
    private val shareWorkspaceButton = testId("share-workspace-button")
    private val workspaceError = testId("workspace-details-error")
    private val accessLevel = testId("workspace-access-level")
    private val methodConfigTab = testId("Method Configurations-tab")

    def clickCloneButton(): CloneWorkspaceModal = {
      click on (await enabled cloneButton)
      new CloneWorkspaceModal
    }

    def clickDeleteWorkspaceButton(): DeleteWorkspaceModal = {
      click on (await enabled deleteWorkspaceButtonQuery)
      new DeleteWorkspaceModal
    }

    def clickPublishButton(): Unit = {
      click on (await enabled publishButtonQuery)
    }

    def clickUnpublishButton(): Unit = {
      click on (await enabled unpublishButtonQuery)
    }

    def clickShareWorkspaceButton(): AclEditor = {
      click on (await enabled shareWorkspaceButton)
      new AclEditor
    }

    def hasShareButton: Boolean = {
      find(shareWorkspaceButton).isDefined
    }

    def hasPublishButton: Boolean = {
      find(publishButtonQuery).isDefined
    }

    def hasUnpublishButton: Boolean = {
      find(unpublishButtonQuery).isDefined
    }

    def hasWorkspaceNotFoundMessage: Boolean = {
      find(withText(s"$namespace/$name does not exist")).isDefined
    }

    def readAuthDomainGroups: String = {
      readText(authDomainGroups)
    }

    def readAuthDomainRestrictionMessage: String = {
      readText(authDomainRestrictionMessage)
    }

    def readError(): String = {
      readText(workspaceError)
    }

    def readWorkspaceName: String = {
      readText(nameHeader)
    }

    def readAccessLevel(): WorkspaceAccessLevel = {
      WorkspaceAccessLevel.withName(readText(accessLevel).toUpperCase)
    }

    def clickMethodConfigTab(namespace: String, name: String): WorkspaceMethodConfigListPage = {
      click on (await enabled methodConfigTab)
      new WorkspaceMethodConfigListPage(namespace, name)
    }
  }
  object ui extends UI
}


class CloneWorkspaceModal(implicit webDriver: WebDriver) extends FireCloudView {

  /**
    * Clones a new workspace. Returns immediately after submitting. Call awaitCloneComplete to wait for cloning to be done.
    *
    * @param workspaceName the name for the new workspace
    * @param billingProjectName the billing project for the workspace
    */
  def cloneWorkspace(billingProjectName: String, workspaceName: String, authDomain: Set[String] = Set.empty): Unit = {
    ui.selectBillingProject(billingProjectName)
    ui.fillWorkspaceName(workspaceName)
    authDomain foreach { ui.selectAuthDomain(_) }

    ui.clickCloneButton()
  }

  def cloneWorkspaceWait(): Unit = {
    // Micro-sleep to make sure the spinner has had a chance to render
    Thread sleep 200
    await notVisible spinner
  }


  object ui {
    private val authDomainSelect = testId("workspace-auth-domain-select")
    private val billingProjectSelect = testId("billing-project-select")
    private val cloneButtonQuery: Query = testId("create-workspace-button")
    private val authDomainGroupsQuery: Query = testId("selected-auth-domain-group")
    private val workspaceNameInput: Query = testId("workspace-name-input")

    def clickCloneButton(): Unit = {
      click on (await enabled cloneButtonQuery)
    }

    def fillWorkspaceName(name: String): Unit = {
      textField(workspaceNameInput).value = name
    }

    def readAuthDomainGroups(): List[(String, Boolean)] = {
      await visible authDomainGroupsQuery

      findAll(authDomainGroupsQuery).map { element =>
        (element.attribute("value").get, element.isEnabled)
      }.toList
    }

    def readLockedAuthDomainGroups(): List[String] = {
      readAuthDomainGroups().filterNot{ case (_, isEnabled) => isEnabled }.map { case (name, _) => name }
    }

    def selectAuthDomain(authDomain: String): Unit = {
      singleSel(authDomainSelect).value = option value authDomain
    }

    def selectBillingProject(billingProjectName: String): Unit = {
      singleSel(billingProjectSelect).value = option value billingProjectName
    }
  }
}


/**
  * Page class for the workspace delete confirmation modal.
  */
class DeleteWorkspaceModal(implicit webDriver: WebDriver) extends FireCloudView {

  /**
    * Confirms the request to delete a workspace. Returns after the FireCloud
    * busy spinner disappears.
    */
  def confirmDelete(): Unit = {
    ui.clickConfirmDeleteButton()
  }

  def confirmDeleteWait(): Unit = {
    // Micro-sleep to make sure the spinner has had a chance to render
    Thread sleep 200
    await notVisible spinner
  }


  object ui {
    private val confirmDeleteButtonQuery: Query = testId("confirm-delete-workspace-button")

    def clickConfirmDeleteButton(): Unit = {
      click on (await enabled confirmDeleteButtonQuery)
    }
  }
}

/**
  * Page class for the Acl Editor modal
  */
class AclEditor(implicit webDriver: WebDriver) extends FireCloudView  {

  def clickOk(): Unit = {
    ui.clickOkButton()
  }

  def clickCancel(): Unit = {
    ui.clickCancelButton()
  }

  /**
    * Shares workspace being viewed.
    * @param email email of user to be shared with
    * @param accessLevel accessLevel to set for user
    * @param share if the Can Share checkbox should be clicked
    * @param compute if the Can Compute checkbox should be clicked
    */
  def shareWorkspace(email: String, accessLevel: WorkspaceAccessLevel, share: Boolean, compute: Boolean): Unit = {
    fillEmailAndAccess(email, accessLevel)
    if (share) {
      ui.clickCanShare()
    }
    if (compute) {
      ui.clickCanCompute()
    }
    ui.clickOkButton()
    await notVisible spinner
  }

  def fillEmailAndAccess(email: String, accessLevel: WorkspaceAccessLevel): Unit = {
    ui.clickAddNewAclButton()
    ui.fillNewAclEmailField(email)
    ui.clickRoleDropdown()
    ui.clickRoleLink(accessLevel)
  }

  def clickDropdown(): Unit = {
    ui.clickRoleDropdown()
  }

  def clickRole(workspaceAccessLevel: WorkspaceAccessLevel): Unit = {
    ui.clickRoleLink(workspaceAccessLevel)
  }

  object ui {
    private val okButton: Query = testId("ok-button")
    private val cancelButton: Query = testId("cancel-button")
    private val addNewAclButton: Query = testId("add-new-acl-button")
    private val newAclEmailField: Query = testId("acl-add-email")
    private val roleDropdown: Query = testId("role-dropdown-true")
    private val canShareBox: Query = testId("acl-share-true")
    private val canComputeBox: Query = testId("acl-compute-true")
    //TODO: add more here for multiple user save
    def clickOkButton(): Unit = {
      click on (await enabled okButton)
    }
    def clickCancelButton(): Unit = {
      click on (await enabled cancelButton)
    }
    def clickAddNewAclButton(): Unit = {
      click on (await enabled addNewAclButton)
    }
    def fillNewAclEmailField(email: String): Unit = {
      emailField(ui.newAclEmailField).value = email
    }

    def clickRoleDropdown(): Unit = {
      click on (await enabled roleDropdown)
    }

    def clickRoleLink(workspaceAccessLevel: WorkspaceAccessLevel): Unit = {
      val role = workspaceAccessLevel.toString
      singleSel(roleDropdown).value = option value role
    }

    def clickCanShare(): Unit = {
      click on (await enabled canShareBox)
    }

    def clickCanCompute(): Unit = {
      click on (await enabled canComputeBox)
    }

    def canComputeEnabled(): Boolean = {
      enabled(canComputeBox)
    }

    def canComputeChecked(): Boolean = {
      checkbox(canComputeBox).isSelected
    }
  }
}
