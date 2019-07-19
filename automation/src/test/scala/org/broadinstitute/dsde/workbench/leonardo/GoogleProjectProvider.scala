package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.auth.{AuthTokenScopes, UserAuthToken}
import org.broadinstitute.dsde.workbench.config.Credentials
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.service.{BillingProject, Orchestration}
import org.scalatest.{BeforeAndAfterAll, Suite}


trait GoogleProjectProvider extends BeforeAndAfterAll { this: Suite with BillingFixtures =>

  // Ron and Hermione are on the dev Leo whitelist, and Hermione is a Project Owner
  lazy val ronCreds: Credentials = LeonardoConfig.Users.NotebooksWhitelisted.getUserCredential("ron")
  lazy val hermioneCreds: Credentials = LeonardoConfig.Users.NotebooksWhitelisted.getUserCredential("hermione")

  lazy val ronAuthToken = UserAuthToken(ronCreds, AuthTokenScopes.userLoginScopes)
  lazy val hermioneAuthToken = UserAuthToken(hermioneCreds, AuthTokenScopes.userLoginScopes)
  lazy val ronEmail = ronCreds.email

  def billingProject = GoogleProjectProvider.getOrClaimProject(claimProject)

  override def beforeAll(): Unit = {
    GoogleProjectProvider.getOrClaimProject(claimProject)
  }

  override def afterAll(): Unit = {
    GoogleProjectProvider.unclaimProjectIfPresent(unclaimProject)
  }

  /**
    * Claim new billing project by Hermione
    */
  private def claimProject: GoogleProject = {
    val claimedBillingProject = claimGPAllocProject(hermioneCreds)
    Orchestration.billing.addUserToBillingProject(claimedBillingProject.projectName, ronEmail, BillingProject.BillingProjectRole.User)(hermioneAuthToken)
    logger.info(s"Billing project claimed: ${claimedBillingProject.projectName}")
    GoogleProject(claimedBillingProject.projectName)
  }

  /**
    * Unclaiming billing project claim by Hermione
    */
  private def unclaimProject(project: GoogleProject): Unit = {
    Orchestration.billing.removeUserFromBillingProject(project.value, ronEmail, BillingProject.BillingProjectRole.User)(hermioneAuthToken)
    releaseGPAllocProject(project.value, hermioneCreds)
    logger.info(s"Billing project released: ${project.value}")
  }

}

object GoogleProjectProvider {
  private var project: GoogleProject = _

  private def getOrClaimProject(claimProject: => GoogleProject): GoogleProject = {
    if (project == null) {
      this.synchronized {
        if (project == null) {
          project = claimProject
        }
      }
    }
    project
  }

  private def unclaimProjectIfPresent(unclaimProject: GoogleProject => Unit): Unit = {
    if (project != null) {
      this.synchronized {
        if (project != null) {
          unclaimProject(project)
          project = null
        }
      }
    }
  }
}
