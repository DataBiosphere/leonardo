package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo.cluster.ClusterStatusTransitionsSpec
import org.broadinstitute.dsde.workbench.leonardo.lab.LabSpec
import org.broadinstitute.dsde.workbench.leonardo.notebooks._
import org.broadinstitute.dsde.workbench.leonardo.rstudio.RStudioSpec
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.service.{BillingProject, Orchestration}
import org.scalatest._

import scala.collection.immutable

trait GPAllocFixtureSpec extends fixture.FreeSpecLike {
  val gpallocProjectKey = "leonardo.billingProject"

  override type FixtureParam = GoogleProject
  override def withFixture(test: OneArgTest): Outcome = {
    val billingProject = System.getProperty(gpallocProjectKey)
    withFixture(test.toNoArgTest(GoogleProject(billingProject)))
  }
}

trait GPAllocBeforeAndAfterAll extends GPAllocFixtureSpec with BeforeAndAfterAll with BillingFixtures with LeonardoTestUtils {

  override def beforeAll(): Unit = {
    super.beforeAll()
    val billingProject = claimProject()
    System.setProperty(gpallocProjectKey, billingProject.value)
  }

  override def afterAll(): Unit = {
    val billingProject = System.getProperty(gpallocProjectKey)
    unclaimProject(GoogleProject(billingProject))
    System.clearProperty(gpallocProjectKey)
    super.afterAll()
  }

  /**
    * Claim new billing project by Hermione
    */
  private def claimProject(): GoogleProject = {
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

final class LeonardoSuite extends Suites(
  new PingSpec,
  new ClusterStatusTransitionsSpec,
  new LabSpec,
  new NotebookClusterMonitoringSpec,
  new NotebookCustomizationSpec,
  new NotebookDataSyncingSpec,
  new NotebookHailSpec,
  new NotebookLocalizeFileSpec,
  new NotebookPyKernelSpec,
  new NotebookRKernelSpec,
  new RStudioSpec
) with GPAllocBeforeAndAfterAll