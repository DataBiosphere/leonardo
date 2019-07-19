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

trait GPAllocBeforeAndAfterAll extends BeforeAndAfterAll with BillingFixtures with LeonardoTestUtils { this: TestSuite =>

  var billingProject: GoogleProject = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    billingProject = claimProject()
  }

  override def afterAll(): Unit = {
    unclaimProject(billingProject)
    billingProject = null
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


final class LeonardoSuite extends TestSuite with GPAllocBeforeAndAfterAll with ParallelTestExecution {

  override def nestedSuites: immutable.IndexedSeq[Suite] = {
    Vector(
      new PingSpec(billingProject),
      new ClusterStatusTransitionsSpec(billingProject),
      new LabSpec(billingProject),
      new NotebookClusterMonitoringSpec(billingProject),
      new NotebookCustomizationSpec(billingProject),
      new NotebookDataSyncingSpec(billingProject),
      new NotebookHailSpec(billingProject),
      new NotebookLocalizeFileSpec(billingProject),
      new NotebookPyKernelSpec(billingProject),
      new NotebookRKernelSpec(billingProject),
      new RStudioSpec(billingProject))
    )
  }

}