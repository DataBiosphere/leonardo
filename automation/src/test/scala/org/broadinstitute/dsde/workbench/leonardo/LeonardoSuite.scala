package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo.cluster.{ClusterAutopauseSpec, ClusterStatusTransitionsSpec}
import org.broadinstitute.dsde.workbench.leonardo.notebooks._
import org.broadinstitute.dsde.workbench.leonardo.GPAllocFixtureSpec._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.service.{BillingProject, Orchestration}
import org.scalatest._
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.lab.LabSpec
import org.broadinstitute.dsde.workbench.leonardo.rstudio.RStudioSpec
import org.broadinstitute.dsde.workbench.leonardo.runtimes.{
  RuntimeAutopauseSpec,
  RuntimeCreationDiskSpec,
  RuntimePatchSpec,
  RuntimeStatusTransitionsSpec
}

trait GPAllocFixtureSpec extends fixture.FreeSpecLike with Retries {
  override type FixtureParam = GoogleProject
  override def withFixture(test: OneArgTest): Outcome = {
    def runTestAndCheckOutcome(project: GoogleProject) = {
      val outcome = super.withFixture(test.toNoArgTest(project))
      if (!outcome.isSucceeded) {
        System.setProperty(shouldUnclaimProjectsKey, "false")
      }
      outcome
    }

    sys.props.get(gpallocProjectKey) match {
      case None                                            => throw new RuntimeException("leonardo.billingProject system property is not set")
      case Some(msg) if msg.startsWith(gpallocErrorPrefix) => throw new RuntimeException(msg)
      case Some(billingProject) =>
        if (isRetryable(test))
          withRetry(runTestAndCheckOutcome(GoogleProject(billingProject)))
        else
          runTestAndCheckOutcome(GoogleProject(billingProject))
    }
  }
}
object GPAllocFixtureSpec {
  val gpallocProjectKey = "leonardo.billingProject"
  val shouldUnclaimProjectsKey = "leonardo.shouldUnclaimProjects"
  val gpallocErrorPrefix = "Failed To Claim Project: "
}

trait GPAllocBeforeAndAfterAll extends BeforeAndAfterAll with BillingFixtures with LeonardoTestUtils {
  this: TestSuite =>

  override def beforeAll(): Unit = {
    super.beforeAll()
    Either.catchNonFatal(claimProject()) match {
      case Left(e)               => sys.props.put(gpallocProjectKey, gpallocErrorPrefix + e.getMessage)
      case Right(billingProject) => sys.props.put(gpallocProjectKey, billingProject.value)
    }
  }

  override def afterAll(): Unit = {
    val shouldUnclaim = sys.props.get(shouldUnclaimProjectsKey)
    logger.info(s"Running GPAllocBeforeAndAfterAll afterAll ${shouldUnclaimProjectsKey}: $shouldUnclaim")
    if (shouldUnclaim != Some("false")) {
      sys.props.get(gpallocProjectKey).foreach { billingProject =>
        unclaimProject(GoogleProject(billingProject))
        sys.props.remove(gpallocProjectKey)
      }
    } else logger.info(s"Not going to release project: ${sys.props.get(gpallocProjectKey)} due to error happened")
    super.afterAll()
  }

  /**
   * Claim new billing project by Hermione
   */
  private def claimProject(): GoogleProject = {
    val claimedBillingProject = claimGPAllocProject(hermioneCreds)
    Orchestration.billing.addUserToBillingProject(claimedBillingProject.projectName,
                                                  ronEmail,
                                                  BillingProject.BillingProjectRole.User)(hermioneAuthToken)
    logger.info(s"Billing project claimed: ${claimedBillingProject.projectName}")
    GoogleProject(claimedBillingProject.projectName)
  }

  /**
   * Unclaiming billing project claim by Hermione
   */
  private def unclaimProject(project: GoogleProject): Unit = {
    Orchestration.billing.removeUserFromBillingProject(project.value, ronEmail, BillingProject.BillingProjectRole.User)(
      hermioneAuthToken
    )
    releaseGPAllocProject(project.value, hermioneCreds)
    logger.info(s"Billing project released: ${project.value}")
  }
}

final class LeonardoSuite
    extends Suites(
      new RuntimeCreationDiskSpec,
      new ClusterStatusTransitionsSpec,
      new LabSpec,
//      new NotebookClusterMonitoringSpec,
//      new NotebookCustomizationSpec, //TODO: enabled this...This spec pass locally
      new NotebookDataSyncingSpec,
      new LeoPubsubSpec,
      new ClusterAutopauseSpec,
      new RuntimeAutopauseSpec,
      new RuntimePatchSpec,
      new RuntimeStatusTransitionsSpec,
      new NotebookGCEClusterMonitoringSpec
//      new NotebookGCECustomizationSpec,
//      new NotebookGCEDataSyncingSpec
    )
    with TestSuite
    with GPAllocBeforeAndAfterAll
    with ParallelTestExecution

final class LeonardoTerraDockerSuite
    extends Suites(
      new NotebookAouSpec,
//      new NotebookBioconductorKernelSpec,
      new NotebookGATKSpec,
      new NotebookHailSpec,
      new NotebookPyKernelSpec,
//      new NotebookRKernelSpec,
      new RStudioSpec
    )
    with TestSuite
    with GPAllocBeforeAndAfterAll
    with ParallelTestExecution
