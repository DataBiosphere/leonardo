package org.broadinstitute.dsde.workbench.leonardo

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.fixture._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.service.Orchestration
import org.broadinstitute.dsde.workbench.service.test.RandomUtil
import org.scalatest.{BeforeAndAfterAll, Outcome, fixture}

import scala.util.{Failure, Success, Try}


/**
  * trait BeforeAndAfterAll - One cluster, billing project per Scalatest Spec.
  */
abstract class ClusterFixtureSpec extends fixture.FreeSpec with BeforeAndAfterAll with LeonardoTestUtils with BillingFixtures with RandomUtil with LazyLogging {

  implicit val ronToken: AuthToken = ronAuthToken
  var claimedBillingProject: ClaimedProject = _
  var billingProject : GoogleProject = _
  var ronCluster: Cluster = _

  /**
    * See
    *  https://www.artima.com/docs-scalatest-2.0.M5/org/scalatest/FreeSpec.html
    *   Section: "Overriding withFixture(OneArgTest)"
    *
    * Claim a billing project for project owner
    * @param billingProject
    */
  case class ClusterFixture(billingProject: GoogleProject, cluster: Cluster)

  type FixtureParam = ClusterFixture

  override def withFixture(test: OneArgTest): Outcome = {
    withFixture(test.toNoArgTest(ClusterFixture(billingProject, ronCluster)))
  }

  /**
    * Claim new billing project by Hermione
    */
  def claimBillingProject(): Unit = {
    Try {
      claimedBillingProject = claimGPAllocProject(hermioneCreds)
      billingProject = GoogleProject(claimedBillingProject.projectName)
      logger.info(s"Billing project claimed: ${claimedBillingProject.projectName}")
    }.recover {
      case ex: Exception =>
        logger.error(s"ERROR: when owner $hermioneCreds is claiming a billing project", ex)
        throw ex
    }
  }

  /**
    * Unclaiming billing project claim by Hermione
    */
  def unclaimBillingProject(): Unit = {
    val projectName = claimedBillingProject.projectName
    Try {
      claimedBillingProject.cleanup(hermioneCreds)
      logger.info(s"Billing project unclaimed: $projectName")
    }.recover{
      case ex: Exception =>
        logger.error(s"ERROR: when owner $hermioneCreds is unclaiming billing project $projectName", ex)
        throw ex
    }
  }

  /**
    * Create new cluster by Ron with all default settings
    */
  def createRonCluster(): Unit = {
    Orchestration.billing.addUserToBillingProject(billingProject.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)
    val highMemClusterRequest = ClusterRequest(machineConfig = Option(MachineConfig(
      masterMachineType = Option("n1-standard-8"),
      workerMachineType = Option("n1-standard-8")
    )))
    Try (createNewCluster(billingProject, request = highMemClusterRequest)(ronAuthToken)) match {
      case Success(outcome) =>
        ronCluster = outcome
        logger.info(s"Successfully created cluster $ronCluster")
      case Failure(ex) =>
        logger.error(s"ERROR: when creating new cluster in billing project $billingProject", ex)
        unclaimBillingProject()
        throw  ex
    }
  }

  /**
    * Delete cluster without monitoring that's owned by Ron
    */
  def deleteRonCluster(monitoringDelete: Boolean = false): Unit = {
    deleteCluster(billingProject, ronCluster.clusterName, monitoringDelete)(ronAuthToken)
  }

  override def beforeAll(): Unit = {
    logger.info("beforeAll")
    super.beforeAll()
    claimBillingProject()
    createRonCluster()
  }

  override def afterAll(): Unit = {
    logger.info("afterAll")
    deleteRonCluster()
    unclaimBillingProject()
    super.afterAll()
  }

}
