package org.broadinstitute.dsde.workbench.leonardo

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.fixture._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.service.test.RandomUtil
import org.broadinstitute.dsde.workbench.service.{BillingProject, Orchestration}
import org.scalatest.{BeforeAndAfterAll, Outcome, fixture}

import scala.util.{Failure, Success, Try}


/**
  * trait BeforeAndAfterAll - One cluster, billing project per Scalatest Spec.
  */
abstract class ClusterFixtureSpec extends fixture.FreeSpec with BeforeAndAfterAll with LeonardoTestUtils with BillingFixtures with RandomUtil with LazyLogging {

  implicit val ronToken: AuthToken = ronAuthToken
  var claimedBillingProject: ClaimedProject = _
  var billingProject : GoogleProject = LeonardoConfig.BillingProject.project
  var ronCluster: Cluster = _

  //To use, comment out the lines in after all that clean-up and run the test once normally. Then, instantiate a mock cluster in your test file via the `mockCluster` method in NotebookTestUtils with the project/cluster created
  //You must also set debug to true. Example usage (goes in the Spec you are creating):
  //Consider adding autopause = Some(false) to the cluster request if you encounter issues with autopause 
  //
  //example usage:
  //  debug = true
  //  mockedCluster = mockCluster("gpalloc-dev-master-0h7pzni","automation-test-apm25lvlz")
  val debug: Boolean = false //if true, will not spin up and tear down a cluster on each test. Used in conjunction with mockedCluster
  var mockedCluster: Cluster = _ //mockCluster("gpalloc-dev-master-1ecxlpm", "automation-test-auhyfvadz") //_ //must specify a google project name and cluster name via the mockCluster utility method in NotebookTestUtils

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
    if (debug) {
      logger.info("[Debug] Using mocked cluster for cluster fixture tests")
      billingProject = mockedCluster.googleProject
      ronCluster = mockedCluster
    }
    withFixture(test.toNoArgTest(ClusterFixture(billingProject, ronCluster)))
  }

  /**
    * Claim new billing project by Hermione
    */
//  def claimBillingProject(): Unit = {
//    Try {
//      val jitter = addJitter(5 seconds, 1 minute)
//      logger.info(s"Sleeping ${jitter.toSeconds} seconds before claiming a billing project")
//      Thread sleep jitter.toMillis
//
//      claimedBillingProject = claimGPAllocProject(hermioneCreds)
//      billingProject = GoogleProject(claimedBillingProject.projectName)
//      logger.info(s"Billing project claimed: ${claimedBillingProject.projectName}")
//    }.recover {
//      case ex: Exception =>
//        logger.error(s"ERROR: when owner $hermioneCreds is claiming a billing project", ex)
//        throw ex
//    }
//  }

  /**
    * Unclaiming billing project claim by Hermione
    */
//  def unclaimBillingProject(): Unit = {
//    val projectName = claimedBillingProject.projectName
//    Try {
//      claimedBillingProject.cleanup(hermioneCreds)
//      logger.info(s"Billing project unclaimed: $projectName")
//    }.recover{
//      case ex: Exception =>
//        logger.error(s"ERROR: when owner $hermioneCreds is unclaiming billing project $projectName", ex)
//        throw ex
//    }
//  }

  /**
    * Create new cluster by Ron with all default settings
    */
  def createRonCluster(): Unit = {
    Orchestration.billing.addUserToBillingProject(billingProject.value, ronEmail, BillingProject.BillingProjectRole.User)(hermioneAuthToken)

    Try (createNewCluster(billingProject, request = getClusterRequest())(ronAuthToken)) match {
      case Success(outcome) =>
        ronCluster = outcome
        logger.info(s"Successfully created cluster $ronCluster")
      case Failure(ex) =>
        logger.error(s"ERROR: when creating new cluster in billing project $billingProject", ex)
//        unclaimBillingProject()
        throw  ex
    }
  }

  def getClusterRequest(): ClusterRequest = {
    val machineConfig = Some(MachineConfig(
      masterMachineType = Some("n1-standard-8"),
      workerMachineType = Some("n1-standard-8")
    ))

    ClusterRequest(
      machineConfig = machineConfig,
      enableWelder = Some(enableWelder),
      autopause = Some(false))
  }

  /**
    * Delete cluster without monitoring that's owned by Ron
    */
  def deleteRonCluster(monitoringDelete: Boolean = false): Unit = {
    deleteCluster(billingProject, ronCluster.clusterName, monitoringDelete)(ronAuthToken)
  }

  override def beforeAll(): Unit = {
    logger.info("beforeAll")
    if (!debug) {
      //claimBillingProject()
      createRonCluster()
    }
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    logger.info("afterAll")
    if (!debug) {
      deleteRonCluster()
      //unclaimBillingProject()
    }
    super.afterAll()
  }

}
