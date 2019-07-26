package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.leonardo.GPAllocFixtureSpec._
import org.scalatest.{BeforeAndAfterAll, Outcome, fixture}


/**
  * trait BeforeAndAfterAll - One cluster per Scalatest Spec.
  */
abstract class ClusterFixtureSpec extends fixture.FreeSpec with BeforeAndAfterAll with LeonardoTestUtils {

  implicit val ronToken: AuthToken = ronAuthToken

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
  case class ClusterFixture(cluster: Cluster)

  override type FixtureParam = ClusterFixture

  override def withFixture(test: OneArgTest): Outcome = {
    if (debug) {
      logger.info(s"[Debug] Using mocked cluster for cluster fixture tests")
      ronCluster = mockedCluster
    }
    withFixture(test.toNoArgTest(ClusterFixture(ronCluster)))
  }

  /**
    * Create new cluster by Ron with all default settings
    */
  def createRonCluster(billingProject: GoogleProject): Unit = {
    logger.info(s"Creating cluster for cluster fixture tests: ${getClass.getSimpleName}")
    ronCluster = createNewCluster(billingProject, request = getClusterRequest())(ronAuthToken)
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
  def deleteRonCluster(billingProject: GoogleProject, monitoringDelete: Boolean = false): Unit = {
    logger.info(s"Deleting cluster for cluster fixture tests: ${getClass.getSimpleName}")
    deleteCluster(billingProject, ronCluster.clusterName, monitoringDelete)(ronAuthToken)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    logger.info("beforeAll")
    if (!debug) {
      sys.props.get(gpallocProjectKey) match {
        case Some(billingProject) => createRonCluster(GoogleProject(billingProject))
        case None => throw new RuntimeException("leonardo.billingProject system property is not set")
      }
    }

  }

  override def afterAll(): Unit = {
    logger.info("afterAll")
    if (!debug) {
      sys.props.get(gpallocProjectKey) match {
        case Some(billingProject) => deleteRonCluster(GoogleProject(billingProject))
        case None => throw new RuntimeException("leonardo.billingProject system property is not set")
      }
    }
    super.afterAll()
  }

}
