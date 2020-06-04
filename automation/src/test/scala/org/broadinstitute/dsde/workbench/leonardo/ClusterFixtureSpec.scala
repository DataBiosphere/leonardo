package org.broadinstitute.dsde.workbench.leonardo

import java.time.Instant

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.leonardo.GPAllocFixtureSpec._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.scalatest.{fixture, BeforeAndAfterAll, Outcome, Retries}
import cats.implicits._
import org.broadinstitute.dsde.workbench.google2.MachineTypeName

/**
 * trait BeforeAndAfterAll - One cluster per Scalatest Spec.
 */
abstract class ClusterFixtureSpec extends fixture.FreeSpec with BeforeAndAfterAll with LeonardoTestUtils with Retries {

  implicit val ronToken: AuthToken = ronAuthToken

  def toolDockerImage: Option[String] = None
  var ronCluster: ClusterCopy = _
  var clusterCreationFailureMsg: String = ""

  //To use, comment out the lines in after all that clean-up and run the test once normally. Then, instantiate a mock cluster in your test file via the `mockCluster` method in NotebookTestUtils with the project/cluster created
  //You must also set debug to true. Example usage (goes in the Spec you are creating):
  //Consider adding autopause = Some(false) to the cluster request if you encounter issues with autopause
  //
  //example usage:
  //  debug = true
  //  mockedCluster = mockCluster("gpalloc-dev-master-0h7pzni","automation-test-apm25lvlz")
  val debug: Boolean = false //if true, will not spin up and tear down a cluster on each test. Used in conjunction with mockedCluster
  var mockedCluster
    : ClusterCopy = _ //mockCluster("gpalloc-dev-master-0wmkqka", "automation-test-adhkmuanz") //_ //must specify a google project name and cluster name via the mockCluster utility method in NotebookTestUtils

  def mockCluster(googleProject: String, clusterName: String): ClusterCopy =
    ClusterCopy(
      RuntimeName(clusterName),
      GoogleProject(googleProject),
      WorkbenchEmail("fake@gmail.com"),
      RuntimeConfig.DataprocConfig(
        numberOfWorkers = 0,
        masterMachineType = MachineTypeName("n1-standard-4"),
        masterDiskSize = DiskSize(5),
        workerMachineType = None,
        workerDiskSize = None,
        numberOfWorkerLocalSSDs = None,
        numberOfPreemptibleWorkers = None,
        properties = Map.empty
      ),
      ClusterStatus.Running,
      WorkbenchEmail(""),
      Map(),
      None,
      List(),
      Instant.now(),
      false,
      0,
      false
    )

  /**
   * See
   *  https://www.artima.com/docs-scalatest-2.0.M5/org/scalatest/FreeSpec.html
   *   Section: "Overriding withFixture(OneArgTest)"
   *
   * Claim a billing project for project owner
   */
  case class ClusterFixture(cluster: ClusterCopy)

  override type FixtureParam = ClusterFixture

  override def withFixture(test: OneArgTest): Outcome = {
    if (debug) {
      logger.info(s"[Debug] Using mocked cluster for cluster fixture tests")
      ronCluster = mockedCluster
    }

    if (clusterCreationFailureMsg.nonEmpty)
      throw new Exception(clusterCreationFailureMsg)

    def runTestAndCheckOutcome() = {
      val outcome = super.withFixture(test.toNoArgTest(ClusterFixture(ronCluster)))
      if (!outcome.isSucceeded) {
        System.setProperty(shouldUnclaimProjectsKey, "false")
      }
      outcome
    }

    if (isRetryable(test))
      withRetry(runTestAndCheckOutcome())
    else {
      runTestAndCheckOutcome()
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (!debug) {
      sys.props.get(gpallocProjectKey) match {
        case Some(msg) if msg.startsWith(gpallocErrorPrefix) =>
          clusterCreationFailureMsg = msg
        case Some(billingProject) =>
          logger.info(s"Creating cluster for cluster fixture tests: ${getClass.getSimpleName}")

          val runtimeConfig = RuntimeConfigRequestCopy.DataprocConfig(
            numberOfWorkers = Some(0),
            masterDiskSize = Some(100),
            masterMachineType = Some("n1-standard-4"),
            workerMachineType = Some("n1-standard-4"),
            workerDiskSize = None,
            numberOfWorkerLocalSSDs = None,
            numberOfPreemptibleWorkers = None,
            properties = Map.empty
          )
          val request = ClusterRequest(
            machineConfig = Some(runtimeConfig),
            enableWelder = Some(enableWelder),
            toolDockerImage = toolDockerImage,
            autopause = Some(false)
          )
          Either
            .catchNonFatal(createNewCluster(GoogleProject(billingProject), request = request)(ronAuthToken))
            .handleError { e =>
              clusterCreationFailureMsg = e.getMessage
              null
            }
            .map(c => ronCluster = c)
            .void
        case None =>
          clusterCreationFailureMsg = "leonardo.billingProject system property is not set"
      }
    }

  }

  override def afterAll(): Unit = {
    if (!debug) {
      sys.props.get(gpallocProjectKey) match {
        case Some(billingProject) =>
          if (ronCluster != null)
            deleteCluster(GoogleProject(billingProject), ronCluster.clusterName, false)(ronAuthToken)
        case None => throw new RuntimeException("leonardo.billingProject system property is not set")
      }
    }
    super.afterAll()
  }
}
