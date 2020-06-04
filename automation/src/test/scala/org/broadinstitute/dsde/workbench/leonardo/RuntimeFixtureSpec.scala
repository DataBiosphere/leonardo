package org.broadinstitute.dsde.workbench.leonardo

import cats.implicits._
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.GPAllocFixtureSpec._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.{fixture, BeforeAndAfterAll, Outcome, Retries}
import RuntimeFixtureSpec._

/**
 * trait BeforeAndAfterAll - One cluster per Scalatest Spec.
 */
abstract class RuntimeFixtureSpec extends fixture.FreeSpec with BeforeAndAfterAll with LeonardoTestUtils with Retries {

  implicit val ronToken: AuthToken = ronAuthToken

  def toolDockerImage: Option[String] = None
  var ronCluster: ClusterCopy = _
  var clusterCreationFailureMsg: String = ""

  /**
   * See
   *  https://www.artima.com/docs-scalatest-2.0.M5/org/scalatest/FreeSpec.html
   *   Section: "Overriding withFixture(OneArgTest)"
   *
   * Claim a billing project for project owner
   */
  case class ClusterFixture(runtime: ClusterCopy)

  override type FixtureParam = ClusterFixture

  override def withFixture(test: OneArgTest): Outcome = {

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

  /**
   * Create new runtime by Ron with all default settings
   */
  def createRonRuntime(billingProject: GoogleProject): Unit = {
    logger.info(s"Creating cluster for cluster fixture tests: ${getClass.getSimpleName}")
    ronCluster =
      createNewRuntime(billingProject, request = getRuntimeRequest(CloudService.GCE, toolDockerImage))(ronAuthToken)
  }

  /**
   * Delete cluster without monitoring that's owned by Ron
   */
  def deleteRonRuntime(billingProject: GoogleProject, monitoringDelete: Boolean = false): Unit = {
    logger.info(s"Deleting cluster for cluster fixture tests: ${getClass.getSimpleName}")
    deleteRuntime(billingProject, ronCluster.clusterName, monitoringDelete)(ronAuthToken)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    logger.info("beforeAll")

    sys.props.get(gpallocProjectKey) match {
      case Some(msg) if msg.startsWith(gpallocErrorPrefix) =>
        clusterCreationFailureMsg = msg
      case Some(billingProject) =>
        Either.catchNonFatal(createRonRuntime(GoogleProject(billingProject))).handleError { e =>
          clusterCreationFailureMsg = e.getMessage
          ronCluster = null
        }
      case None =>
        clusterCreationFailureMsg = "leonardo.billingProject system property is not set"
    }

  }

  override def afterAll(): Unit = {
    logger.info("afterAll")

    sys.props.get(gpallocProjectKey) match {
      case Some(billingProject) => deleteRonRuntime(GoogleProject(billingProject))
      case None                 => throw new RuntimeException("leonardo.billingProject system property is not set")
    }

    super.afterAll()
  }
}

object RuntimeFixtureSpec {
  def getRuntimeRequest(cloudService: CloudService, toolDockerImage: Option[String]): RuntimeRequest = {

    val machineConfig = cloudService match {
      case CloudService.GCE =>
        RuntimeConfigRequestCopy.GceConfig(
          machineType = Some("n1-standard-4"),
          diskSize = Some(500)
        )
      case CloudService.Dataproc =>
        RuntimeConfigRequestCopy.DataprocConfig(
          numberOfWorkers = Some(0),
          masterDiskSize = Some(500),
          masterMachineType = Some("n1-standard-8"),
          workerMachineType = Some("n1-standard-8"),
          workerDiskSize = None,
          numberOfWorkerLocalSSDs = None,
          numberOfPreemptibleWorkers = None,
          properties = Map.empty
        )

    }

    RuntimeRequest(
      runtimeConfig = Some(machineConfig),
      toolDockerImage = toolDockerImage,
      autopause = Some(false)
    )
  }
}
