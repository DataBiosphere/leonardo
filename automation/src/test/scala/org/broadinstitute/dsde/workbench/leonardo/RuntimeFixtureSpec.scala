package org.broadinstitute.dsde.workbench.leonardo

import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.GPAllocFixtureSpec._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.{BeforeAndAfterAll, Outcome, Retries}
import RuntimeFixtureSpec._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.TestUser.{getAuthTokenAndAuthorization, Ron}
import org.broadinstitute.dsde.workbench.leonardo.http.{CreateRuntime2Request, RuntimeConfigRequest}
import org.http4s.client.Client
import org.scalatest.freespec.FixtureAnyFreeSpec

/**
 * trait BeforeAndAfterAll - One cluster per Scalatest Spec.
 */
abstract class RuntimeFixtureSpec
    extends FixtureAnyFreeSpec
    with BeforeAndAfterAll
    with LeonardoTestUtils
    with Retries {

  implicit val (ronAuthToken, ronAuthorization) = getAuthTokenAndAuthorization(Ron)

  def toolDockerImage: Option[String] = None
  def welderRegistry: Option[ContainerRegistry] = None
  def cloudService: Option[CloudService] = Some(CloudService.GCE)
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

  override def withFixture(test: NoArgTest) =
    if (isRetryable(test))
      withRetry(super.withFixture(test))
    else
      super.withFixture(test)

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

    val runtimeName = randomClusterName
    val res = LeonardoApiClient.client.use { c =>
      implicit val client: Client[IO] = c
      for {
        getRuntimeResponse <- LeonardoApiClient.createRuntimeWithWait(
          billingProject,
          runtimeName,
          getRuntimeRequest(cloudService.getOrElse(CloudService.GCE),
                            toolDockerImage.map(i => ContainerImage(i, ContainerRegistry.GCR)),
                            welderRegistry)
        )
      } yield {
        ronCluster = ClusterCopy(
          runtimeName,
          billingProject,
          getRuntimeResponse.serviceAccount,
          null,
          null,
          getRuntimeResponse.auditInfo.creator,
          null,
          null,
          null,
          null,
          15,
          false
        )
      }
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  /**
   * Delete cluster without monitoring that's owned by Ron
   */
  def deleteRonRuntime(billingProject: GoogleProject, monitoringDelete: Boolean = false): Unit = {
    logger.info(s"Deleting cluster for cluster fixture tests: ${getClass.getSimpleName}")
    // TODO: Remove unsafeRunSync() when deleteRuntime() accepts an IO[AuthToken]
    deleteRuntime(billingProject, ronCluster.clusterName, monitoringDelete)(ronAuthToken.unsafeRunSync())
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    logger.info("beforeAll")

    sys.props.get(googleProjectKey) match {
      case Some(msg) if msg.startsWith(gpallocErrorPrefix) =>
        clusterCreationFailureMsg = msg
      case Some(googleProjectId) =>
        createRonRuntime(GoogleProject(googleProjectId))
      case None =>
        clusterCreationFailureMsg = "leonardo.googleProject system property is not set"
    }

  }

  override def afterAll(): Unit = {
    logger.info("afterAll")

    sys.props.get(googleProjectKey) match {
      case Some(billingProject) => deleteRonRuntime(GoogleProject(billingProject))
      case None                 => throw new RuntimeException("leonardo.googleProject system property is not set")
    }

    super.afterAll()
  }
}

object RuntimeFixtureSpec {
  // Simulate custom environment variables set by Terra UI
  def getCustomEnvironmentVariables: Map[String, String] =
    Map(
      "WORKSPACE_NAME" -> sys.props.getOrElse(workspaceNameKey, "workspace"),
      "WORKSPACE_NAMESPACE" -> sys.props.getOrElse(workspaceNamespaceKey, "workspace-namespace"),
      "WORKSPACE_BUCKET" -> "workspace-bucket",
      "GOOGLE_PROJECT" -> sys.props.getOrElse(googleProjectKey, "google-project")
    )

  def getRuntimeRequest(cloudService: CloudService,
                        toolDockerImage: Option[ContainerImage],
                        welderRegistry: Option[ContainerRegistry]): CreateRuntime2Request = {
    val machineConfig = cloudService match {
      case CloudService.GCE =>
        RuntimeConfigRequest.GceConfig(
          machineType = Some(MachineTypeName("n1-standard-4")),
          diskSize = Some(DiskSize(100)),
          None,
          None
        )
      case CloudService.Dataproc =>
        RuntimeConfigRequest.DataprocConfig(
          numberOfWorkers = Some(0),
          masterDiskSize = Some(DiskSize(100)),
          masterMachineType = Some(MachineTypeName("n1-standard-8")),
          workerMachineType = Some(MachineTypeName("n1-standard-8")),
          workerDiskSize = None,
          numberOfWorkerLocalSSDs = None,
          numberOfPreemptibleWorkers = None,
          properties = Map.empty,
          region = None,
          componentGatewayEnabled = true,
          workerPrivateAccess = false
        )

    }

    CreateRuntime2Request(
      runtimeConfig = Some(machineConfig),
      toolDockerImage = toolDockerImage,
      labels = Map.empty,
      userScriptUri = None,
      startUserScriptUri = None,
      userJupyterExtensionConfig = None,
      autopause = None,
      autopauseThreshold = None,
      defaultClientId = None,
      welderRegistry = welderRegistry,
      scopes = Set.empty,
      customEnvironmentVariables = getCustomEnvironmentVariables
    )
  }
}
