package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.{Deferred, IO}
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.google2.{MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.BillingProjectFixtureSpec._
import org.broadinstitute.dsde.workbench.leonardo.RuntimeFixtureSpec._
import org.broadinstitute.dsde.workbench.leonardo.TestUser.{getAuthTokenAndAuthorization, Ron}
import org.broadinstitute.dsde.workbench.leonardo.http.{CreateRuntimeRequest, RuntimeConfigRequest}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.scalatest.freespec.{FixtureAnyFreeSpec, FixtureAnyFreeSpecLike}
import org.scalatest.{
  BeforeAndAfterAll,
  BeforeAndAfterEachTestData,
  FixedThreadPoolParallelExecution,
  Outcome,
  ParallelTestExecution,
  Retries,
  TestData
}

/**
 * trait BeforeAndAfterAll - One cluster per Scalatest Spec.
 */

trait RuntimeFixtureSpec
    extends FixtureAnyFreeSpecLike
    with BeforeAndAfterAll
    with LeonardoTestUtils
    with Retries
    with BeforeAndAfterEachTestData
    with FixedThreadPoolParallelExecution {

  implicit val (ronAuthToken: IO[AuthToken], ronAuthorization: IO[Authorization]) = getAuthTokenAndAuthorization(Ron)

  def toolDockerImage: Option[String] = None
  def welderRegistry: Option[ContainerRegistry] = None
  def cloudService: Option[CloudService] = Some(CloudService.GCE)

  var clusterCreationFailureMsg: String = ""

  def runtimeSystemKey: Option[String] = None

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
      val project = GoogleProject(sys.props.get(googleProjectKey).get)
      val runtimeName = getRuntimeName
      val runtime = LeonardoApiClient.client
        .use { c =>
          implicit val client: Client[IO] = c
          LeonardoApiClient.getRuntime(project, runtimeName)
        }
        .map { getRuntimeResponse =>
          ClusterCopy(
            runtimeName,
            project,
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
      logger.info(s"in run test and check outcome for spec: ${getClass.getSimpleName}, runtime: ${runtimeName}")

      val outcome = super.withFixture(
        test.toNoArgTest(
          ClusterFixture(
            runtime.unsafeRunSync()
          )
        )
      )
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
  // TODO: rename this function and the cluster name
  def createRonRuntime(billingProject: GoogleProject): Unit = {

    // TODO: use spec-based name
    val runtimeName = randomClusterName
    logger.info(
      s"Creating cluster for cluster fixture tests: ${getClass.getSimpleName}, runtime to be created: ${billingProject.value}/${runtimeName.asString}"
    )
    val res = LeonardoApiClient.client.use { c =>
      implicit val client: Client[IO] = c
      for {
        getRuntimeResponse <- LeonardoApiClient.createRuntimeWithWait(
          billingProject,
          runtimeName,
          getRuntimeRequest(cloudService.getOrElse(CloudService.GCE),
                            toolDockerImage.map(i => ContainerImage(i, ContainerRegistry.GCR)),
                            welderRegistry
          )
        )
        _ = logger.info(s"before set, runtimeSystemKey: ${runtimeSystemKey}")
        _ <- IO(
          sys.props.put(
            runtimeSystemKey.getOrElse(
              throw new RuntimeException(s"must override runtimeSystemKey in class ${getClass.getSimpleName}")
            ),
            runtimeName.asString
          )
        )
      } yield getRuntimeResponse
    }

    val resp = res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    logger.info(s"Created cluster for cluster fixture tests: ${getClass.getSimpleName}, runtime ${resp}")
  }

  /**
   * Delete cluster without monitoring that's owned by Ron
   */
  def deleteRonRuntime(billingProject: GoogleProject,
                       runtimeName: RuntimeName,
                       monitoringDelete: Boolean = false
  ): Unit = {
    logger.info(s"Deleting cluster for cluster fixture tests: ${getClass.getSimpleName}")
    // TODO: Remove unsafeRunSync() when deleteRuntime() accepts an IO[AuthToken]
    deleteRuntime(
      billingProject,
      runtimeName,
      monitoringDelete
    )(ronAuthToken.unsafeRunSync())
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    logger.info(s"beforeAll in runtimeFixture for ${getClass.getSimpleName}")

    sys.props.get(googleProjectKey) match {
      case Some(msg) if msg.startsWith(createBillingProjectErrorPrefix) =>
        clusterCreationFailureMsg = msg
      case Some(googleProjectId) =>
        createRonRuntime(GoogleProject(googleProjectId))
      case None =>
        clusterCreationFailureMsg = "leonardo.googleProject system property is not set"
    }
    logger.info(s"end of beforeall in runtimeFixture for ${getClass.getSimpleName}")
  }
  import java.time.LocalDateTime
  override def beforeEach(testData: TestData): Unit = {
    super.beforeEach(testData)
    logger.info(
      s"Start time for test ${testData.name} in suite ${getClass.getSimpleName}: ${LocalDateTime.now()}"
    )
  }

  override def afterEach(testData: TestData): Unit = {
    super.afterEach(testData)
    logger.info(s"End time for test ${testData.name} in suite ${getClass.getSimpleName}: ${LocalDateTime.now()}")
  }

  def getRuntimeName(): RuntimeName =
    RuntimeName(
      sys.props
        .get(
          runtimeSystemKey
            .getOrElse(throw new RuntimeException(s"must override runtimeSystemKey in class ${getClass.getSimpleName}"))
        )
        .getOrElse(throw new RuntimeException(s"runtime name not defined for spec ${getClass.getSimpleName}"))
    )
  override def afterAll(): Unit = {
    logger.info(s"afterAll in runtimeFixture for ${getClass.getSimpleName}")

    sys.props.get(googleProjectKey) match {
      case Some(billingProject) => deleteRonRuntime(GoogleProject(billingProject), getRuntimeName)
      case None                 => throw new RuntimeException("leonardo.googleProject system property is not set")
    }

    super.afterAll()
  }
}

object RuntimeFixtureSpec {
  // TODO: can we delete this? why are our automation tests simulating env vars used by terra UI? I hope none of our tests depend on this?
  // Simulate custom environment variables set by Terra UI
  val runtimeFixtureZone = ZoneName("us-east1-c")
  def getCustomEnvironmentVariables: Map[String, String] =
    Map(
      "WORKSPACE_NAME" -> sys.props.getOrElse(workspaceNameKey, "workspace"),
      "WORKSPACE_NAMESPACE" -> sys.props.getOrElse(workspaceNamespaceKey, "workspace-namespace"),
      "WORKSPACE_BUCKET" -> "workspace-bucket",
      "GOOGLE_PROJECT" -> sys.props.getOrElse(googleProjectKey, "google-project")
    )

  def getRuntimeRequest(cloudService: CloudService,
                        toolDockerImage: Option[ContainerImage],
                        welderRegistry: Option[ContainerRegistry]
  ): CreateRuntimeRequest = {
    val machineConfig = cloudService match {
      case CloudService.GCE =>
        RuntimeConfigRequest.GceConfig(
          machineType = Some(MachineTypeName("n1-standard-4")),
          diskSize = Some(DiskSize(100)),
          Some(runtimeFixtureZone),
          None
        )
      case CloudService.Dataproc =>
        RuntimeConfigRequest.DataprocConfig(
          numberOfWorkers = Some(0),
          masterDiskSize = Some(DiskSize(130)),
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
      case CloudService.AzureVm =>
        throw new NotImplementedError()
    }

    CreateRuntimeRequest(
      runtimeConfig = Some(machineConfig),
      toolDockerImage = toolDockerImage,
      autopause = Some(false),
      labels = Map.empty,
      userScriptUri = None,
      startUserScriptUri = None,
      userJupyterExtensionConfig = None,
      autopauseThreshold = None,
      defaultClientId = None,
      welderRegistry = welderRegistry,
      scopes = Set.empty,
      customEnvironmentVariables = getCustomEnvironmentVariables,
      checkToolsInterruptAfter = None
    )
  }
}
