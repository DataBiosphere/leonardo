package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.IO
import cats.effect.std.UUIDGen
import cats.syntax.all._
import com.typesafe.scalalogging.LazyLogging
import io.circe.parser._
import org.broadinstitute.dsde.rawls.model.WorkspaceName
import org.broadinstitute.dsde.workbench.auth.AuthTokenScopes.billingScopes
import org.broadinstitute.dsde.workbench.config.ServiceTestConfig
import org.broadinstitute.dsde.workbench.leonardo.BillingProjectFixtureSpec._
import org.broadinstitute.dsde.workbench.pipeline.TestUser.{Hermione, Ron}
// import org.broadinstitute.dsde.workbench.leonardo.TestUser.{Hermione, Ron}
import org.broadinstitute.dsde.workbench.leonardo.azure.{AzureAutopauseSpec, AzureDiskSpec, AzureRuntimeSpec}
import org.broadinstitute.dsde.workbench.leonardo.lab.LabSpec
import org.broadinstitute.dsde.workbench.leonardo.notebooks._
import org.broadinstitute.dsde.workbench.leonardo.rstudio.RStudioSpec
import org.broadinstitute.dsde.workbench.leonardo.runtimes._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.pipeline.Pipeline.BILLING_PROJECT
import org.broadinstitute.dsde.workbench.service.BillingProject.BillingProjectRole
import org.broadinstitute.dsde.workbench.service.{Orchestration, Rawls}
import org.scalatest._
import org.scalatest.freespec.FixtureAnyFreeSpecLike
import org.http4s.headers.Authorization

import java.util.UUID

trait BillingProjectFixtureSpec extends FixtureAnyFreeSpecLike with Retries with LazyLogging {
  override type FixtureParam = GoogleProject
  override def withFixture(test: OneArgTest): Outcome = {
    def runTestAndCheckOutcome(project: GoogleProject) = {
      val outcome = super.withFixture(test.toNoArgTest(project))
      if (!outcome.isSucceeded) {
        System.setProperty(shouldUnclaimProjectsKey, "false")
      }
      outcome
    }

    sys.props.get(googleProjectKey) match {
      case None => throw new RuntimeException("leonardo.googleProject system property is not set")
      case Some(msg) if msg.startsWith(createBillingProjectErrorPrefix) => throw new RuntimeException(msg)
      case Some(googleProjectId) =>
        if (isRetryable(test))
          withRetry(runTestAndCheckOutcome(GoogleProject(googleProjectId)))
        else
          runTestAndCheckOutcome(GoogleProject(googleProjectId))
    }
  }
}
object BillingProjectFixtureSpec {
  val googleProjectKey = "leonardo.googleProject"
  val workspaceNamespaceKey = "leonardo.workspaceNamespace"
  val workspaceNameKey = "leonardo.workspaceName"
  val shouldUnclaimProjectsKey = "leonardo.shouldUnclaimProjects"
  val createBillingProjectErrorPrefix = "Failed to create new billing project and workspace: "
  val initalRuntimeName = RuntimeName("initial-runtime")
  val proxyRedirectServerPortKey = "proxyRedirectServerPort"
}

case class GoogleProjectAndWorkspaceName(
  googleProject: GoogleProject,
  workspaceName: WorkspaceName
)

trait BillingProjectUtils extends LeonardoTestUtils {
  this: TestSuite =>

  /**
   * Create a new billing project by Hermione
   */
  protected def createBillingProjectAndWorkspace: IO[GoogleProjectAndWorkspaceName] =
    for {
      hermioneAuthToken <- Hermione.authToken(billingScopes)
      randomSuffix <- UUIDGen.randomString[IO].map(_.replace("-", ""))
      billingProjectName = ("leonardo-test-" + randomSuffix).take(30)

      _ <- IO {
        Orchestration.billingV2.createBillingProject(
          billingProjectName,
          billingInformation = Left(ServiceTestConfig.Projects.billingAccountId)
        )(hermioneAuthToken)
      }

      _ <- loggerIO.info(s"Billing project claimed: ${billingProjectName}")
      _ <- IO {
        Orchestration.billingV2.addUserToBillingProject(
          billingProjectName,
          Ron.email,
          BillingProjectRole.User
        )(hermioneAuthToken)
      }

      workspaceName = randomIdWithPrefix("leo-leonardo-test-workspace-")
      ronAuthToken <- Ron.authToken()
      workspaceDetails <- IO {
        Orchestration.workspaces.create(billingProjectName, workspaceName)(ronAuthToken)
        Rawls.workspaces.getWorkspaceDetails(billingProjectName, workspaceName)(ronAuthToken)
      }

      json <- IO.fromEither(parse(workspaceDetails))
      googleProjectOpt = json.hcursor.downField("workspace").get[String]("googleProject").toOption
      googleProjectId <- IO.fromOption(googleProjectOpt)(
        new Exception(s"Could not get googleProject from workspace $workspaceName")
      )
    } yield GoogleProjectAndWorkspaceName(
      GoogleProject(googleProjectId),
      WorkspaceName(billingProjectName, workspaceName)
    )

  /**
   * Clean up billing project and resources
   */
  protected def deleteWorkspaceAndBillingProject(workspaceName: WorkspaceName): IO[Unit] =
    for {
      hermioneAuthToken <- Hermione.authToken()
      ronAuthToken <- Ron.authToken()
      releaseProject <- IO {
        Orchestration.workspaces.delete(workspaceName.namespace, workspaceName.name)(ronAuthToken)
        Orchestration.billingV2.deleteBillingProject(workspaceName.namespace)(hermioneAuthToken)
      }.attempt

      _ <- releaseProject match {
        case Left(e) => loggerIO.warn(e)(s"Failed to delete billing project: ${workspaceName.namespace}")
        case _       => loggerIO.info(s"Billing project deleted: ${workspaceName.namespace}")
      }
    } yield ()

  def withNewProject[T](testCode: GoogleProject => IO[T]): T = {
    val test = for {
      _ <- loggerIO.info("Allocating a new single-test project")
      googleProjectAndWorkspaceName <- createBillingProjectAndWorkspace
      _ <- loggerIO.info(s"Single test project ${googleProjectAndWorkspaceName.workspaceName.namespace} claimed")
      t <- testCode(googleProjectAndWorkspaceName.googleProject)
      _ <- loggerIO.info(s"Releasing single-test project: ${googleProjectAndWorkspaceName.workspaceName.namespace}")
      _ <- deleteWorkspaceAndBillingProject(googleProjectAndWorkspaceName.workspaceName)
    } yield t

    test.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
}

trait NewBillingProjectAndWorkspaceBeforeAndAfterAll extends BillingProjectUtils with BeforeAndAfterAll {
  this: TestSuite =>

  implicit val ronTestersonAuthorization: IO[Authorization] = Ron.authorization()

  override def beforeAll(): Unit = {
    val res = for {
      _ <- IO(super.beforeAll())
      _ <- loggerIO.info("Running NewBillingProjectAndWorkspaceBeforeAndAfterAll.beforeAll()")
      claimAttempt <- createBillingProjectAndWorkspace.attempt
      _ <- claimAttempt match {
        case Left(e) => IO(sys.props.put(googleProjectKey, createBillingProjectErrorPrefix + e.getMessage))
        case Right(googleProjectAndWorkspaceName) =>
          IO(
            sys.props.addAll(
              Map(
                googleProjectKey -> googleProjectAndWorkspaceName.googleProject.value,
                workspaceNamespaceKey -> googleProjectAndWorkspaceName.workspaceName.namespace,
                workspaceNameKey -> googleProjectAndWorkspaceName.workspaceName.name
              )
            )
          ) >> createInitialRuntime(
            googleProjectAndWorkspaceName.googleProject
          )
      }
      port <- ProxyRedirectClient.startServer()
      _ <- IO(sys.props.put(proxyRedirectServerPortKey, port.toString))
      _ <- loggerIO.info(s"Serving proxy redirect page at ${ProxyRedirectClient.baseUri.renderString}")
    } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  override def afterAll(): Unit = {
    val res = for {
      shouldUnclaimProp <- IO(sys.props.get(shouldUnclaimProjectsKey))
      _ <- loggerIO.info(
        s"Running NewBillingProjectAndWorkspaceBeforeAndAfterAll.afterAll() ${shouldUnclaimProjectsKey}: $shouldUnclaimProp"
      )
      projectProp <- IO(sys.props.get(googleProjectKey))
      workspaceNamespaceProp <- IO(sys.props.get(workspaceNamespaceKey))
      workspaceNameProp <- IO(sys.props.get(workspaceNameKey))
      project = projectProp.filterNot(_.startsWith(createBillingProjectErrorPrefix)).map(GoogleProject)
      _ <-
        if (!shouldUnclaimProp.contains("false")) {
          (project, workspaceNamespaceProp, workspaceNameProp).traverseN { case (p, n, w) =>
            deleteInitialRuntime(p) >> deleteWorkspaceAndBillingProject(WorkspaceName(n, w))
          }
        } else loggerIO.info(s"Not going to release project: ${workspaceNamespaceProp} due to error happened")
      _ <- IO(sys.props.subtractAll(List(googleProjectKey, workspaceNamespaceKey, workspaceNameKey)))
      _ <- ProxyRedirectClient.stopServer(sys.props.get(proxyRedirectServerPortKey).get.toInt)
      _ <- loggerIO.info(s"Stopped proxy redirect server")
      _ <- IO(super.afterAll())
    } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  // NOTE: createInitialRuntime / deleteInitialRuntime exists so we can ensure that project-level
  // resources like networks, subnets, etc are set up prior to the concurrent test execution.
  // We can remove this once https://broadworkbench.atlassian.net/browse/IA-2121 is done.

  private def createInitialRuntime(project: GoogleProject): IO[Unit] =
    if (isHeadless) {
      LeonardoApiClient.client.use { implicit c =>
        for {
          res <- LeonardoApiClient
            .createRuntimeWithWait(
              project,
              initalRuntimeName,
              LeonardoApiClient.defaultCreateRuntime2Request
            )
            .attempt
          _ <- res match {
            case Right(_) =>
              loggerIO.info(s"Created initial runtime ${project.value} / ${initalRuntimeName.asString}")
            case Left(err) =>
              loggerIO
                .warn(err)(
                  s"Failed to create initial runtime ${project.value} / ${initalRuntimeName.asString} with error"
                )
          }
        } yield ()
      }
    } else IO.unit

  private def deleteInitialRuntime(project: GoogleProject): IO[Unit] =
    if (isHeadless) {
      LeonardoApiClient.client.use { implicit c =>
        for {
          res <- LeonardoApiClient
            .deleteRuntime(
              project,
              initalRuntimeName
            )
            .attempt
          _ <- res match {
            case Right(_) =>
              loggerIO.info(s"Deleted initial runtime ${project.value} / ${initalRuntimeName.asString}")
            case Left(err) =>
              loggerIO.warn(err)(
                s"Failed to delete initial runtime ${project.value} / ${initalRuntimeName.asString} with error"
              )
          }
        } yield ()
      }
    } else IO.unit
}

final case class AzureBillingProjectName(value: String) extends AnyVal
trait AzureBilling extends FixtureAnyFreeSpecLike {
  this: TestSuite =>
  import io.circe.{parser, Decoder}
  import org.broadinstitute.dsde.workbench.auth.AuthToken
  import org.broadinstitute.dsde.workbench.azure.{AzureCloudContext, ManagedResourceGroupName, SubscriptionId, TenantId}
  override type FixtureParam = WorkspaceResponse
  val azureProjectKey = "leonardo.azureProject"

  // These are static coordinates for this managed app in the azure portal: https://portal.azure.com/#@azure.dev.envs-terra.bio/resource/subscriptions/f557c728-871d-408c-a28b-eb6b2141a087/resourceGroups/staticTestingMrg/overview
  // Note that the final 'optional' field for a pre-created landing zone is not technically optional
  // If you fail to include a landing zone, the wb-libs call to create the billing project will fail, timing out due to landing zone creation not being an expected part of creation
  // Contact the workspaces team if this fails to work
  // implicit val azureManagedAppCoordinates: AzureManagedAppCoordinates = AzureManagedAppCoordinates(
  //   UUID.fromString("fad90753-2022-4456-9b0a-c7e5b934e408"),
  //   UUID.fromString("f557c728-871d-408c-a28b-eb6b2141a087"),
  //   "staticTestingMrg",
  //   Some(UUID.fromString("f41c1a97-179b-4a18-9615-5214d79ba600"))
  // )
  override def withFixture(test: OneArgTest): Outcome = {
    def runTestAndCheckOutcome(workspace: WorkspaceResponse) =
      super.withFixture(test.toNoArgTest(workspace))

    implicit val accessToken = Hermione.authToken().unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    try
      sys.props.get(azureProjectKey) match {
        case None => throw new RuntimeException("leonardo.azureProject system property is not set")
        case Some(projectName) =>
          println("withFixture projectName (should match pipeline projectName): " + projectName)
          withRawlsWorkspace(AzureBillingProjectName(projectName)) { workspace =>
            runTestAndCheckOutcome(workspace)
          }
      }
    catch {
      case e: org.broadinstitute.dsde.workbench.service.RestException
          if e.message == "Project cannot be deleted because it contains workspaces." =>
        println(
          s"Exception occurred in test, but it is classed as a non-fatal cleanup error (likely in `withTemporaryAzureBillingProject`): $e"
        )
        Succeeded
      case e: Throwable => throw e
    }
  }

  def withRawlsWorkspace[T](
    projectName: AzureBillingProjectName
  )(testCode: WorkspaceResponse => T)(implicit authToken: AuthToken): T = {
    // hardcode this if you want to use a static workspace
//        val workspaceName = "ddf3f5fa-a80e-4b2f-ab6e-9bd07817fad1-azure-test-workspace"

    val workspaceName = generateWorkspaceName()

    println(s"withRawlsWorkspace: Calling create rawls workspace with name ${workspaceName}")

    Rawls.workspaces.create(
      projectName.value,
      workspaceName,
      Set.empty,
      Map("disableAutomaticAppCreation" -> "true")
    )

    val response =
      workspaceResponse(projectName.value, workspaceName).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    println(s"withRawlsWorkspace: Rawls workspace get called, response: ${response}")

    try
      testCode(response)
    catch {
      case e: Throwable =>
        println(s"Exception occurred during test: ${e}")
        throw e
    } finally
      try
        Rawls.workspaces.delete(projectName.value, workspaceName)
      catch {
        case e: Exception =>
          println(
            s"withRawlsWorkspace: ignoring rawls workspace deletion error, not relevant to Leo tests. \n\tError: ${e}"
          )
      }
  }

  private def workspaceResponse(projectName: String, workspaceName: String)(implicit
    authToken: AuthToken
  ): IO[WorkspaceResponse] = for {
    responseString <- IO.pure(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName))
    json <- IO.fromEither(parser.parse(responseString))
    parsedResponse <- IO.fromEither(json.as[WorkspaceResponse])
  } yield parsedResponse

  case class WorkspaceResponse(accessLevel: Option[String],
                               canShare: Option[Boolean],
                               canCompute: Option[Boolean],
                               catalog: Option[Boolean],
                               workspace: WorkspaceDetails,
                               owners: Option[Set[String]],
                               azureContext: Option[AzureCloudContext]
  )

  implicit val azureContextDecoder: Decoder[AzureCloudContext] = Decoder.instance { c =>
    for {
      tenantId <- c.downField("tenantId").as[String]
      subscriptionId <- c.downField("subscriptionId").as[String]
      resourceGroupId <- c.downField("managedResourceGroupId").as[String]
    } yield AzureCloudContext(TenantId(tenantId),
                              SubscriptionId(subscriptionId),
                              ManagedResourceGroupName(resourceGroupId)
    )
  }

  implicit val workspaceDetailsDecoder: Decoder[WorkspaceDetails] = Decoder.forProduct10(
    "namespace",
    "name",
    "workspaceId",
    "bucketName",
    "createdDate",
    "lastModified",
    "createdBy",
    "isLocked",
    "billingAccountErrorMessage",
    "errorMessage"
  )(WorkspaceDetails.apply)

  implicit val workspaceResponseDecoder: Decoder[WorkspaceResponse] = Decoder.forProduct7(
    "accessLevel",
    "canShare",
    "canCompute",
    "catalog",
    "workspace",
    "owners",
    "azureContext"
  )(WorkspaceResponse.apply)

  // Note this isn't the full model available, we only need a few fields
  // The full model lives in rawls here https://github.com/broadinstitute/rawls/blob/develop/model/src/main/scala/org/broadinstitute/dsde/rawls/model/WorkspaceModel.scala#L712
  case class WorkspaceDetails(
    namespace: String,
    name: String,
    workspaceId: String,
    bucketName: String,
    createdDate: String,
    lastModified: String,
    createdBy: String,
    isLocked: Boolean = false,
    billingAccountErrorMessage: Option[String] = None,
    errorMessage: Option[String] = None
  )

  private def generateWorkspaceName(): String =
    s"${UUID.randomUUID().toString()}-azure-test-workspace"
}

final class LeonardoSuite
    extends Suites(
      new RuntimeCreationDiskSpec,
      new LabSpec,
      new RuntimeAutopauseSpec,
      new RuntimePatchSpec,
      new RuntimeStatusTransitionsSpec,
      new NotebookGCECustomizationSpec,
      new NotebookGCEDataSyncingSpec,
      new RuntimeDataprocSpec,
      new RuntimeGceSpec
    )
    with TestSuite
    with NewBillingProjectAndWorkspaceBeforeAndAfterAll
    with ParallelTestExecution

final class LeonardoTerraDockerSuite
    extends Suites(
      new NotebookHailSpec,
      new NotebookPyKernelSpec,
      new NotebookRKernelSpec,
      new RStudioSpec
    )
    with TestSuite
    with NewBillingProjectAndWorkspaceBeforeAndAfterAll
    with ParallelTestExecution

final class LeonardoAzureSuite
    extends Suites(
      new AzureRuntimeSpec,
      new AzureDiskSpec,
      new AzureAutopauseSpec
    )
    with TestSuite
    with AzureBilling
    with ParallelTestExecution
    with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    // implicit val accessToken = Hermione.authToken().unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val res = for {
      _ <- IO(println("in beforeAll for AzureBillingBeforeAndAfter"))
      _ <- IO(super.beforeAll())
      // _ <- withTemporaryAzureBillingProject(azureManagedAppCoordinates, shouldCleanup = false) { projectName =>
      //  IO(sys.props.put(azureProjectKey, projectName))
      // }
      _ <- IO(sys.props.put(azureProjectKey, BILLING_PROJECT))
      // hardcode this if you want to use a static billing project
      //  _ <- IO(sys.props.put(azureProjectKey, "tmp-billing-project-beddf71a74"))
    } yield ()
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

}
