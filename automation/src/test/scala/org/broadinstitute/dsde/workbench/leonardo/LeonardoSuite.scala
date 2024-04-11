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
import org.broadinstitute.dsde.workbench.leonardo.TestUser.{Hermione, Ron}
import org.broadinstitute.dsde.workbench.leonardo.apps.AppLifecycleSpec
import org.broadinstitute.dsde.workbench.leonardo.notebooks._
import org.broadinstitute.dsde.workbench.leonardo.rstudio.RStudioSpec
import org.broadinstitute.dsde.workbench.leonardo.runtimes._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.service.BillingProject.BillingProjectRole
import org.broadinstitute.dsde.workbench.service.{Orchestration, Rawls}
import org.http4s.headers.Authorization
import org.scalatest._
import org.scalatest.freespec.FixtureAnyFreeSpecLike

trait BillingProjectFixtureSpec
    extends FixtureAnyFreeSpecLike
    with Retries
    with LazyLogging
    with BillingProjectUtils
    with BeforeAndAfterAll {
  override type FixtureParam = GoogleProject

  var fixtureProject: GoogleProject = _
  var workspaceName: WorkspaceName = _
  var shouldUnclaimProjects: Boolean = true

  override def withFixture(test: OneArgTest): Outcome = {
    def runTestAndCheckOutcome(project: GoogleProject) = {
      val outcome = super.withFixture(test.toNoArgTest(project))
      if (!outcome.isSucceeded) {
        shouldUnclaimProjects = false
      }
      outcome
    }

    if (isRetryable(test))
      withRetry(runTestAndCheckOutcome(fixtureProject))
    else
      runTestAndCheckOutcome(fixtureProject)
  }

  override def beforeAll(): Unit = {
    implicit val ronTestersonAuthorization: IO[Authorization] = Ron.authorization()
    val res = for {
      _ <- IO(super.beforeAll())
      _ <- loggerIO.info("Running BillingFixtureSpec.beforeAll()")
      claimAttempt <- createBillingProjectAndWorkspace.attempt
      _ <- claimAttempt match {
        case Left(e) =>
          IO.raiseError(
            new RuntimeException(s"Unable to claim billing project for spec ${getClass.getSimpleName}, error: ${e}")
          )
        case Right(googleProjectAndWorkspaceName) =>
          fixtureProject = googleProjectAndWorkspaceName.googleProject
          workspaceName = googleProjectAndWorkspaceName.workspaceName
          createInitialRuntime(
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
    implicit val ronTestersonAuthorization: IO[Authorization] = Ron.authorization()
    val res = for {
      _ <- loggerIO.info(
        s"Running NewBillingProjectAndWorkspaceBeforeAndAfterAll.afterAll() shouldUnclaimProjects: $shouldUnclaimProjects"
      )
      _ <-
        if (shouldUnclaimProjects && fixtureProject != null) {
          deleteInitialRuntime(fixtureProject) >> deleteWorkspaceAndBillingProject(workspaceName)
        } else loggerIO.info(s"Not going to release project: ${workspaceName} due to error happened")
      _ <- ProxyRedirectClient.stopServer(sys.props.get(proxyRedirectServerPortKey).get.toInt)
      _ <- loggerIO.info(s"Stopped proxy redirect server")
      _ <- IO(super.afterAll())
    } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

}
object BillingProjectFixtureSpec {
  val initalRuntimeName = RuntimeName("initial-runtime")
  val proxyRedirectServerPortKey = "proxyRedirectServerPort"
  val googleProjectKey = "leonardo.googleProject"
  val workspaceNamespaceKey = "leonardo.workspaceNamespace"
  val workspaceNameKey = "leonardo.workspaceName"
  val shouldUnclaimProjectsKey = "leonardo.shouldUnclaimProjects"
  val createBillingProjectErrorPrefix = "Failed to create new billing project and workspace: "
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

  // Do not use this for tests that interact with runtimes, only for apps. Use `BillingProjectFixtureSpec` for most use cases
  def withNewProject[T](testCode: GoogleProject => IO[T]): T = {
    val test = for {
      _ <- loggerIO.info("Allocating a new project")
      googleProjectAndWorkspaceName <- createBillingProjectAndWorkspace
      _ <- loggerIO.info(s"Allocated a project ${googleProjectAndWorkspaceName.workspaceName.namespace}")
      t <- testCode(googleProjectAndWorkspaceName.googleProject)
      _ <- loggerIO.info(s"Releasing single-test project: ${googleProjectAndWorkspaceName.workspaceName.namespace}")
      _ <- deleteWorkspaceAndBillingProject(googleProjectAndWorkspaceName.workspaceName)
    } yield t

    test.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  // NOTE: createInitialRuntime / deleteInitialRuntime exists so we can ensure that project-level
  // resources like networks, subnets, etc are set up prior to the concurrent test execution.
  // We can remove this once https://broadworkbench.atlassian.net/browse/IA-2121 is done.

  protected def createInitialRuntime(project: GoogleProject)(implicit auth: IO[Authorization]): IO[Unit] =
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

  protected def deleteInitialRuntime(project: GoogleProject)(implicit auth: IO[Authorization]): IO[Unit] =
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

}

//Each spec in this suite gets a new google project and creates its own runtimes/apps
final class LeonardoSuite
    extends Suites(
      new RuntimeCreationDiskSpec,
      new RuntimeAutopauseSpec,
      new RuntimePatchSpec,
      new RuntimeStatusTransitionsSpec,
      new NotebookGCECustomizationSpec,
      new RuntimeDataprocSpec,
      new RuntimeGceSpec,
      new AppLifecycleSpec
    )
    with TestSuite
    with ParallelTestExecution

// This suite uses a single google project for all runtimes, and each implements `RuntimeFixtureSpec` to share a runtime between tests
final class LeonardoRuntimeFixtureSuite
    extends Suites(
      new RuntimeSystemSpec,
      new NotebookGCEDataSyncingSpec,
      new NotebookHailSpec,
      new NotebookPyKernelSpec,
      new NotebookRKernelSpec,
      new RStudioSpec
    )
    with TestSuite
    with NewBillingProjectAndWorkspaceBeforeAndAfterAll
    with ParallelTestExecution
