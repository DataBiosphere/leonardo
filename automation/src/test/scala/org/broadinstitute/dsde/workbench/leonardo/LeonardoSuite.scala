package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.IO
import cats.syntax.all._
import com.typesafe.scalalogging.LazyLogging
import io.circe.parser._
import org.broadinstitute.dsde.rawls.model.WorkspaceName
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo.GPAllocFixtureSpec.{shouldUnclaimProjectsKey, _}
import org.broadinstitute.dsde.workbench.leonardo.apps.{AppCreationSpec, AppLifecycleSpec}
import org.broadinstitute.dsde.workbench.leonardo.lab.LabSpec
import org.broadinstitute.dsde.workbench.leonardo.notebooks._
import org.broadinstitute.dsde.workbench.leonardo.rstudio.RStudioSpec
import org.broadinstitute.dsde.workbench.leonardo.runtimes._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.service.{BillingProject, Orchestration, Rawls}
import org.http4s.AuthScheme
import org.http4s.Credentials.Token
import org.http4s.headers.Authorization
import org.scalatest._
import org.scalatest.freespec.FixtureAnyFreeSpecLike

import java.util.UUID

trait GPAllocFixtureSpec extends FixtureAnyFreeSpecLike with Retries with LazyLogging {
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
      case None                                            => throw new RuntimeException("leonardo.googleProject system property is not set")
      case Some(msg) if msg.startsWith(gpallocErrorPrefix) => throw new RuntimeException(msg)
      case Some(googleProjectId) =>
        if (isRetryable(test))
          withRetry(runTestAndCheckOutcome(GoogleProject(googleProjectId)))
        else
          runTestAndCheckOutcome(GoogleProject(googleProjectId))
    }
  }
}
object GPAllocFixtureSpec {
  val googleProjectKey = "leonardo.googleProject"
  val workspaceNamespaceKey = "leonardo.workspaceNamespace"
  val workspaceNameKey = "leonardo.workspaceName"
  val shouldUnclaimProjectsKey = "leonardo.shouldUnclaimProjects"
  val gpallocErrorPrefix = "Failed To Claim Project: "
  val initalRuntimeName = RuntimeName("initial-runtime")
  val proxyRedirectServerPortKey = "proxyRedirectServerPort"
}

case class GoogleProjectAndWorkspaceName(
  googleProject: GoogleProject,
  workspaceName: WorkspaceName
)

trait GPAllocUtils extends BillingFixtures with LeonardoTestUtils {
  this: TestSuite =>

  /**
   * Claim new billing project by Hermione
   */
  protected def claimGPAllocProjectAndCreateWorkspace(): IO[GoogleProjectAndWorkspaceName] =
    for {
      claimedBillingProject <- IO(claimGPAllocProject(hermioneCreds))
      _ <- IO(
        Orchestration.billing.addUserToBillingProject(claimedBillingProject.projectName,
                                                      ronEmail,
                                                      BillingProject.BillingProjectRole.User)(hermioneAuthToken)
      )
      _ <- loggerIO.info(s"Billing project claimed: ${claimedBillingProject.projectName}")
      workspaceName <- IO(UUID.randomUUID().toString)
      _ <- IO(
        Orchestration.workspaces.create(claimedBillingProject.projectName, workspaceName)(ronAuthToken)
      )
      workspaceDetails <- IO(
        Rawls.workspaces.getWorkspaceDetails(claimedBillingProject.projectName, workspaceName)(ronAuthToken)
      )
      json <- IO.fromEither(parse(workspaceDetails))
      googleProjectOpt = json.hcursor.downField("workspace").get[String]("googleProject").toOption
      googleProjectId <- IO.fromOption(googleProjectOpt)(
        new Exception(s"Could not get googleProject from workspace $workspaceName")
      )
    } yield GoogleProjectAndWorkspaceName(GoogleProject(googleProjectId),
                                          WorkspaceName(claimedBillingProject.projectName, workspaceName))

  /**
   * Unclaiming billing project claim by Hermione
   */
  protected def unclaimProject(workspaceName: WorkspaceName): IO[Unit] =
    for {
      _ <- IO(
        Orchestration.workspaces.delete(workspaceName.namespace, workspaceName.name)(ronAuthToken)
      ).attempt
      _ <- IO(
        Orchestration.billing
          .removeUserFromBillingProject(workspaceName.namespace, ronEmail, BillingProject.BillingProjectRole.User)(
            hermioneAuthToken
          )
      )
      releaseProject <- IO(releaseGPAllocProject(workspaceName.namespace, hermioneCreds)).attempt
      _ <- releaseProject match {
        case Left(e) => loggerIO.warn(e)(s"Failed to release billing project: ${workspaceName.namespace}")
        case _       => loggerIO.info(s"Billing project released: ${workspaceName.namespace}")
      }
    } yield ()

  def withNewProject[T](testCode: GoogleProject => IO[T]): T = {
    val test = for {
      _ <- loggerIO.info("Allocating a new single-test project")
      googleProjectAndWorkspaceName <- claimGPAllocProjectAndCreateWorkspace()
      _ <- loggerIO.info(s"Single test project ${googleProjectAndWorkspaceName.workspaceName.namespace} claimed")
      t <- testCode(googleProjectAndWorkspaceName.googleProject)
      _ <- loggerIO.info(s"Releasing single-test project: ${googleProjectAndWorkspaceName.workspaceName.namespace}")
      _ <- unclaimProject(googleProjectAndWorkspaceName.workspaceName)
    } yield t

    test.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
}

trait GPAllocBeforeAndAfterAll extends GPAllocUtils with BeforeAndAfterAll {
  this: TestSuite =>

  override def beforeAll(): Unit = {
    val res = for {
      _ <- IO(super.beforeAll())
      _ <- loggerIO.info(s"Running GPAllocBeforeAndAfterAll beforeAll")
      claimAttempt <- claimGPAllocProjectAndCreateWorkspace().attempt
      _ <- claimAttempt match {
        case Left(e) => IO(sys.props.put(googleProjectKey, gpallocErrorPrefix + e.getMessage))
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
      _ <- loggerIO.info(s"Running GPAllocBeforeAndAfterAll afterAll ${shouldUnclaimProjectsKey}: $shouldUnclaimProp")
      projectProp <- IO(sys.props.get(googleProjectKey))
      workspaceNamespaceProp <- IO(sys.props.get(workspaceNamespaceKey))
      workspaceNameProp <- IO(sys.props.get(workspaceNameKey))
      project = projectProp.filterNot(_.startsWith(gpallocErrorPrefix)).map(GoogleProject)
      _ <- if (!shouldUnclaimProp.contains("false")) {
        (project, workspaceNamespaceProp, workspaceNameProp).traverseN {
          case (p, n, w) => deleteInitialRuntime(p) >> unclaimProject(WorkspaceName(n, w))
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
        implicit val authHeader = Authorization(Token(AuthScheme.Bearer, ronCreds.makeAuthToken().value))
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
        implicit val authHeader = Authorization(Token(AuthScheme.Bearer, ronCreds.makeAuthToken().value))
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

final class LeonardoSuite
    extends Suites(
      new AppCreationSpec,
      new AppLifecycleSpec,
      new RuntimeCreationDiskSpec,
      new LabSpec,
      new LeoPubsubSpec,
      new RuntimeAutopauseSpec,
      new RuntimePatchSpec,
      new RuntimeStatusTransitionsSpec,
      new NotebookGCECustomizationSpec,
      new NotebookGCEDataSyncingSpec,
      new RuntimeDataprocSpec,
      new RuntimeGceSpec
    )
    with TestSuite
    with GPAllocBeforeAndAfterAll
    with ParallelTestExecution

final class LeonardoTerraDockerSuite
    extends Suites(
      new NotebookAouSpec,
      new NotebookBioconductorKernelSpec,
      new NotebookGATKSpec,
      new NotebookHailSpec,
      new NotebookPyKernelSpec,
      new NotebookRKernelSpec,
      new RStudioSpec
    )
    with TestSuite
    with GPAllocBeforeAndAfterAll
    with ParallelTestExecution
