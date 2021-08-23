package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.IO
import cats.syntax.all._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.model.WorkspaceName
import org.broadinstitute.dsde.workbench.fixture.{BillingFixtures}
import org.broadinstitute.dsde.workbench.leonardo.GPAllocFixtureSpec.{shouldUnclaimProjectsKey, _}
import org.broadinstitute.dsde.workbench.leonardo.apps.{AppCreationSpec, CustomAppCreationSpec}
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
import spray.json._
import spray.json.DefaultJsonProtocol.StringJsonFormat

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

    sys.props.get(workspaceNamespaceKey) match {
      case Some(msg) if msg.startsWith(gpallocErrorPrefix) => throw new RuntimeException(msg)
      case x                                               => logger.info(s"Workspace namespace is: ${x}")
    }

    sys.props.get(googleProjectKey) match {
      case None => throw new RuntimeException("leonardo.googleProject system property is not set")
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
      workspaceName = UUID.randomUUID().toString
      _ <- IO(
        Orchestration.workspaces.create(claimedBillingProject.projectName, workspaceName)(ronAuthToken)
      )
      workspaceDetails <- IO(
        Rawls.workspaces.getWorkspaceDetails(claimedBillingProject.projectName, workspaceName)(ronAuthToken)
      )
      googleProjectId <- IO(
        workspaceDetails.parseJson.asJsObject
          .getFields("workspace")
          .flatMap(workspace => workspace.asJsObject.getFields("googleProjectId"))
          .head
          .convertTo[String]
      )
      _ <- loggerIO.info(s"Workspace details: ${workspaceDetails}")
    } yield GoogleProjectAndWorkspaceName(GoogleProject(googleProjectId),
                                          WorkspaceName(claimedBillingProject.projectName, workspaceName))

  /**
   * Unclaiming billing project claim by Hermione
   */
  protected def unclaimProject(workspaceName: WorkspaceName): IO[Unit] =
    for {
      _ <- IO(
        Orchestration.workspaces.delete(workspaceName.namespace, workspaceName.name)(ronAuthToken)
      )
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

    test.unsafeRunSync()
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
        case Left(e) => IO(sys.props.put(workspaceNamespaceKey, gpallocErrorPrefix + e.getMessage))
        case Right(googleProjectAndWorkspaceName) =>
          IO(sys.props.put(googleProjectKey, googleProjectAndWorkspaceName.googleProject.value)) >> IO(
            sys.props.put(workspaceNamespaceKey, googleProjectAndWorkspaceName.workspaceName.namespace)
          ) >> IO(sys.props.put(workspaceNameKey, googleProjectAndWorkspaceName.workspaceName.name)) >> createInitialRuntime(
            googleProjectAndWorkspaceName.googleProject
          )
      }
      proxyRedirectServer <- ProxyRedirectClient.baseUri
      _ <- loggerIO.info(s"Serving proxy redirect page at ${proxyRedirectServer.renderString}")
    } yield ()

    res.unsafeRunSync()
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
        project.traverse(p => deleteInitialRuntime(p)).flatMap { _ =>
          workspaceNamespaceProp.traverse(workspaceNamespace =>
            workspaceNameProp
              .traverse(workspaceName => unclaimProject(WorkspaceName(workspaceNamespace, workspaceName)))
          )
        }
      } else loggerIO.info(s"Not going to release project: ${workspaceNamespaceProp} due to error happened")
      _ <- IO(sys.props.remove(googleProjectKey))
      _ <- IO(sys.props.remove(workspaceNamespaceKey))
      _ <- IO(sys.props.remove(workspaceNameKey))
      _ <- ProxyRedirectClient.stopServer()
      _ <- loggerIO.info(s"Stopped proxy redirect server")
      _ <- IO(super.afterAll())
    } yield ()

    res.unsafeRunSync()
  }

  // NOTE: createInitialRuntime / deleteInitialRuntime exists so we can ensure that project-level
  // resources like networks, subnets, etc are set up prior to the concurrent test execution.
  // We can remove this once https://broadworkbench.atlassian.net/browse/IA-2121 is done.

  private def createInitialRuntime(project: GoogleProject): IO[Unit] =
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

  private def deleteInitialRuntime(project: GoogleProject): IO[Unit] =
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
            IO(
              loggerIO.warn(err)(
                s"Failed to delete initial runtime ${project.value} / ${initalRuntimeName.asString} with error"
              )
            )
        }
      } yield ()
    }
}

final class LeonardoSuite
    extends Suites(
      new AppCreationSpec,
      new CustomAppCreationSpec,
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
