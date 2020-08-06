package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.IO
import cats.implicits._
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo.GPAllocFixtureSpec.{shouldUnclaimProjectsKey, _}
import org.broadinstitute.dsde.workbench.leonardo.cluster.{ClusterAutopauseSpec, ClusterStatusTransitionsSpec}
import org.broadinstitute.dsde.workbench.leonardo.lab.LabSpec
import org.broadinstitute.dsde.workbench.leonardo.notebooks._
import org.broadinstitute.dsde.workbench.leonardo.rstudio.RStudioSpec
import org.broadinstitute.dsde.workbench.leonardo.runtimes.{
  RuntimeAutopauseSpec,
  RuntimeCreationDiskSpec,
  RuntimePatchSpec,
  RuntimeStatusTransitionsSpec
}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.service.{BillingProject, Orchestration}
import org.http4s.AuthScheme
import org.http4s.Credentials.Token
import org.http4s.headers.Authorization
import org.scalatest._
import org.scalatest.freespec.FixtureAnyFreeSpecLike

trait GPAllocFixtureSpec extends FixtureAnyFreeSpecLike with Retries {
  override type FixtureParam = GoogleProject
  override def withFixture(test: OneArgTest): Outcome = {
    def runTestAndCheckOutcome(project: GoogleProject) = {
      val outcome = super.withFixture(test.toNoArgTest(project))
      if (!outcome.isSucceeded) {
        System.setProperty(shouldUnclaimProjectsKey, "false")
      }
      outcome
    }

    sys.props.get(gpallocProjectKey) match {
      case None                                            => throw new RuntimeException("leonardo.billingProject system property is not set")
      case Some(msg) if msg.startsWith(gpallocErrorPrefix) => throw new RuntimeException(msg)
      case Some(billingProject) =>
        if (isRetryable(test))
          withRetry(runTestAndCheckOutcome(GoogleProject(billingProject)))
        else
          runTestAndCheckOutcome(GoogleProject(billingProject))
    }
  }
}
object GPAllocFixtureSpec {
  val gpallocProjectKey = "leonardo.billingProject"
  val shouldUnclaimProjectsKey = "leonardo.shouldUnclaimProjects"
  val gpallocErrorPrefix = "Failed To Claim Project: "
  val initalRuntimeName = RuntimeName("initial-runtime")
}

trait GPAllocBeforeAndAfterAll extends BeforeAndAfterAll with BillingFixtures with LeonardoTestUtils {
  this: TestSuite =>

  override def beforeAll(): Unit = {
    val res = for {
      _ <- IO(super.beforeAll())
      claimAttempt <- claimProject().attempt
      _ <- claimAttempt match {
        case Left(e) => IO(sys.props.put(gpallocProjectKey, gpallocErrorPrefix + e.getMessage))
        case Right(billingProject) =>
          IO(sys.props.put(gpallocProjectKey, billingProject.value)) >> createInitialRuntime(billingProject)
      }
    } yield ()

    res.unsafeRunSync()
  }

  override def afterAll(): Unit = {
    val res = for {
      shouldUnclaim <- IO(sys.props.get(shouldUnclaimProjectsKey))
      prop <- IO(sys.props.get(gpallocProjectKey))
      _ <- IO(logger.info(s"Running GPAllocBeforeAndAfterAll afterAll ${shouldUnclaimProjectsKey}: $shouldUnclaim"))
      _ <- if (shouldUnclaim != Some("false")) {
        val project = prop.filterNot(_.startsWith(gpallocErrorPrefix)).map(GoogleProject)
        for {
          _ <- project.fold(IO.unit)(p =>
            deleteInitialRuntime(p) >> unclaimProject(p) >> IO(sys.props.remove(gpallocProjectKey))
          )
        } yield ()
      } else IO(logger.info(s"Not going to release project: ${prop} due to error happened"))
      _ <- IO(super.afterAll())
    } yield ()

    res.unsafeRunSync()
  }

  /**
   * Claim new billing project by Hermione
   */
  private def claimProject(): IO[GoogleProject] =
    for {
      claimedBillingProject <- IO(claimGPAllocProject(hermioneCreds))
      _ <- IO(
        Orchestration.billing.addUserToBillingProject(claimedBillingProject.projectName,
                                                      ronEmail,
                                                      BillingProject.BillingProjectRole.User)(hermioneAuthToken)
      )
      _ <- IO(logger.info(s"Billing project claimed: ${claimedBillingProject.projectName}"))
    } yield GoogleProject(claimedBillingProject.projectName)

  /**
   * Unclaiming billing project claim by Hermione
   */
  private def unclaimProject(project: GoogleProject): IO[Unit] =
    for {
      _ <- IO(
        Orchestration.billing
          .removeUserFromBillingProject(project.value, ronEmail, BillingProject.BillingProjectRole.User)(
            hermioneAuthToken
          )
      )
      _ <- IO(releaseGPAllocProject(project.value, hermioneCreds))
      _ <- IO(logger.info(s"Billing project released: ${project.value}"))
    } yield ()

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
            IO(logger.info(s"Created initial runtime ${project.value} / ${initalRuntimeName.asString}"))
          case Left(err) =>
            IO(logger.warn(
                 s"Failed to create initial runtime ${project.value} / ${initalRuntimeName.asString} with error"
               ),
               err)
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
            IO(logger.info(s"Deleted initial runtime ${project.value} / ${initalRuntimeName.asString}"))
          case Left(err) =>
            IO(logger.warn(
                 s"Failed to delete initial runtime ${project.value} / ${initalRuntimeName.asString} with error"
               ),
               err)
        }
      } yield ()
    }
}

final class LeonardoSuite
    extends Suites(
      new RuntimeCreationDiskSpec,
      new ClusterStatusTransitionsSpec,
      new LabSpec,
      new NotebookClusterMonitoringSpec,
      new NotebookCustomizationSpec,
      new NotebookDataSyncingSpec,
      new LeoPubsubSpec,
      new ClusterAutopauseSpec,
      new RuntimeAutopauseSpec,
      new RuntimePatchSpec,
      new RuntimeStatusTransitionsSpec,
      new NotebookGCEClusterMonitoringSpec,
      new NotebookGCECustomizationSpec,
      new NotebookGCEDataSyncingSpec
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
