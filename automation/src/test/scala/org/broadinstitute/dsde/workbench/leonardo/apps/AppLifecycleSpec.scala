package org.broadinstitute.dsde.workbench.leonardo
package apps

import cats.effect.IO
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, streamUntilDoneOrTimeout, Generators}
import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient._
import org.broadinstitute.dsde.workbench.leonardo.TestUser.{getAuthTokenAndAuthorization, Ron}
import org.broadinstitute.dsde.workbench.leonardo.http.{CreateAppRequest, ListAppResponse, PersistentDiskRequest}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.tagobjects.Retryable
import org.scalatest.{Assertion, DoNotDiscover, Outcome, ParallelTestExecution, Retries}
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.BillingProjectFixtureSpec.shouldUnclaimProjectsKey
import org.scalatest.freespec.FixtureAnyFreeSpecLike

import scala.concurrent.duration._

@DoNotDiscover
class AppLifecycleSpec
    extends FixtureAnyFreeSpecLike
    with Retries
    with LeonardoTestUtils
    with BillingProjectUtils
    with TableDrivenPropertyChecks
    with ParallelTestExecution {
  implicit val (ronAuthToken: IO[AuthToken], ronAuthorization: IO[Authorization]) = getAuthTokenAndAuthorization(Ron)

  override type FixtureParam = GoogleProject

  override def withFixture(test: OneArgTest): Outcome = {
    def runTestAndCheckOutcome(project: GoogleProject) = {
      val outcome = super.withFixture(test.toNoArgTest(project))
      if (!outcome.isSucceeded) {
        System.setProperty(shouldUnclaimProjectsKey, "false")
      }
      outcome
    }
    val billingProjectAndWorkspace =
      createBillingProjectAndWorkspace.attempt.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    billingProjectAndWorkspace match {
      case Left(e) => throw new RuntimeException(s"google project could not be claimed for test, ${e}")
      case Right(projectAndWorkspace) =>
        if (isRetryable(test))
          withRetry(runTestAndCheckOutcome(projectAndWorkspace.googleProject))
        else
          runTestAndCheckOutcome(projectAndWorkspace.googleProject)
    }
  }

  def createAppRequest(appType: AppType,
                       workspaceName: String,
                       descriptorPath: Option[org.http4s.Uri],
                       allowedChartName: Option[AllowedChartName] = None
  ): CreateAppRequest = defaultCreateAppRequest.copy(
    diskConfig = Some(
      PersistentDiskRequest(
        Generators.genDiskName.sample.get,
        Some(DiskSize(300)),
        None,
        Map.empty
      )
    ),
    allowedChartName = allowedChartName,
    appType = appType,
    customEnvironmentVariables = Map("WORKSPACE_NAME" -> workspaceName),
    descriptorPath = descriptorPath,
    sourceWorkspaceId = None
  )
  // Test galaxy app first so that there will be a GKE cluster created already for the next two tests
  //  "create GALAXY app, start/stop, delete it and re-create it with same disk" in { googleProject =>
  //    test(googleProject, createAppRequest(AppType.Galaxy, "Galaxy-Workshop-ASHG_2020_GWAS_Demo", None), true, true)
  //  }

  "create CROMWELL app, delete it and re-create it with same disk" taggedAs (Tags.SmokeTest, Retryable) in {
    googleProject =>
      test(googleProject, createAppRequest(AppType.Cromwell, "cromwell-test-workspace", None), false, true)
  }

  "create RSTUDIO app, delete it and re-create it with same disk" taggedAs (Tags.SmokeTest, Retryable) in {
    googleProject =>
      test(googleProject,
           createAppRequest(AppType.Allowed, "rstudio-test-workspace", None, Some(AllowedChartName.RStudio)),
           false,
           true
      )
  }

  "create CUSTOM app then delete it" taggedAs Retryable ignore { googleProject =>
    test(
      googleProject,
      createAppRequest(
        AppType.Custom,
        "custom-test-workspace",
        Some(
          org.http4s.Uri.unsafeFromString(
            "https://raw.githubusercontent.com/DataBiosphere/terra-app/main/apps/ucsc_genome_browser/app.yaml"
          )
        )
      ),
      false,
      false
    )
  }

  def test(googleProject: GoogleProject,
           createAppRequest: CreateAppRequest,
           testStartStop: Boolean,
           testPersistentDisk: Boolean
  ): Assertion = {
    val appName = randomAppName
    val restoreAppName = AppName(s"restore-${appName.value}")

    val res = LeonardoApiClient.client.use { implicit client =>
      for {
        _ <- loggerIO.info(s"AppCreationSpec: About to create app ${googleProject.value}/${appName.value}")

        rat <- Ron.authToken()
        implicit0(auth: Authorization) = Authorization(Credentials.Token(AuthScheme.Bearer, rat.value))

        // Create the app
        _ <- LeonardoApiClient.createApp(googleProject, appName, createAppRequest)

        // Verify the initial getApp call
        getApp = LeonardoApiClient.getApp(googleProject, appName)
        getAppResponse <- getApp
        _ = getAppResponse.status should (be(AppStatus.Provisioning) or be(AppStatus.Precreating))

        // Verify the app eventually becomes Running
        _ <- IO.sleep(120 seconds)
        monitorCreateResult <- streamUntilDoneOrTimeout(
          getApp,
          120,
          15 seconds,
          s"AppCreationSpec: app ${googleProject.value}/${appName.value} did not finish creating after 30 minutes"
        )(implicitly, appInStateOrError(AppStatus.Running))
        _ <- loggerIO.info(
          s"AppCreationSpec: app ${googleProject.value}/${appName.value} monitor result: ${monitorCreateResult}"
        )
        _ = monitorCreateResult.status shouldBe AppStatus.Running

        _ <- IO.sleep(1 minute)

        _ <-
          if (!testStartStop) IO.unit
          else {
            for {
              // Stop the app
              _ <- LeonardoApiClient.stopApp(googleProject, appName)

              // Verify getApp again
              getAppResponse <- getApp
              _ = getAppResponse.status should (be(AppStatus.Stopping) or be(AppStatus.PreStopping) or be(
                AppStatus.Stopped
              ))

              // Verify the app eventually becomes Stopped. Resizing nodepool to 0 takes more than 10 minutes
              _ <- IO.sleep(10 minutes)
              monitorStopResult <- streamUntilDoneOrTimeout(
                getApp,
                120,
                10 seconds,
                s"AppCreationSpec: app ${googleProject.value}/${appName.value} did not finish stopping after 20 minutes"
              )(implicitly, appInStateOrError(AppStatus.Stopped))
              _ <- loggerIO.info(
                s"AppCreationSpec: app ${googleProject.value}/${appName.value} stop result: $monitorStopResult"
              )
              _ = monitorStopResult.status shouldBe AppStatus.Stopped

              // Start the app
              _ <- LeonardoApiClient.startApp(googleProject, appName)

              // Verify getApp again
              getAppResponse <- getApp
              _ = getAppResponse.status should (be(AppStatus.Starting) or be(AppStatus.PreStarting))

              // Verify the app eventually becomes Running
              _ <- IO.sleep(30 seconds)
              monitorStartResult <- streamUntilDoneOrTimeout(
                getApp,
                120,
                10 seconds,
                s"AppCreationSpec: app ${googleProject.value}/${appName.value} did not finish starting after 20 minutes"
              )(implicitly, appInStateOrError(AppStatus.Running))
              _ <- loggerIO.info(
                s"AppCreationSpec: app ${googleProject.value}/${appName.value} start result: $monitorStartResult"
              )
              _ = monitorStartResult.status shouldBe AppStatus.Running
            } yield ()
          }

        // Delete the app
        _ <- LeonardoApiClient.deleteApp(googleProject, appName, !testPersistentDisk)

        // Verify getApp again
        getAppResponse <- getApp
        _ = getAppResponse.status should (be(AppStatus.Deleting) or be(AppStatus.Predeleting))

        // Verify the app eventually becomes Deleted
        // Don't fail the test if the deletion times out because the app pre-delete job can sporadically fail.
        // See https://broadworkbench.atlassian.net/browse/IA-2471
        listApps = LeonardoApiClient.listApps(googleProject, true)
        implicit0(deletedDoneCheckable: DoneCheckable[List[ListAppResponse]]) = appDeleted(appName)
        monitorDeleteResult <- streamFUntilDone(
          listApps,
          120,
          10 seconds
        ).compile.lastOrError
        // TODO remove first case in below if statement when app deletion is reliable
        _ <-
          if (!deletedDoneCheckable.isDone(monitorDeleteResult)) {
            loggerIO.warn(
              s"AppCreationSpec: app ${googleProject.value}/${appName.value} did not finish deleting after 20 minutes. Result: $monitorDeleteResult"
            )
          } else {
            // Verify creating another app with the same disk doesn't error out
            for {
              _ <- loggerIO.info(
                s"AppCreationSpec: app ${googleProject.value}/${appName.value} delete result: $monitorDeleteResult"
              )
              _ <-
                if (testPersistentDisk)
                  for {
                    _ <- LeonardoApiClient.createAppWithWait(googleProject, restoreAppName, createAppRequest)(
                      client,
                      ronAuthorization,
                      loggerIO
                    )
                    _ <- LeonardoApiClient.deleteAppWithWait(googleProject, restoreAppName, true)
                    // Verify the app eventually becomes Deleted
                    // Don't fail the test if the deletion times out because the Galaxy pre-delete job can sporadically fail.
                    // See https://broadworkbench.atlassian.net/browse/IA-2471
                    listApps = LeonardoApiClient.listApps(googleProject, true)
                    monitorDeleteResult <- streamFUntilDone(listApps, 120, 10 seconds).compile.lastOrError
                    // TODO remove first case in below if statement when Galaxy deletion is reliable
                    _ <-
                      if (!deletedDoneCheckable.isDone(monitorDeleteResult)) {
                        loggerIO.warn(
                          s"AppCreationSpec: app ${googleProject.value}/${appName.value} did not finish deleting after 20 minutes. Result: $monitorDeleteResult"
                        )
                      } else {
                        // verify disk is also deleted
                        for {
                          _ <- loggerIO.info(
                            s"AppCreationSpec: app ${googleProject.value}/${appName.value} delete result: $monitorDeleteResult"
                          )
                          getDiskResp <- LeonardoApiClient
                            .getDisk(googleProject, createAppRequest.diskConfig.get.name)
                            .attempt
                        } yield getDiskResp.toOption shouldBe None
                      }
                  } yield ()
                else IO.unit
            } yield ()
          }
      } yield succeed
    }
    res.unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }
}
