package org.broadinstitute.dsde.workbench.leonardo
package apps

import org.broadinstitute.dsde.workbench.google2.Generators
import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient._
import org.broadinstitute.dsde.workbench.leonardo.http.PersistentDiskRequest
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

@DoNotDiscover
class AppCreationSpec extends GPAllocFixtureSpec with LeonardoTestUtils with GPAllocUtils with ParallelTestExecution {
  implicit val auth: Authorization =
    Authorization(Credentials.Token(AuthScheme.Bearer, ronCreds.makeAuthToken().value))

  "create, delete an app and re-create an app with same disk" taggedAs Tags.SmokeTest in { googleProject =>
    val appName = randomAppName
    val restoreAppName = AppName(s"restore-${appName.value}")
    val diskName = Generators.genDiskName.sample.get

    val createAppRequest = defaultCreateAppRequest.copy(
      diskConfig = Some(
        PersistentDiskRequest(
          diskName,
          Some(DiskSize(300)),
          None,
          Map.empty
        )
      ),
      customEnvironmentVariables = Map("WORKSPACE_NAME" -> "Galaxy-Workshop-ASHG_2020_GWAS_Demo")
    )

    LeonardoApiClient.client
      .use { implicit client =>
        for {
          _ <- loggerIO.info(s"AppCreationSpec: About to create app ${googleProject.value}/${appName.value}")

          // Create the app
          _ <- LeonardoApiClient.createAppWithRetry(googleProject, appName, createAppRequest)

          // Verify the initial getApp call
          getApp = LeonardoApiClient.getApp(googleProject, appName)
          getAppResponse <- getApp
          _ = getAppResponse.status should (be(AppStatus.Provisioning) or be(AppStatus.Precreating))

          // Verify the app eventually becomes Running
          _ <- waitUntilAppRunning(googleProject, appName)

          // Delete the app
          _ <- LeonardoApiClient.deleteApp(googleProject, appName, false)

          // Verify getApp again
          getAppResponse <- getApp
          _ = getAppResponse.status should (be(AppStatus.Deleting) or be(AppStatus.Predeleting))

          // Verify the app eventually becomes Deleted
          // Don't fail the test if the deletion times out because the Galaxy pre-delete job can sporadically fail.
          // See https://broadworkbench.atlassian.net/browse/IA-2471
          // TODO remove attempt when Galaxy deletion is reliable.
          deleteResult <- LeonardoApiClient.waitUntilAppDeleted(googleProject, appName).attempt

          _ <- deleteResult match {
            case Left(e) =>
              loggerIO.warn(e)(
                s"AppCreationSpec: app ${googleProject.value}/${appName.value} did not finish deleting after 20 minutes."
              )
            case Right(_) =>
              // Verify creating another app with the same disk doesn't error out
              for {
                _ <- loggerIO.info(
                  s"AppCreationSpec: app ${googleProject.value}/${appName.value} has been deleted."
                )
                _ <- LeonardoApiClient.createAppWithRetry(googleProject, restoreAppName, createAppRequest)
                _ <- waitUntilAppRunning(googleProject, appName)
              } yield ()
          }
        } yield ()
      }
      .unsafeRunSync()
  }

  "stop and start an app" in { googleProject =>
    val appName = randomAppName
    val diskName = Generators.genDiskName.sample.get

    val createAppRequest = defaultCreateAppRequest.copy(
      diskConfig = Some(
        PersistentDiskRequest(
          diskName,
          Some(DiskSize(500)),
          None,
          Map.empty
        )
      )
    )

    LeonardoApiClient.client
      .use { implicit client =>
        for {
          _ <- loggerIO.info(s"AppCreationSpec: About to create app ${googleProject.value}/${appName.value}")

          // Create the app
          _ <- LeonardoApiClient.createAppWithRetry(googleProject, appName, createAppRequest)

          // Verify the initial getApp call
          getApp = LeonardoApiClient.getApp(googleProject, appName)
          getAppResponse <- getApp
          _ = getAppResponse.status should (be(AppStatus.Provisioning) or be(AppStatus.Precreating))

          // Verify the app eventually becomes Running
          _ <- waitUntilAppRunning(googleProject, appName)

          // Stop the app
          _ <- LeonardoApiClient.stopApp(googleProject, appName)

          // Verify getApp again
          getAppResponse <- getApp
          _ = getAppResponse.status should (be(AppStatus.Stopping) or be(AppStatus.PreStopping))

          // Verify the app eventually becomes Stopped
          _ <- LeonardoApiClient.waitUntilAppStopped(googleProject, appName)

          // Start the app
          _ <- LeonardoApiClient.startApp(googleProject, appName)

          // Verify getApp again
          getAppResponse <- getApp
          _ = getAppResponse.status should (be(AppStatus.Starting) or be(AppStatus.PreStarting))

          // Verify the app eventually becomes Running
          _ <- waitUntilAppRunning(googleProject, appName)

          // Delete the app
          _ <- LeonardoApiClient.deleteApp(googleProject, appName, true)

          // Verify getApp again
          getAppResponse <- getApp
          _ = getAppResponse.status should (be(AppStatus.Deleting) or be(AppStatus.Predeleting))

          // Verify getApp again
          getAppResponse <- getApp
          _ = getAppResponse.status should (be(AppStatus.Deleting) or be(AppStatus.Predeleting))

          // Verify the app eventually becomes Deleted
          // Don't fail the test if the deletion times out because the Galaxy pre-delete job can sporadically fail.
          // See https://broadworkbench.atlassian.net/browse/IA-2471
          // TODO remove attempt when Galaxy deletion is reliable.
          deleteResult <- LeonardoApiClient.waitUntilAppDeleted(googleProject, appName).attempt

          _ <- deleteResult match {
            case Left(e) =>
              loggerIO.warn(e)(
                s"AppCreationSpec: app ${googleProject.value}/${appName.value} did not finish deleting after 20 minutes."
              )
            case Right(_) =>
              for {
                _ <- loggerIO.info(
                  s"AppCreationSpec: app ${googleProject.value}/${appName.value} has been deleted."
                )
                getDiskResp <- LeonardoApiClient.getDisk(googleProject, diskName).attempt
              } yield getDiskResp.toOption shouldBe (None)
          }
        } yield ()
      }
      .unsafeRunSync()
  }

}
