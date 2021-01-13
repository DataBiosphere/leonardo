package org.broadinstitute.dsde.workbench.leonardo
package apps

import org.broadinstitute.dsde.workbench.google2.streamUntilDoneOrTimeout
import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient._
import org.broadinstitute.dsde.workbench.leonardo.http.PersistentDiskRequest
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

import scala.concurrent.duration._

@DoNotDiscover
class AppCreationSpec extends GPAllocFixtureSpec with LeonardoTestUtils with GPAllocUtils with ParallelTestExecution {
  implicit val auth: Authorization =
    Authorization(Credentials.Token(AuthScheme.Bearer, ronCreds.makeAuthToken().value))

  "create and delete an app" in { _ =>
    withNewProject { googleProject =>
      val appName = randomAppName

      val createAppRequest = defaultCreateAppRequest.copy(
        diskConfig = Some(
          PersistentDiskRequest(
            randomDiskName,
            Some(DiskSize(500)),
            None,
            Map.empty
          )
        )
      )

      LeonardoApiClient.client.use { implicit client =>
        for {
          _ <- loggerIO.info(s"AppCreationSpec: About to create app ${googleProject.value}/${appName.value}")

          // Create the app
          _ <- LeonardoApiClient.createApp(googleProject, appName, createAppRequest)

          // Verify the initial getApp call
          getApp = LeonardoApiClient.getApp(googleProject, appName)
          getAppResponse <- getApp
          _ = getAppResponse.status should (be(AppStatus.Provisioning) or be(AppStatus.Precreating))

          // Verify the app eventually becomes Running
          _ <- testTimer.sleep(60 seconds)
          monitorCreateResult <- streamUntilDoneOrTimeout(
            getApp,
            120,
            10 seconds,
            s"AppCreationSpec: app ${googleProject.value}/${appName.value} did not finish creating after 20 minutes"
          )(implicitly, implicitly, appInStateOrError(AppStatus.Running))
          _ <- loggerIO.info(
            s"AppCreationSpec: app ${googleProject.value}/${appName.value} monitor result: ${monitorCreateResult}"
          )
          _ = monitorCreateResult.status shouldBe AppStatus.Running

          _ <- testTimer.sleep(1 minute)

          // Delete the app
          _ <- LeonardoApiClient.deleteApp(googleProject, appName)

          // Verify getApp again
          getAppResponse <- getApp
          _ = getAppResponse.status should (be(AppStatus.Deleting) or be(AppStatus.Predeleting))

          // Verify the app eventually becomes Deleted
          listApps = LeonardoApiClient.listApps(googleProject, true)
          monitorDeleteResult <- streamUntilDoneOrTimeout(
            listApps,
            120,
            10 seconds,
            s"AppCreationSpec: app ${googleProject.value}/${appName.value} did not finish deleting after 20 minutes"
          )(implicitly, implicitly, appDeleted(appName))
          _ <- loggerIO.info(
            s"AppCreationSpec: app ${googleProject.value}/${appName.value} delete result: $monitorDeleteResult"
          )

        } yield ()
      }
    }
  }

  "stop and start an app" in { _ =>
    withNewProject { googleProject =>
      val appName = randomAppName

      val createAppRequest = defaultCreateAppRequest.copy(
        diskConfig = Some(
          PersistentDiskRequest(
            randomDiskName,
            Some(DiskSize(500)),
            None,
            Map.empty
          )
        )
      )

      LeonardoApiClient.client.use { implicit client =>
        for {
          _ <- loggerIO.info(s"AppCreationSpec: About to create app ${googleProject.value}/${appName.value}")

          // Create the app
          _ <- LeonardoApiClient.createApp(googleProject, appName, createAppRequest)

          // Verify the initial getApp call
          getApp = LeonardoApiClient.getApp(googleProject, appName)
          getAppResponse <- getApp
          _ = getAppResponse.status should (be(AppStatus.Provisioning) or be(AppStatus.Precreating))

          // Verify the app eventually becomes Running
          _ <- testTimer.sleep(60 seconds)
          monitorCreateResult <- streamUntilDoneOrTimeout(
            getApp,
            120,
            10 seconds,
            s"AppCreationSpec: app ${googleProject.value}/${appName.value} did not finish creating after 20 minutes"
          )(implicitly, implicitly, appInStateOrError(AppStatus.Running))
          _ <- loggerIO.info(
            s"AppCreationSpec: app ${googleProject.value}/${appName.value} monitor result: ${monitorCreateResult}"
          )
          _ = monitorCreateResult.status shouldBe AppStatus.Running

          // Stop the app
          _ <- LeonardoApiClient.stopApp(googleProject, appName)

          // Verify getApp again
          getAppResponse <- getApp
          _ = getAppResponse.status should (be(AppStatus.Stopping) or be(AppStatus.PreStopping))

          // Verify the app eventually becomes Stopped
          _ <- testTimer.sleep(30 seconds)
          monitorStopResult <- streamUntilDoneOrTimeout(
            getApp,
            120,
            10 seconds,
            s"AppCreationSpec: app ${googleProject.value}/${appName.value} did not finish stopping after 20 minutes"
          )(implicitly, implicitly, appInStateOrError(AppStatus.Stopped))
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
          _ <- testTimer.sleep(30 seconds)
          monitorStartResult <- streamUntilDoneOrTimeout(
            getApp,
            120,
            10 seconds,
            s"AppCreationSpec: app ${googleProject.value}/${appName.value} did not finish starting after 20 minutes"
          )(implicitly, implicitly, appInStateOrError(AppStatus.Running))
          _ <- loggerIO.info(
            s"AppCreationSpec: app ${googleProject.value}/${appName.value} start result: $monitorStartResult"
          )
          _ = monitorStartResult.status shouldBe AppStatus.Running

          _ <- testTimer.sleep(1 minute)

          // Delete the app
          _ <- LeonardoApiClient.deleteApp(googleProject, appName)

          // Verify getApp again
          getAppResponse <- getApp
          _ = getAppResponse.status should (be(AppStatus.Deleting) or be(AppStatus.Predeleting))

          // Verify the app eventually becomes Deleted
          listApps = LeonardoApiClient.listApps(googleProject, true)
          monitorDeleteResult <- streamUntilDoneOrTimeout(
            listApps,
            180,
            10 seconds,
            s"AppCreationSpec: app ${googleProject.value}/${appName.value} did not finish deleting after 30 minutes"
          )(implicitly, implicitly, appDeleted(appName))
          _ <- loggerIO.info(
            s"AppCreationSpec: app ${googleProject.value}/${appName.value} delete result: $monitorDeleteResult"
          )

        } yield ()
      }
    }
  }

}
