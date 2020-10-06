package org.broadinstitute.dsde.workbench.leonardo
package apps

import cats.effect.IO
import cats.implicits._
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.google2.streamFUntilDone
import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient._
import org.broadinstitute.dsde.workbench.leonardo.http.{GetAppResponse, ListAppResponse, PersistentDiskRequest}
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

import scala.concurrent.duration._

@DoNotDiscover
class AppCreationSpec extends GPAllocFixtureSpec with LeonardoTestUtils with GPAllocUtils with ParallelTestExecution {

  implicit val authTokenForOldApiClient = ronAuthToken
  implicit val auth: Authorization =
    Authorization(Credentials.Token(AuthScheme.Bearer, ronCreds.makeAuthToken().value))

  val dependencies = for {
    httpClient <- LeonardoApiClient.client
  } yield AppDependencies(httpClient)

  "create app when cluster doesn't exist" in { _ =>
    withNewProject { googleProject =>
      val appName = randomAppName
      val appName2 = randomAppName

      val app1DeletedDoneCheckable: DoneCheckable[List[ListAppResponse]] =
        x => x.filter(_.appName == appName).map(_.status).distinct == List(AppStatus.Deleted)

      val app2DeletedDoneCheckable: DoneCheckable[List[ListAppResponse]] =
        x => x.filter(_.appName == appName2).map(_.status).distinct == List(AppStatus.Deleted)

      logger.info(s"AppCreationSpec: Google Project 1 " + googleProject.value)

      val createAppRequest = defaultCreateAppRequest.copy(
        diskConfig = Some(
          PersistentDiskRequest(
            randomDiskName,
            Some(DiskSize(30)),
            None,
            Map.empty
          )
        )
      )
      val res = dependencies.use { dep =>
        implicit val client = dep.httpClient
        val creatingDoneCheckable: DoneCheckable[GetAppResponse] =
          x => x.status == AppStatus.Running

        for {

          _ <- loggerIO.info(s"AppCreationSpec: About to create app ${googleProject}/${appName}")

          _ <- LeonardoApiClient.createApp(googleProject, appName, createAppRequest)

          gar = LeonardoApiClient.getApp(googleProject, appName)

          getAppResponse <- gar

          _ = getAppResponse.status should (be(AppStatus.Provisioning) or be(AppStatus.Precreating))

          monitorStartingResult <- testTimer.sleep(120 seconds) >> streamFUntilDone(gar, 120, 10 seconds)(
            testTimer,
            creatingDoneCheckable
          ).compile.lastOrError

          _ <- loggerIO.info(
            s"AppCreationSpec: app ${googleProject}/${appName} monitor result: ${monitorStartingResult}"
          )

          _ = monitorStartingResult.status shouldBe AppStatus.Running

          _ <- LeonardoApiClient.deleteApp(googleProject, appName)

          listApps = LeonardoApiClient.listApps(googleProject, true)

          monitorApp1DeletionResult <- testTimer.sleep(30 seconds) >> streamFUntilDone(listApps, 120, 10 seconds)(
            testTimer,
            app1DeletedDoneCheckable
          ).compile.lastOrError

          _ <- loggerIO.info(
            s"AppCreationSpec: app ${googleProject}/${appName} delete result: $monitorApp1DeletionResult"
          )

          _ = app1DeletedDoneCheckable.isDone(monitorApp1DeletionResult) shouldBe true

          // TODO investigate why this is necessary - in theory the second app should be able
          // to be created after the first is deleted.
          _ <- testTimer.sleep(600 seconds)

          _ <- loggerIO.info(s"AppCreationSpec: About to create app ${googleProject}/${appName2}")

          _ <- LeonardoApiClient.createApp(googleProject, appName2, createAppRequest)

          gar = LeonardoApiClient.getApp(googleProject, appName2)

          getAppResponse <- gar

          _ = getAppResponse.status should (be(AppStatus.Provisioning) or be(AppStatus.Precreating))

          monitorApp2CreationResult <- testTimer.sleep(180 seconds) >> streamFUntilDone(gar, 120, 10 seconds)(
            testTimer,
            creatingDoneCheckable
          ).compile.lastOrError

          _ <- loggerIO.info(
            s"AppCreationSpec: app ${googleProject}/${appName2} monitor result: ${monitorApp2CreationResult}"
          )

          _ = monitorApp2CreationResult.status shouldBe AppStatus.Running

          _ <- LeonardoApiClient.deleteApp(googleProject, appName2)

          listApps = LeonardoApiClient.listApps(googleProject, true)

          monitorApp2DeletionResult <- testTimer.sleep(30 seconds) >> streamFUntilDone(listApps, 120, 10 seconds)(
            testTimer,
            app2DeletedDoneCheckable
          ).compile.lastOrError

          _ <- loggerIO.info(
            s"AppCreationSpec: app ${googleProject}/${appName2} delete result: $monitorApp2DeletionResult"
          )

        } yield ()
      }
      res.unsafeRunSync()
    }
  }

}

final case class AppDependencies(httpClient: Client[IO])
