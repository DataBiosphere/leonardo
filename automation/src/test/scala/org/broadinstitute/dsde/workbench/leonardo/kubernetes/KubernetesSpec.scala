package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.google2.streamFUntilDone
import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient._
import org.http4s.client.Client
import cats.effect.IO
import cats.implicits._
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.leonardo.http.{GetAppResponse, ListAppResponse, PersistentDiskRequest}
import org.http4s.{AuthScheme, Credentials}
import org.http4s.headers.Authorization
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

import scala.concurrent.duration._

//@DoNotDiscover
class KubernetesSpec extends GPAllocFixtureSpec with LeonardoTestUtils with GPAllocUtils with ParallelTestExecution {

  implicit val authTokenForOldApiClient = ronAuthToken
  implicit val auth: Authorization =
    Authorization(Credentials.Token(AuthScheme.Bearer, ronCreds.makeAuthToken().value))

  val dependencies = for {
    httpClient <- LeonardoApiClient.client
  } yield AppDependencies(httpClient)

  "create app when cluster doesn't exist" - {
    withNewProject { googleProject =>
      val appName = randomAppName
      val appName2 = randomAppName

      val app1DeletedDoneCheckable: DoneCheckable[List[ListAppResponse]] =
        x => x.filter(_.appName == appName).map(_.status).distinct == List(AppStatus.Deleted)

      val app2DeletedDoneCheckable: DoneCheckable[List[ListAppResponse]] =
        x => x.filter(_.appName == appName2).map(_.status).distinct == List(AppStatus.Deleted)

      logger.info(s"Google Project 1 " + googleProject.value)

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

          _ <- loggerIO.info("About to create app")

          _ <- LeonardoApiClient.createApp(googleProject, appName, createAppRequest)

          gar = LeonardoApiClient.getApp(googleProject, appName)

          getAppResponse <- gar

          _ = getAppResponse.status should (be(AppStatus.Provisioning) or be(AppStatus.Precreating))

          monitorStartingResult <- testTimer.sleep(120 seconds) >> streamFUntilDone(gar, 30, 30 seconds)(
            testTimer,
            creatingDoneCheckable
          ).compile.lastOrError

          _ <- loggerIO.info(s"app 1 monitor result: ${monitorStartingResult}")

          _ = monitorStartingResult.status shouldBe AppStatus.Running

          _ <- LeonardoApiClient.deleteApp(googleProject, appName)

          listApps = LeonardoApiClient.listApps(googleProject, true)

          monitorApp1DeletionResult <- testTimer.sleep(30 seconds) >> streamFUntilDone(listApps, 120, 10 seconds)(
            testTimer,
            app1DeletedDoneCheckable
          ).compile.lastOrError

          _ <- loggerIO.info(s"app1 delete result: $monitorApp1DeletionResult")

          _ <- testTimer.sleep(480 seconds)

          _ <- loggerIO.info("About to create app2")

          _ <- LeonardoApiClient.createApp(googleProject, appName2, createAppRequest)

          gar = LeonardoApiClient.getApp(googleProject, appName2)

          getAppResponse <- gar

          _ = getAppResponse.status should (be(AppStatus.Provisioning) or be(AppStatus.Precreating))

          monitorApp2CreationResult <- testTimer.sleep(180 seconds) >> streamFUntilDone(gar, 30, 30 seconds)(
            testTimer,
            creatingDoneCheckable
          ).compile.lastOrError

          _ <- loggerIO.info(s"app 2 monitor result: ${monitorApp2CreationResult}")

          _ = monitorApp2CreationResult.status shouldBe AppStatus.Running

          _ <- LeonardoApiClient.deleteApp(googleProject, appName2)

          listApps = LeonardoApiClient.listApps(googleProject, true)

          monitorApp2DeletionResult <- testTimer.sleep(30 seconds) >> streamFUntilDone(listApps, 120, 10 seconds)(
            testTimer,
            app2DeletedDoneCheckable
          ).compile.lastOrError

          _ <- loggerIO.info(s"app 2 delete result: $monitorApp2DeletionResult")

        } yield ()
      }
      res.unsafeRunSync()
    }
  }

  "create app in a cluster that exists" - {
    withNewProject { googleProject =>
      val appName = randomAppName
      val appName2 = randomAppName

      val app1DeletedDoneCheckable: DoneCheckable[List[ListAppResponse]] =
        x => x.filter(_.appName == appName).map(_.status).distinct == List(AppStatus.Deleted)

      val app2DeletedDoneCheckable: DoneCheckable[List[ListAppResponse]] =
        x => x.filter(_.appName == appName2).map(_.status).distinct == List(AppStatus.Deleted)

      logger.info(s"Google Project 2 " + googleProject.value)

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

      val createAppRequest2 = defaultCreateAppRequest.copy(
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

          _ <- loggerIO.info("About to create app 1")

          _ <- LeonardoApiClient.createApp(googleProject, appName, createAppRequest)

          gar = LeonardoApiClient.getApp(googleProject, appName)

          getAppResponse <- gar

          _ = getAppResponse.status should (be(AppStatus.Provisioning) or be(AppStatus.Precreating))

          monitorStartingResult <- testTimer.sleep(120 seconds) >> streamFUntilDone(gar, 30, 30 seconds)(
            testTimer,
            creatingDoneCheckable
          ).compile.lastOrError

          _ <- loggerIO.info(s"app monitor result: ${monitorStartingResult}")

          _ = monitorStartingResult.status shouldBe AppStatus.Running

          _ <- loggerIO.info("About to create app 2")

          _ <- LeonardoApiClient.createApp(googleProject, appName2, createAppRequest2)

          gar = LeonardoApiClient.getApp(googleProject, appName2)

          getAppResponse <- gar

          _ = getAppResponse.status should (be(AppStatus.Provisioning) or be(AppStatus.Precreating))

          monitorStartingResult <- testTimer.sleep(120 seconds) >> streamFUntilDone(gar, 30, 30 seconds)(
            testTimer,
            creatingDoneCheckable
          ).compile.lastOrError

          _ <- loggerIO.info(s"app monitor result: ${monitorStartingResult}")

          _ = monitorStartingResult.status shouldBe AppStatus.Running

          _ <- loggerIO.info(s"listing apps")

          listOfApps <- LeonardoApiClient.listApps(googleProject)

          appStatusValue = listOfApps.collect { case resp if resp.appName == appName => resp.status }

          _ = appStatusValue.head shouldBe AppStatus.Running

          app2StatusValue = listOfApps.collect { case resp if resp.appName == appName2 => resp.status }

          _ = app2StatusValue.head shouldBe AppStatus.Running

          _ <- LeonardoApiClient.deleteApp(googleProject, appName)

          listApps = LeonardoApiClient.listApps(googleProject, true)

          monitorApp1DeletionResult <- testTimer.sleep(30 seconds) >> streamFUntilDone(listApps, 120, 10 seconds)(
            testTimer,
            app1DeletedDoneCheckable
          ).compile.lastOrError

          _ <- loggerIO.info(s"app1 delete result: $monitorApp1DeletionResult")

          _ <- LeonardoApiClient.deleteApp(googleProject, appName2)

          monitorApp2DeletionResult <- testTimer.sleep(30 seconds) >> streamFUntilDone(listApps, 120, 10 seconds)(
            testTimer,
            app2DeletedDoneCheckable
          ).compile.lastOrError

          _ <- loggerIO.info(s"app1 delete result: $monitorApp2DeletionResult")
        } yield ()
      }
      res.unsafeRunSync()
    }
  }
}

final case class AppDependencies(httpClient: Client[IO])
