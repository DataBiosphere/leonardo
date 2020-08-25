package org.broadinstitute.dsde.workbench.leonardo.kubernetes

import org.broadinstitute.dsde.workbench.google2.streamFUntilDone
import org.broadinstitute.dsde.workbench.leonardo.{
  AppStatus,
  DiskSize,
  GPAllocBeforeAndAfterAll,
  GPAllocFixtureSpec,
  LeonardoApiClient,
  LeonardoTestUtils
}
import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient._
import org.http4s.client.Client
import cats.effect.IO
import cats.implicits._
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.leonardo.http.{CreateAppRequest, GetAppResponse, PersistentDiskRequest}
import org.broadinstitute.dsde.workbench.leonardo.notebooks.NotebookTestUtils
import org.http4s.{AuthScheme, Credentials}
import org.http4s.headers.Authorization
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

import scala.concurrent.duration._

//@DoNotDiscover
class KubernetesSpec
    extends GPAllocFixtureSpec
    with ParallelTestExecution
    with LeonardoTestUtils
    with NotebookTestUtils
    with GPAllocBeforeAndAfterAll {

  "KubernetesSpec" - {

    implicit val authTokenForOldApiClient = ronAuthToken
    implicit val auth: Authorization =
      Authorization(Credentials.Token(AuthScheme.Bearer, ronCreds.makeAuthToken().value))

    val dependencies = for {
      httpClient <- LeonardoApiClient.client
    } yield AppDependencies(httpClient)

    "create app when cluster doesn't exist" in { googleProject =>
      val appName = randomAppName
      val appName2 = randomAppName

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
          _ <- LeonardoApiClient.createApp(googleProject, appName, createAppRequest)

          gar = LeonardoApiClient.getApp(googleProject, appName)

          getAppResponse <- gar

          _ = getAppResponse.status should (be(AppStatus.Provisioning) or be(AppStatus.Precreating))

          monitorStartingResult <- testTimer.sleep(120 seconds) >> streamFUntilDone(gar, 30, 30 seconds)(
            testTimer,
            creatingDoneCheckable
          ).compile.lastOrError

          _ = monitorStartingResult.status shouldBe AppStatus.Running

          _ <- LeonardoApiClient.deleteAppWithWait(googleProject, appName)

          _ <- monitorDeleteApp(googleProject, appName)

          _ <- LeonardoApiClient.createApp(googleProject, appName2, createAppRequest)

          getAppResponse <- LeonardoApiClient.getApp(googleProject, appName2)

          _ = getAppResponse.status should (be(AppStatus.Provisioning) or be(AppStatus.Precreating))

        } yield ()
      }
      res.unsafeRunSync()
    }

    "create app in a cluster that exists" in { googleProject =>
      val appName = randomAppName
      val appName2 = randomAppName
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
          _ <- LeonardoApiClient.createApp(googleProject, appName, createAppRequest)

          gar = LeonardoApiClient.getApp(googleProject, appName)

          getAppResponse <- gar

          _ = getAppResponse.status should (be(AppStatus.Provisioning) or be(AppStatus.Precreating))

          monitorStartingResult <- testTimer.sleep(120 seconds) >> streamFUntilDone(gar, 30, 30 seconds)(
            testTimer,
            creatingDoneCheckable
          ).compile.lastOrError

          _ = monitorStartingResult.status shouldBe AppStatus.Running

          _ <- LeonardoApiClient.createApp(googleProject, appName2, createAppRequest2)

          gar = LeonardoApiClient.getApp(googleProject, appName2)

          getAppResponse <- gar

          _ = getAppResponse.status should (be(AppStatus.Provisioning) or be(AppStatus.Precreating))

          monitorStartingResult <- testTimer.sleep(120 seconds) >> streamFUntilDone(gar, 30, 30 seconds)(
            testTimer,
            creatingDoneCheckable
          ).compile.lastOrError

          _ = monitorStartingResult.status shouldBe AppStatus.Running

          listOfApps <- LeonardoApiClient.listApps(googleProject)

          appStatusValue = listOfApps.collect { case resp if resp.appName == appName => resp.status }

          _ = appStatusValue.head shouldBe AppStatus.Running

          app2StatusValue = listOfApps.collect { case resp if resp.appName == appName2 => resp.status }

          _ = app2StatusValue.head shouldBe AppStatus.Running
        } yield ()
      }
      res.unsafeRunSync()
    }
  }
}

final case class AppDependencies(httpClient: Client[IO])
