package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.{IO, Resource, Timer}
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, DiskName}
import org.broadinstitute.dsde.workbench.leonardo.ApiJsonDecoder._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util.ExecutionContexts
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.middleware.Logger
import org.http4s.client.{blaze, Client}
import org.http4s.headers.Authorization
import org.http4s.{Headers, Method, Request, Uri}

import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._

object LeonardoApiClient {
  implicit val cs = IO.contextShift(global)
  // Once a runtime is deleted, leonardo returns 404 for getRuntime API call
  implicit def eitherDoneCheckable[A]: DoneCheckable[Either[Throwable, A]] = new DoneCheckable[Either[Throwable, A]] {
    def isDone(op: Either[Throwable, A]): Boolean = op.isLeft
  }

  val client: Resource[IO, Client[IO]] = for {
    blockingEc <- ExecutionContexts.cachedThreadPool[IO]
    client <- blaze.BlazeClientBuilder[IO](blockingEc).resource
  } yield Logger[IO](logHeaders = false, logBody = true)(client)

  def getDisk(
    googleProject: GoogleProject,
    diskName: DiskName
  )(implicit client: Client[IO], authHeader: Authorization): IO[GetPersistentDiskResponse] =
    client.expect[GetPersistentDiskResponse](
      Request[IO](
        method = Method.GET,
        headers = Headers.of(authHeader),
        uri = Uri
          .unsafeFromString(LeonardoConfig.Leonardo.apiUrl)
          .withPath(s"/api/google/v1/disks/${googleProject.value}/${diskName.value}")
      )
    )

  def getRuntime(
    googleProject: GoogleProject,
    runtimeName: RuntimeName
  )(implicit client: Client[IO], authHeader: Authorization): IO[GetRuntimeResponseCopy] =
    client.expect[GetRuntimeResponseCopy](
      Request[IO](
        method = Method.GET,
        headers = Headers.of(authHeader),
        uri = Uri
          .unsafeFromString(LeonardoConfig.Leonardo.apiUrl)
          .withPath(s"/api/google/v1/runtimes/${googleProject.value}/${runtimeName.asString}")
      )
    )

  def deleteRuntime(googleProject: GoogleProject, runtimeName: RuntimeName)(implicit client: Client[IO],
                                                                            authHeader: Authorization): IO[Unit] =
    client
      .successful(
        Request[IO](
          method = Method.DELETE,
          headers = Headers.of(authHeader),
          uri = Uri
            .unsafeFromString(LeonardoConfig.Leonardo.apiUrl)
            .withPath(s"/api/google/v1/runtimes/${googleProject.value}/${runtimeName.asString}")
        )
      )
      .flatMap { success =>
        if (success)
          IO.unit
        else IO.raiseError(new Exception(s"Fail to delete runtime ${googleProject.value}/${runtimeName.asString}"))
      }

  def deleteRuntimeWithWait(googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit timer: Timer[IO],
    client: Client[IO],
    authHeader: Authorization
  ): IO[Unit] =
    for {
      _ <- deleteRuntime(googleProject, runtimeName)
      ioa = getRuntime(googleProject, runtimeName).attempt
      _ <- streamFUntilDone(ioa, 10, 10 seconds).compile.lastOrError
    } yield ()

  def deleteDisk(googleProject: GoogleProject, diskName: DiskName)(implicit client: Client[IO],
                                                                   authHeader: Authorization): IO[Unit] =
    client
      .successful(
        Request[IO](
          method = Method.DELETE,
          headers = Headers.of(authHeader),
          uri = Uri
            .unsafeFromString(LeonardoConfig.Leonardo.apiUrl)
            .withPath(s"/api/google/v1/disks/${googleProject.value}/${diskName.value}")
        )
      )
      .flatMap { success =>
        if (success)
          IO.unit
        else IO.raiseError(new Exception(s"Fail to delete disk ${googleProject.value}/${diskName.value}"))
      }

  def deleteDiskWithWait(googleProject: GoogleProject, diskName: DiskName)(
    implicit timer: Timer[IO],
    client: Client[IO],
    authHeader: Authorization
  ): IO[Unit] =
    for {
      _ <- deleteDisk(googleProject, diskName)
      ioa = getDisk(googleProject, diskName).attempt
      _ <- streamFUntilDone(ioa, 5, 5 seconds).compile.lastOrError
    } yield ()
}
