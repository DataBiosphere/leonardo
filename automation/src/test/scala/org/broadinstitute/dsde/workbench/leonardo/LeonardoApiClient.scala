package org.broadinstitute.dsde.workbench.leonardo

import java.util.UUID
import java.util.concurrent.TimeoutException

import cats.implicits._
import cats.effect.{IO, Resource, Timer}
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, DiskName}
import org.broadinstitute.dsde.workbench.leonardo.http.{
  BatchNodepoolCreateRequest,
  CreateAppRequest,
  CreateDiskRequest,
  CreateRuntime2Request,
  GetAppResponse,
  GetPersistentDiskResponse,
  ListAppResponse,
  ListPersistentDiskResponse,
  UpdateRuntimeRequest
}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util.ExecutionContexts
import org.http4s.client.middleware.Logger
import org.http4s.client.{blaze, Client}
import org.http4s.headers._
import org.http4s.circe.CirceEntityEncoder._
import org.broadinstitute.dsde.workbench.leonardo.http.DiskRoutesTestJsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.RuntimeRoutesTestJsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.AppRoutesTestJsonCodec._

import scala.concurrent.duration._
import ApiJsonDecoder._
import org.http4s._

import scala.concurrent.ExecutionContext.global
import org.broadinstitute.dsde.workbench.DoneCheckableSyntax._

import scala.util.control.NoStackTrace

object LeonardoApiClient {
  val defaultMediaType = `Content-Type`(MediaType.application.json)
  implicit def http4sBody[A](body: A)(implicit encoder: EntityEncoder[IO, A]): EntityBody[IO] =
    encoder.toEntity(body).body
  implicit val cs = IO.contextShift(global)
  // Once a runtime is deleted, leonardo returns 404 for getRuntime API call
  implicit def eitherDoneCheckable[A]: DoneCheckable[Either[Throwable, A]] = (op: Either[Throwable, A]) => op.isLeft

  implicit def getDiskDoneCheckable[A]: DoneCheckable[GetPersistentDiskResponse] =
    (op: GetPersistentDiskResponse) => op.status == DiskStatus.Ready

  implicit def getRuntimeDoneCheckable[A]: DoneCheckable[GetRuntimeResponseCopy] =
    (op: GetRuntimeResponseCopy) => op.status == ClusterStatus.Running || op.status == ClusterStatus.Error

  val client: Resource[IO, Client[IO]] = for {
    blockingEc <- ExecutionContexts.cachedThreadPool[IO]
    client <- blaze.BlazeClientBuilder[IO](blockingEc).resource
  } yield Logger[IO](logHeaders = false, logBody = true)(client)

  val rootUri = Uri.unsafeFromString(LeonardoConfig.Leonardo.apiUrl)
  val defaultCreateDiskRequest = CreateDiskRequest(
    Map.empty,
    None,
    None,
    None
  )

  val defaultCreateRuntime2Request = CreateRuntime2Request(
    Map("foo" -> UUID.randomUUID().toString),
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    Set.empty,
    Map.empty
  )

  val defaultCreateAppRequest = CreateAppRequest(
    None,
    AppType.Galaxy,
    None,
    Map.empty,
    Map.empty
  )

  val defaultBatchNodepoolRequest = BatchNodepoolCreateRequest(
    NumNodepools(10),
    None,
    None
  )

  def createRuntime(
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    createRuntime2Request: CreateRuntime2Request = defaultCreateRuntime2Request
  )(implicit client: Client[IO], authHeader: Authorization): IO[Unit] =
    client
      .expectOr[Unit](
        Request[IO](
          method = Method.POST,
          headers = Headers.of(authHeader, defaultMediaType),
          uri = rootUri.withPath(s"/api/google/v1/runtimes/${googleProject.value}/${runtimeName.asString}"),
          body = createRuntime2Request
        )
      )(onError(s"Failed to create runtime ${googleProject.value}/${runtimeName.asString}"))

  def createRuntimeWithWait(googleProject: GoogleProject,
                            runtimeName: RuntimeName,
                            createRuntime2Request: CreateRuntime2Request)(
    implicit timer: Timer[IO],
    client: Client[IO],
    authHeader: Authorization
  ): IO[GetRuntimeResponseCopy] =
    for {
      _ <- createRuntime(googleProject, runtimeName, createRuntime2Request)
      res <- waitUntilRunning(googleProject, runtimeName)
    } yield res

  def startRuntime(
    googleProject: GoogleProject,
    runtimeName: RuntimeName
  )(implicit client: Client[IO], authHeader: Authorization): IO[Unit] =
    client
      .expectOr[Unit](
        Request[IO](
          method = Method.POST,
          headers = Headers.of(authHeader),
          uri = rootUri.withPath(s"/api/google/v1/runtimes/${googleProject.value}/${runtimeName.asString}/start")
        )
      )(onError(s"Failed to start runtime ${googleProject.value}/${runtimeName.asString}"))

  def startRuntimeWithWait(
    googleProject: GoogleProject,
    runtimeName: RuntimeName
  )(implicit client: Client[IO], authHeader: Authorization, timer: Timer[IO]): IO[GetRuntimeResponseCopy] =
    for {
      _ <- startRuntime(googleProject, runtimeName)
      res <- waitUntilRunning(googleProject, runtimeName)
    } yield res

  def updateRuntime(
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    req: UpdateRuntimeRequest
  )(implicit client: Client[IO], authHeader: Authorization): IO[Unit] =
    client
      .expectOr[Unit](
        Request[IO](
          method = Method.PATCH,
          headers = Headers.of(authHeader, defaultMediaType),
          uri = rootUri.withPath(s"/api/google/v1/runtimes/${googleProject.value}/${runtimeName.asString}"),
          body = req
        )
      )(onError(s"Failed to update runtime ${googleProject.value}/${runtimeName.asString}"))

  def createApp(
    googleProject: GoogleProject,
    appName: AppName,
    createAppRequest: CreateAppRequest = defaultCreateAppRequest
  )(implicit client: Client[IO], authHeader: Authorization): IO[Unit] =
    client
      .expectOr[Unit](
        Request[IO](
          method = Method.POST,
          headers = Headers.of(authHeader, defaultMediaType),
          uri = rootUri.withPath(s"/api/google/v1/apps/${googleProject.value}/${appName.value}"),
          body = createAppRequest
        )
      )(onError(s"Failed to create app ${googleProject.value}/${appName.value}"))

  //This line causes the body to be decoded as JSON, which will prevent error messagges from being seen
  //If you care about the error message, place the function before this line
  import org.http4s.circe.CirceEntityDecoder._

  def waitUntilRunning(googleProject: GoogleProject, runtimeName: RuntimeName, shouldError: Boolean = true)(
    implicit timer: Timer[IO],
    client: Client[IO],
    authHeader: Authorization
  ): IO[GetRuntimeResponseCopy] = {
    val ioa = getRuntime(googleProject, runtimeName)
    for {
      res <- timer.sleep(80 seconds) >> streamFUntilDone(ioa, 60, 10 seconds).compile.lastOrError
      _ <- res.status match {
        case ClusterStatus.Error =>
          if (shouldError)
            IO.raiseError(
              new RuntimeException(s"${googleProject.value}/${runtimeName.asString} errored due to ${res.errors}")
            )
          else IO.pure(res)
        case ClusterStatus.Running => IO.unit
        case other =>
          IO.raiseError(
            new TimeoutException(
              s"create runtime ${googleProject.value}/${runtimeName.asString}. Runtime is still in ${other}"
            )
          )
      }
    } yield res
  }

  def getRuntime(
    googleProject: GoogleProject,
    runtimeName: RuntimeName
  )(implicit client: Client[IO], authHeader: Authorization): IO[GetRuntimeResponseCopy] =
    client.expectOr[GetRuntimeResponseCopy](
      Request[IO](
        method = Method.GET,
        headers = Headers.of(authHeader),
        uri = rootUri.withPath(s"/api/google/v1/runtimes/${googleProject.value}/${runtimeName.asString}")
      )
    )(onError(s"Failed to get runtime ${googleProject.value}/${runtimeName.asString}"))

  def deleteRuntime(googleProject: GoogleProject,
                    runtimeName: RuntimeName,
                    deleteDisk: Boolean = true)(implicit client: Client[IO], authHeader: Authorization): IO[Unit] =
    client
      .expectOr[Unit](
        Request[IO](
          method = Method.DELETE,
          headers = Headers.of(authHeader),
          uri = rootUri
            .withPath(s"/api/google/v1/runtimes/${googleProject.value}/${runtimeName.asString}")
            .withQueryParam("deleteDisk", deleteDisk)
        )
      )(onError(s"Failed to delete runtime ${googleProject.value}/${runtimeName.asString}"))

  def deleteRuntimeWithWait(googleProject: GoogleProject, runtimeName: RuntimeName, deleteDisk: Boolean = true)(
    implicit timer: Timer[IO],
    client: Client[IO],
    authHeader: Authorization
  ): IO[Unit] =
    for {
      _ <- deleteRuntime(googleProject, runtimeName, deleteDisk)
      ioa = getRuntime(googleProject, runtimeName).attempt
      res <- timer.sleep(20 seconds) >> streamFUntilDone(ioa, 50, 5 seconds).compile.lastOrError
      _ <- if (res.isDone) IO.unit
      else IO.raiseError(new TimeoutException(s"delete runtime ${googleProject.value}/${runtimeName.asString}"))
    } yield ()

  def createDisk(
    googleProject: GoogleProject,
    diskName: DiskName,
    createDiskRequest: CreateDiskRequest = defaultCreateDiskRequest
  )(implicit client: Client[IO], authHeader: Authorization): IO[Unit] =
    client
      .expectOr[Unit](
        Request[IO](
          method = Method.POST,
          headers = Headers.of(authHeader, defaultMediaType),
          uri = rootUri.withPath(s"/api/google/v1/disks/${googleProject.value}/${diskName.value}"),
          body = createDiskRequest
        )
      )(onError(s"Failed to create disk ${googleProject.value}/${diskName.value}"))

  def createDiskWithWait(googleProject: GoogleProject, diskName: DiskName, createDiskRequest: CreateDiskRequest)(
    implicit timer: Timer[IO],
    client: Client[IO],
    authHeader: Authorization
  ): IO[Unit] =
    for {
      _ <- createDisk(googleProject, diskName, createDiskRequest)
      ioa = getDisk(googleProject, diskName)
      _ <- streamFUntilDone(ioa, 5, 5 seconds).compile.lastOrError
    } yield ()

  def getDisk(
    googleProject: GoogleProject,
    diskName: DiskName
  )(implicit client: Client[IO], authHeader: Authorization): IO[GetPersistentDiskResponse] =
    client.expectOr[GetPersistentDiskResponse](
      Request[IO](
        method = Method.GET,
        headers = Headers.of(authHeader),
        uri = rootUri
          .withPath(s"/api/google/v1/disks/${googleProject.value}/${diskName.value}")
      )
    )(onError(s"Failed to get disk ${googleProject.value}/${diskName.value}"))

  def listDisk(
    googleProject: GoogleProject,
    includeDeleted: Boolean = false
  )(implicit client: Client[IO], authHeader: Authorization): IO[List[ListPersistentDiskResponse]] = {
    val uriWithoutQueryParam = rootUri
      .withPath(s"/api/google/v1/disks/${googleProject.value}")

    val uri =
      if (includeDeleted) uriWithoutQueryParam.withQueryParam("includeDeleted", "true")
      else uriWithoutQueryParam
    client.expectOr[List[ListPersistentDiskResponse]](
      Request[IO](
        method = Method.GET,
        headers = Headers.of(authHeader),
        uri = uri
      )
    )(onError(s"Failed to list disks in project ${googleProject.value}"))
  }

  def deleteDisk(googleProject: GoogleProject, diskName: DiskName)(implicit client: Client[IO],
                                                                   authHeader: Authorization): IO[Unit] =
    client
      .expectOr[Unit](
        Request[IO](
          method = Method.DELETE,
          headers = Headers.of(authHeader),
          uri = rootUri.withPath(s"/api/google/v1/disks/${googleProject.value}/${diskName.value}")
        )
      )(onError(s"Failed to delete disk ${googleProject.value}/${diskName.value}"))

  def deleteDiskWithWait(googleProject: GoogleProject, diskName: DiskName)(
    implicit timer: Timer[IO],
    client: Client[IO],
    authHeader: Authorization
  ): IO[Unit] =
    for {
      _ <- deleteDisk(googleProject, diskName)
      ioa = getDisk(googleProject, diskName).attempt
      res <- timer.sleep(3 seconds) >> streamFUntilDone(ioa, 5, 5 seconds).compile.lastOrError
      _ <- if (res.isDone) IO.unit
      else IO.raiseError(new TimeoutException(s"delete disk ${googleProject.value}/${diskName.value}"))
    } yield ()

  private def onError(message: String)(response: Response[IO]): IO[Throwable] =
    for {
      body <- response.bodyText.compile.foldMonoid
    } yield RestError(message, response.status, body)

  def deleteApp(googleProject: GoogleProject, appName: AppName)(implicit client: Client[IO],
                                                                authHeader: Authorization): IO[Unit] =
    client
      .expectOr[Unit](
        Request[IO](
          method = Method.DELETE,
          headers = Headers.of(authHeader),
          uri = rootUri.withPath(s"/api/google/v1/apps/${googleProject.value}/${appName.value}")
        )
      )(onError(s"Failed to delete app ${googleProject.value}/${appName.value}"))

  def getApp(googleProject: GoogleProject, appName: AppName)(implicit client: Client[IO],
                                                             authHeader: Authorization): IO[GetAppResponse] =
    client.expectOr[GetAppResponse](
      Request[IO](
        method = Method.GET,
        headers = Headers.of(authHeader),
        uri = rootUri
          .withPath(s"/api/google/v1/apps/${googleProject.value}/${appName.value}")
      )
    )(onError(s"Failed to get app ${googleProject.value}/${appName.value}"))

  def listApps(
    googleProject: GoogleProject,
    includeDeleted: Boolean = false
  )(implicit client: Client[IO], authHeader: Authorization): IO[List[ListAppResponse]] = {
    val uriWithoutQueryParam = rootUri
      .withPath(s"/api/google/v1/apps/${googleProject.value}")

    val uri =
      if (includeDeleted) uriWithoutQueryParam.withQueryParam("includeDeleted", "true")
      else uriWithoutQueryParam
    client.expectOr[List[ListAppResponse]](
      Request[IO](
        method = Method.GET,
        headers = Headers.of(authHeader),
        uri = uri
      )
    )(onError(s"Failed to list apps in project ${googleProject.value}"))
  }

  def batchNodepoolCreate(
    googleProject: GoogleProject,
    req: BatchNodepoolCreateRequest = defaultBatchNodepoolRequest
  )(implicit client: Client[IO], authHeader: Authorization): IO[Unit] =
    client
      .expectOr[Unit](
        Request[IO](
          method = Method.POST,
          headers = Headers.of(authHeader, defaultMediaType),
          uri = rootUri.withPath(s"/api/google/v1/apps/${googleProject.value}/batchNodepoolCreate"),
          body = req
        )
      )(onError(s"Failed to batch create node pools in project ${googleProject.value}"))
}

final case class RestError(message: String, statusCode: Status, body: String) extends NoStackTrace {
  override def getMessage: String = s"message: ${message}, status: ${statusCode} body: ${message}"
}
