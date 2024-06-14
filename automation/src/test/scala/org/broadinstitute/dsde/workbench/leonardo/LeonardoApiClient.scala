package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.{IO, Resource}
import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.DoneCheckableSyntax._
import org.broadinstitute.dsde.workbench.google2.{
  streamFUntilDone,
  streamUntilDoneOrTimeout,
  DiskName,
  MachineTypeName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.ApiJsonDecoder._
import org.broadinstitute.dsde.workbench.leonardo.http.AppRoutesTestJsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.DiskRoutesTestJsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.RuntimeRoutesTestJsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util2.ExecutionContexts
import org.http4s._
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.Client
import org.http4s.client.middleware.{Logger, Retry, RetryPolicy}
import org.http4s.headers._
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.util.UUID
import java.util.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

/**
 * This is the legacy wrapper for leonardo APIs
 * You should not need to use this in new tests over `GeneratedLeonardoApiClient`
 * If you find yourself updating existing tests and using this, port over to `GeneratedLeonardoApiClient` unless you have a very good reason
 */
object LeonardoApiClient {
  val defaultMediaType = `Content-Type`(MediaType.application.json)
  implicit val logger: StructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  implicit def http4sBody[A](body: A)(implicit encoder: EntityEncoder[IO, A]): EntityBody[IO] =
    encoder.toEntity(body).body
  // Once a runtime is deleted, leonardo returns 404 for getRuntime API call
  implicit def eitherDoneCheckable[A]: DoneCheckable[Either[Throwable, A]] = (op: Either[Throwable, A]) => op.isLeft

  implicit def getDiskDoneCheckable[A]: DoneCheckable[GetPersistentDiskResponse] =
    (op: GetPersistentDiskResponse) => op.status == DiskStatus.Ready

  implicit def getRuntimeDoneCheckable[A]: DoneCheckable[GetRuntimeResponseCopy] =
    (op: GetRuntimeResponseCopy) => op.status == ClusterStatus.Running || op.status == ClusterStatus.Error

  val client: Resource[IO, Client[IO]] =
    for {
      blockingEc <- ExecutionContexts.cachedThreadPool[IO]
      retryPolicy = RetryPolicy[IO](RetryPolicy.exponentialBackoff(30 seconds, 5))
      client <- BlazeClientBuilder[IO](blockingEc).resource.map(c => Retry(retryPolicy)(c))
    } yield Logger[IO](logHeaders = true, logBody = true)(client)

  val defaultCreateRequestZone = ZoneName("us-east1-b")

  val rootUri = Uri.unsafeFromString(LeonardoConfig.Leonardo.apiUrl)
  val defaultCreateDiskRequest = CreateDiskRequest(
    Map.empty,
    None,
    None,
    None,
    Some(defaultCreateRequestZone),
    None
  )

  val defaultCreateRuntime2Request = CreateRuntimeRequest(
    Map("foo" -> UUID.randomUUID().toString),
    None,
    None,
    Some(RuntimeConfigRequest.GceConfig(None, None, Some(defaultCreateRequestZone), None)),
    None,
    None,
    None,
    None,
    None,
    None,
    Set.empty,
    Map.empty,
    None
  )

  val defaultCreateDataprocRuntimeRequest = CreateRuntimeRequest(
    Map("foo" -> UUID.randomUUID().toString),
    None,
    None,
    Some(
      RuntimeConfigRequest.DataprocConfig(
        Some(0),
        Some(MachineTypeName("n1-standard-4")),
        Some(DiskSize(100)),
        None,
        None,
        None,
        None,
        Map.empty,
        None,
        true,
        false
      )
    ),
    None,
    None,
    None,
    None,
    None,
    None,
    Set.empty,
    Map.empty,
    None
  )

  val defaultCreateAppRequest = CreateAppRequest(
    None,
    AppType.Galaxy,
    None,
    None,
    None,
    Map.empty,
    Map.empty,
    None,
    List.empty,
    None,
    None,
    None,
    None,
    None,
    None
  )

  val defaultCreateAzureRuntimeRequest = CreateAzureRuntimeRequest(
    Map.empty,
    VirtualMachineSizeTypes.STANDARD_DS1_V2,
    Map.empty,
    CreateAzureDiskRequest(
      Map.empty,
      AzureDiskName(UUID.randomUUID().toString.substring(0, 8)),
      None,
      None
    ),
    Some(0)
  )

  def createRuntime(
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    createRuntime2Request: CreateRuntimeRequest = defaultCreateRuntime2Request
  )(implicit client: Client[IO], authorization: IO[Authorization]): IO[Unit] =
    for {
      traceIdHeader <- genTraceIdHeader()
      authHeader <- authorization
      r <- client
        .run(
          Request[IO](
            method = Method.POST,
            headers = Headers(authHeader, defaultMediaType, traceIdHeader),
            uri = rootUri.withPath(
              Uri.Path.unsafeFromString(s"/api/google/v1/runtimes/${googleProject.value}/${runtimeName.asString}")
            ),
            entity = createRuntime2Request
          )
        )
        .use { resp =>
          if (!resp.status.isSuccess) {
            onError(s"Failed to create runtime ${googleProject.value}/${runtimeName.asString}")(resp)
              .flatMap(IO.raiseError)
          } else
            IO.unit
        }
    } yield r

  def createRuntimeWithWait(
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    createRuntime2Request: CreateRuntimeRequest
  )(implicit client: Client[IO], authorization: IO[Authorization]): IO[GetRuntimeResponseCopy] =
    for {
      _ <- createRuntime(googleProject, runtimeName, createRuntime2Request)
      res <- waitUntilRunning(googleProject, runtimeName)
    } yield res

  def startRuntime(
    googleProject: GoogleProject,
    runtimeName: RuntimeName
  )(implicit client: Client[IO], authorization: IO[Authorization]): IO[Unit] =
    for {
      traceIdHeader <- genTraceIdHeader()
      authHeader <- authorization
      r <- client
        .run(
          Request[IO](
            method = Method.POST,
            headers = Headers(authHeader, traceIdHeader),
            uri = rootUri.withPath(
              Uri.Path.unsafeFromString(s"/api/google/v1/runtimes/${googleProject.value}/${runtimeName.asString}/start")
            )
          )
        )
        .use { resp =>
          if (!resp.status.isSuccess) {
            onError(s"Failed to start runtime ${googleProject.value}/${runtimeName.asString}")(resp)
              .flatMap(IO.raiseError)
          } else
            IO.unit
        }
    } yield r

  def startRuntimeWithWait(
    googleProject: GoogleProject,
    runtimeName: RuntimeName
  )(implicit client: Client[IO], authorization: IO[Authorization]): IO[GetRuntimeResponseCopy] =
    for {
      _ <- startRuntime(googleProject, runtimeName)
      res <- waitUntilRunning(googleProject, runtimeName)
    } yield res

  def updateRuntime(
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    req: UpdateRuntimeRequest
  )(implicit client: Client[IO], authorization: IO[Authorization]): IO[Unit] =
    for {
      traceIdHeader <- genTraceIdHeader()
      authHeader <- authorization
      r <- client
        .run(
          Request[IO](
            method = Method.PATCH,
            headers = Headers(authHeader, defaultMediaType, traceIdHeader),
            uri = rootUri.withPath(
              Uri.Path.unsafeFromString(s"/api/google/v1/runtimes/${googleProject.value}/${runtimeName.asString}")
            ),
            entity = req
          )
        )
        .use { resp =>
          if (!resp.status.isSuccess) {
            onError(s"Failed to update runtime ${googleProject.value}/${runtimeName.asString}")(resp)
              .flatMap(IO.raiseError)
          } else
            IO.unit
        }
    } yield r

  // This line causes the body to be decoded as JSON, which will prevent error messagges from being seen
  // If you care about the error message, place the function before this line
  import org.http4s.circe.CirceEntityDecoder._

  def waitUntilRunning(googleProject: GoogleProject, runtimeName: RuntimeName, shouldError: Boolean = true)(implicit
    client: Client[IO],
    authorization: IO[Authorization]
  ): IO[GetRuntimeResponseCopy] = {
    val ioa = getRuntime(googleProject, runtimeName)
    for {
      res <- IO.sleep(80 seconds) >> streamFUntilDone(ioa, 100, 10 seconds).compile.lastOrError
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

  def waitUntilAppRunning(googleProject: GoogleProject, appName: AppName, shouldError: Boolean = true)(implicit
    client: Client[IO],
    authorization: IO[Authorization],
    logger: StructuredLogger[IO]
  ): IO[GetAppResponse] = {
    val ioa = getApp(googleProject, appName)
    implicit val doneCheckeable: DoneCheckable[GetAppResponse] = x =>
      x.status == AppStatus.Running || x.status == AppStatus.Error
    for {
      res <- IO.sleep(80 seconds) >> streamUntilDoneOrTimeout(
        ioa,
        120,
        15 seconds,
        s"app ${googleProject.value}/${appName.value} did not finish app creation after 30 minutes."
      )
      _ <- res.status match {
        case AppStatus.Error =>
          if (shouldError)
            IO.raiseError(
              new RuntimeException(s"${googleProject.value}/${appName.value} errored due to ${res.errors}")
            )
          else logger.info(s"${googleProject.value}/${appName.value} errored due to ${res.errors}")
        case _ => IO.unit
      }
    } yield res
  }

  def getRuntime(
    googleProject: GoogleProject,
    runtimeName: RuntimeName
  )(implicit client: Client[IO], authorization: IO[Authorization]): IO[GetRuntimeResponseCopy] =
    for {
      traceIdHeader <- genTraceIdHeader()
      authHeader <- authorization
      r <- client.expectOr[GetRuntimeResponseCopy](
        Request[IO](
          method = Method.GET,
          headers = Headers(authHeader, traceIdHeader),
          uri = rootUri.withPath(
            Uri.Path.unsafeFromString(s"/api/google/v1/runtimes/${googleProject.value}/${runtimeName.asString}")
          )
        )
      )(onError(s"Failed to get runtime ${googleProject.value}/${runtimeName.asString}"))
    } yield r

  def deleteRuntime(googleProject: GoogleProject, runtimeName: RuntimeName, deleteDisk: Boolean = true)(implicit
    client: Client[IO],
    authorization: IO[Authorization]
  ): IO[Unit] =
    for {
      traceIdHeader <- genTraceIdHeader()
      authHeader <- authorization
      r <- client
        .run(
          Request[IO](
            method = Method.DELETE,
            headers = Headers(authHeader, traceIdHeader),
            uri = rootUri
              .withPath(
                Uri.Path.unsafeFromString(s"/api/google/v1/runtimes/${googleProject.value}/${runtimeName.asString}")
              )
              .withQueryParam("deleteDisk", deleteDisk)
          )
        )
        .use { resp =>
          if (!resp.status.isSuccess) {
            onError(s"Failed to delete runtime ${googleProject.value}/${runtimeName.asString}")(resp)
              .flatMap(IO.raiseError)
          } else
            IO.unit
        }
    } yield r

  def deleteRuntimeWithWait(googleProject: GoogleProject, runtimeName: RuntimeName, deleteDisk: Boolean = true)(implicit
    client: Client[IO],
    authorization: IO[Authorization]
  ): IO[Unit] =
    for {
      _ <- deleteRuntime(googleProject, runtimeName, deleteDisk)
      ioa = getRuntime(googleProject, runtimeName).attempt
      res <- IO.sleep(20 seconds) >> streamFUntilDone(ioa, 50, 5 seconds).compile.lastOrError
      _ <-
        if (res.isDone) IO.unit
        else IO.raiseError(new TimeoutException(s"delete runtime ${googleProject.value}/${runtimeName.asString}"))
    } yield ()

  def createDisk(
    googleProject: GoogleProject,
    diskName: DiskName,
    createDiskRequest: CreateDiskRequest = defaultCreateDiskRequest
  )(implicit client: Client[IO], authorization: IO[Authorization]): IO[Unit] =
    for {
      traceIdHeader <- genTraceIdHeader()
      authHeader <- authorization
      r <- client
        .run(
          Request[IO](
            method = Method.POST,
            headers = Headers(authHeader, defaultMediaType, traceIdHeader),
            uri = rootUri
              .withPath(Uri.Path.unsafeFromString(s"/api/google/v1/disks/${googleProject.value}/${diskName.value}")),
            entity = createDiskRequest
          )
        )
        .use { resp =>
          if (!resp.status.isSuccess) {
            onError(s"Failed to create disk ${googleProject.value}/${diskName.value}")(resp)
              .flatMap(IO.raiseError)
          } else
            IO.unit
        }
    } yield r

  def patchDisk(
    googleProject: GoogleProject,
    diskName: DiskName,
    req: UpdateDiskRequest
  )(implicit client: Client[IO], authorization: IO[Authorization]): IO[Unit] =
    for {
      traceIdHeader <- genTraceIdHeader()
      authHeader <- authorization
      r <- client
        .run(
          Request[IO](
            method = Method.PATCH,
            headers = Headers(authHeader, defaultMediaType, traceIdHeader),
            uri = rootUri
              .withPath(Uri.Path.unsafeFromString(s"/api/google/v1/disks/${googleProject.value}/${diskName.value}")),
            entity = req
          )
        )
        .use { resp =>
          if (!resp.status.isSuccess) {
            onError(s"Failed to patch disk ${googleProject.value}/${diskName.value}")(resp)
              .flatMap(IO.raiseError)
          } else
            IO.unit
        }
    } yield r

  def createDiskWithWait(googleProject: GoogleProject, diskName: DiskName, createDiskRequest: CreateDiskRequest)(
    implicit
    client: Client[IO],
    authorization: IO[Authorization]
  ): IO[Unit] =
    for {
      _ <- createDisk(googleProject, diskName, createDiskRequest)
      ioa = getDisk(googleProject, diskName)
      _ <- streamFUntilDone(ioa, 5, 5 seconds).compile.lastOrError
    } yield ()

  def getDisk(
    googleProject: GoogleProject,
    diskName: DiskName
  )(implicit client: Client[IO], authorization: IO[Authorization]): IO[GetPersistentDiskResponse] =
    for {
      traceIdHeader <- genTraceIdHeader()
      authHeader <- authorization
      r <- client
        .run(
          Request[IO](
            method = Method.GET,
            headers = Headers(authHeader, traceIdHeader),
            uri = rootUri
              .withPath(Uri.Path.unsafeFromString(s"/api/google/v1/disks/${googleProject.value}/${diskName.value}"))
          )
        )
        .use { resp =>
          if (resp.status.isSuccess)
            resp.as[GetPersistentDiskResponse]
          else {
            for {
              body <- resp.bodyText.compile.foldMonoid
              rr <- IO.raiseError[GetPersistentDiskResponse](RestError("getDisk failed", resp.status, Some(body)))
            } yield rr
          }
        }
    } yield r

  def listDisk(
    googleProject: GoogleProject,
    includeDeleted: Boolean = false
  )(implicit client: Client[IO], authorization: IO[Authorization]): IO[List[ListPersistentDiskResponse]] = {
    val uriWithoutQueryParam = rootUri
      .withPath(Uri.Path.unsafeFromString(s"/api/google/v1/disks/${googleProject.value}"))

    val uri =
      if (includeDeleted) uriWithoutQueryParam.withQueryParam("includeDeleted", "true")
      else uriWithoutQueryParam

    for {
      traceIdHeader <- genTraceIdHeader()
      authHeader <- authorization
      r <- client.expectOr[List[ListPersistentDiskResponse]](
        Request[IO](
          method = Method.GET,
          headers = Headers(authHeader, traceIdHeader),
          uri = uri
        )
      )(onError(s"Failed to list disks in project ${googleProject.value}"))
    } yield r
  }

  def deleteDisk(googleProject: GoogleProject, diskName: DiskName)(implicit
    client: Client[IO],
    authorization: IO[Authorization]
  ): IO[Unit] =
    for {
      traceIdHeader <- genTraceIdHeader()
      authHeader <- authorization
      r <- client
        .run(
          Request[IO](
            method = Method.DELETE,
            headers = Headers(authHeader, traceIdHeader),
            uri = rootUri
              .withPath(Uri.Path.unsafeFromString(s"/api/google/v1/disks/${googleProject.value}/${diskName.value}"))
          )
        )
        .use { resp =>
          if (!resp.status.isSuccess) {
            onError(s"Failed to delete disk ${googleProject.value}/${diskName.value}")(resp)
              .flatMap(IO.raiseError)
          } else
            IO.unit
        }
    } yield r

  def deleteDiskWithWait(googleProject: GoogleProject, diskName: DiskName)(implicit
    client: Client[IO],
    authorization: IO[Authorization]
  ): IO[Unit] =
    for {
      _ <- deleteDisk(googleProject, diskName)
      ioa = getDisk(googleProject, diskName).attempt
      res <- IO.sleep(3 seconds) >> streamFUntilDone(ioa, 5, 5 seconds).compile.lastOrError
      _ <-
        if (res.isDone) IO.unit
        else IO.raiseError(new TimeoutException(s"delete disk ${googleProject.value}/${diskName.value}"))
    } yield ()

  def onError(message: String)(response: Response[IO]): IO[Throwable] =
    for {
      body <- response.bodyText.compile.foldMonoid
    } yield RestError(message, response.status, Some(body))

  def createApp(
    googleProject: GoogleProject,
    appName: AppName,
    createAppRequest: CreateAppRequest = defaultCreateAppRequest
  )(implicit client: Client[IO], authorization: IO[Authorization]): IO[Unit] =
    for {
      traceIdHeader <- genTraceIdHeader()
      authHeader <- authorization
      r <- client
        .run(
          Request[IO](
            method = Method.POST,
            headers = Headers(authHeader, defaultMediaType, traceIdHeader),
            uri = rootUri
              .withPath(Uri.Path.unsafeFromString(s"/api/google/v1/apps/${googleProject.value}/${appName.value}")),
            entity = createAppRequest
          )
        )
        .use { resp =>
          if (resp.status.isSuccess)
            IO.unit
          else
            onError(s"Failed to create app ${googleProject.value}/${appName.value}")(resp).flatMap(IO.raiseError)
        }
    } yield r

  def createAppWithWait(
    googleProject: GoogleProject,
    appName: AppName,
    createAppRequest: CreateAppRequest = defaultCreateAppRequest
  )(implicit client: Client[IO], authorization: IO[Authorization], logger: StructuredLogger[IO]): IO[Unit] =
    for {
      _ <- createApp(googleProject, appName, createAppRequest)
      _ <- waitUntilAppRunning(googleProject, appName, true)
    } yield ()

  def deleteApp(googleProject: GoogleProject, appName: AppName, deleteDisk: Boolean = true)(implicit
    client: Client[IO],
    authorization: IO[Authorization]
  ): IO[Unit] =
    for {
      traceIdHeader <- genTraceIdHeader()
      authHeader <- authorization
      r <- client
        .run(
          Request[IO](
            method = Method.DELETE,
            headers = Headers(authHeader, traceIdHeader),
            uri = rootUri
              .withPath(Uri.Path.unsafeFromString(s"/api/google/v1/apps/${googleProject.value}/${appName.value}"))
              .withQueryParam("deleteDisk", deleteDisk)
          )
        )
        .use { resp =>
          if (!resp.status.isSuccess) {
            onError(s"Failed to delete app ${googleProject.value}/${appName.value}")(resp)
              .flatMap(IO.raiseError)
          } else
            IO.unit
        }
    } yield r

  def deleteAppWithWait(googleProject: GoogleProject, appName: AppName, deleteDisk: Boolean = true)(implicit
    client: Client[IO],
    authorization: IO[Authorization]
  ): IO[Unit] =
    for {
      _ <- deleteApp(googleProject, appName, deleteDisk)
      ioa = getApp(googleProject, appName).attempt
      res <- IO.sleep(120 seconds) >> streamFUntilDone(ioa, 30, 30 seconds).compile.lastOrError
      _ <-
        if (res.isDone) IO.unit
        else IO.raiseError(new TimeoutException(s"delete app ${googleProject.value}/${appName.value}"))
    } yield ()

  def getApp(googleProject: GoogleProject, appName: AppName)(implicit
    client: Client[IO],
    authorization: IO[Authorization]
  ): IO[GetAppResponse] =
    for {
      traceIdHeader <- genTraceIdHeader()
      authHeader <- authorization
      r <- client.expectOr[GetAppResponse](
        Request[IO](
          method = Method.GET,
          headers = Headers(authHeader, traceIdHeader),
          uri = rootUri
            .withPath(Uri.Path.unsafeFromString(s"/api/google/v1/apps/${googleProject.value}/${appName.value}"))
        )
      )(onError(s"Failed to get app ${googleProject.value}/${appName.value}"))
    } yield r

  def listApps(
    googleProject: GoogleProject,
    includeDeleted: Boolean = false
  )(implicit client: Client[IO], authorization: IO[Authorization]): IO[List[ListAppResponse]] = {
    val uriWithoutQueryParam = rootUri
      .withPath(Uri.Path.unsafeFromString(s"/api/google/v1/apps/${googleProject.value}"))

    val uri =
      if (includeDeleted) uriWithoutQueryParam.withQueryParam("includeDeleted", "true")
      else uriWithoutQueryParam

    for {
      traceIdHeader <- genTraceIdHeader()
      authHeader <- authorization
      r <- client.expectOr[List[ListAppResponse]](
        Request[IO](
          method = Method.GET,
          headers = Headers(authHeader, traceIdHeader),
          uri = uri
        )
      )(onError(s"Failed to list apps in project ${googleProject.value}"))
    } yield r
  }

  def stopApp(googleProject: GoogleProject, appName: AppName)(implicit
    client: Client[IO],
    authorization: IO[Authorization]
  ): IO[Unit] =
    for {
      traceIdHeader <- genTraceIdHeader()
      authHeader <- authorization
      r <- client
        .run(
          Request[IO](
            method = Method.POST,
            headers = Headers(authHeader, traceIdHeader),
            uri = rootUri
              .withPath(Uri.Path.unsafeFromString(s"/api/google/v1/apps/${googleProject.value}/${appName.value}/stop"))
          )
        )
        .use { resp =>
          if (!resp.status.isSuccess) {
            onError(s"Failed to stop app ${googleProject.value}/${appName.value}")(resp)
              .flatMap(IO.raiseError)
          } else
            IO.unit
        }
    } yield r

  def startApp(googleProject: GoogleProject, appName: AppName)(implicit
    client: Client[IO],
    authorization: IO[Authorization]
  ): IO[Unit] =
    for {
      traceIdHeader <- genTraceIdHeader()
      authHeader <- authorization
      r <- client
        .run(
          Request[IO](
            method = Method.POST,
            headers = Headers(authHeader, traceIdHeader),
            uri = rootUri
              .withPath(Uri.Path.unsafeFromString(s"/api/google/v1/apps/${googleProject.value}/${appName.value}/start"))
          )
        )
        .use { resp =>
          if (!resp.status.isSuccess) {
            onError(s"Failed to start app ${googleProject.value}/${appName.value}")(resp)
              .flatMap(IO.raiseError)
          } else
            IO.unit
        }
    } yield r

  def createAzureRuntime(
    workspaceId: WorkspaceId,
    runtimeName: RuntimeName,
    useExistingDisk: Boolean,
    createAzureRuntimeRequest: CreateAzureRuntimeRequest = defaultCreateAzureRuntimeRequest
  )(implicit client: Client[IO], authorization: IO[Authorization]): IO[Unit] =
    for {
      traceIdHeader <- genTraceIdHeader()
      authHeader <- authorization
      r <- client
        .run(
          Request[IO](
            method = Method.POST,
            headers = Headers(authHeader, defaultMediaType, traceIdHeader),
            uri = rootUri
              .withPath(
                Uri.Path
                  .unsafeFromString(s"/api/v2/runtimes/${workspaceId.value.toString}/azure/${runtimeName.asString}")
              )
              .withQueryParam("useExistingDisk", useExistingDisk),
            entity = createAzureRuntimeRequest
          )
        )
        .use { resp =>
          if (!resp.status.isSuccess) {
            onError(s"Failed to create runtime ${workspaceId.value.toString}/${runtimeName.asString}")(resp)
              .flatMap(IO.raiseError)
          } else
            IO.unit
        }
    } yield ()

  def getAzureRuntime(
    workspaceId: WorkspaceId,
    runtimeName: RuntimeName
  )(implicit client: Client[IO], authorization: IO[Authorization]): IO[GetRuntimeResponseCopy] =
    for {
      traceIdHeader <- genTraceIdHeader()
      authHeader <- authorization
      r <- client.expectOr[GetRuntimeResponseCopy](
        Request[IO](
          method = Method.GET,
          headers = Headers(authHeader, traceIdHeader),
          uri = rootUri.withPath(
            Uri.Path.unsafeFromString(s"/api/v2/runtimes/${workspaceId.value.toString}/azure/${runtimeName.asString}")
          )
        )
      )(onError(s"Failed to get runtime ${workspaceId.value.toString}/${runtimeName.asString}"))
    } yield r

  def deleteRuntimeV2(
    workspaceId: WorkspaceId,
    runtimeName: RuntimeName,
    deleteDisk: Boolean = true
  )(implicit client: Client[IO], authorization: IO[Authorization]): IO[Unit] =
    for {
      traceIdHeader <- genTraceIdHeader()
      authHeader <- authorization
      r <- client
        .run(
          Request[IO](
            method = Method.DELETE,
            headers = Headers(authHeader, traceIdHeader),
            uri = rootUri
              .withPath(
                Uri.Path
                  .unsafeFromString(s"/api/v2/runtimes/${workspaceId.value.toString}/azure/${runtimeName.asString}")
              )
              .withQueryParam("deleteDisk", deleteDisk)
          )
        )
        .use { resp =>
          if (!resp.status.isSuccess) {
            onError(s"Failed to delete runtime ${workspaceId.value.toString}/${runtimeName.asString}")(resp)
              .flatMap(IO.raiseError)
          } else
            IO.unit
        }
    } yield r

  // TODO: delete this
  def deleteRuntimeV2WithWait(
    workspaceId: WorkspaceId,
    runtimeName: RuntimeName,
    deleteDisk: Boolean = true
  )(implicit client: Client[IO], authorization: IO[Authorization]): IO[Unit] =
    for {
      _ <- deleteRuntimeV2(workspaceId, runtimeName, deleteDisk)
      ioa = getAzureRuntime(workspaceId, runtimeName).attempt
      res <- IO.sleep(20 seconds) >> streamFUntilDone(ioa, 50, 5 seconds).compile.lastOrError
      _ <-
        if (res.isDone) IO.unit
        else IO.raiseError(new TimeoutException(s"delete runtime ${workspaceId}/${runtimeName.asString}"))
    } yield ()

  def testSparkWebUi(
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    path: String
  )(implicit client: Client[IO], authorization: IO[Authorization]): IO[Unit] =
    for {
      traceIdHeader <- genTraceIdHeader()
      refererHeader <- ProxyRedirectClient.genRefererHeader()
      authHeader <- authorization
      _ <- client
        .run(
          Request[IO](
            method = Method.GET,
            headers = Headers(authHeader, traceIdHeader, refererHeader),
            uri = rootUri
              .withPath(Uri.Path.unsafeFromString(s"/proxy/${googleProject.value}/${runtimeName.asString}/${path}"))
          )
        )
        .use { resp =>
          if (!resp.status.isSuccess) {
            onError(s"Failed to load Spark Web UI: $path")(resp)
              .flatMap(IO.raiseError)
          } else
            IO.unit
        }
    } yield ()

  private def genTraceIdHeader(): IO[Header.Raw] = {
    val uuid = UUID.randomUUID().toString.replaceAll("\\-", "")
    val digitString = "3939911508519804487"
    IO(uuid + "/" + digitString).map(uuid => Header.Raw(traceIdHeaderString, uuid))
  }

}

final case class RestError(message: String, statusCode: Status, body: Option[String]) extends NoStackTrace {
  override def getMessage: String = s"message: ${message}, status: ${statusCode} body: ${body.getOrElse("")}"
}
