package org.broadinstitute.dsde.workbench.leonardo
package dao

import org.typelevel.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import cats.effect.Async
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.config.HttpWsmDaoConfig
import org.http4s.{AuthScheme, Credentials, Headers, Method, Request, Response, Uri}
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import cats.implicits._
import org.http4s.circe.CirceEntityDecoder._
import _root_.io.circe.syntax._
import WsmDecoders._
import WsmEncoders._
import fs2.Stream
import org.http4s.headers.Authorization

class HttpWsmDao[F[_]](httpClient: Client[F], config: HttpWsmDaoConfig)(implicit logger: StructuredLogger[F],
                                                                        F: Async[F],
                                                                        metrics: OpenTelemetryMetrics[F])
    extends WsmDao[F]
    with Http4sClientDsl[F] {

  override def createIp(request: CreateIpRequest)(implicit ev: Ask[F, AppContext]): F[CreateIpResponse] =
    httpClient.expectOr[CreateIpResponse](
      Request[F](
        method = Method.POST,
        uri = config.uri
          .withPath(
            Uri.Path
              .unsafeFromString(
                s"/api/workspaces/v1/${request.workspaceId.value.toString}/resources/controlled/azure/ip"
              )
          ),
        body = Stream.emits(request.asJson.noSpaces.getBytes),
        headers = Headers(fakeAuth()) //TODO
      )
    )(onError)

  override def createDisk(request: CreateDiskRequest)(implicit ev: Ask[F, AppContext]): F[CreateDiskResponse] =
    httpClient.expectOr[CreateDiskResponse](
      Request[F](
        method = Method.POST,
        uri = config.uri
          .withPath(
            Uri.Path
              .unsafeFromString(
                s"/api/workspaces/v1/${request.workspaceId.value.toString}/resources/controlled/azure/disk"
              )
          ),
        body = Stream.emits(request.asJson.noSpaces.getBytes),
        headers = Headers(fakeAuth())
      )
    )(onError)

  override def createNetwork(request: CreateNetworkRequest)(implicit ev: Ask[F, AppContext]): F[CreateNetworkResponse] =
    httpClient.expectOr[CreateNetworkResponse](
      Request[F](
        method = Method.POST,
        uri = config.uri
          .withPath(
            Uri.Path
              .unsafeFromString(
                s"/api/workspaces/v1/${request.workspaceId.value.toString}/resources/controlled/azure/disk"
              )
          ),
        body = Stream.emits(request.asJson.noSpaces.getBytes),
        headers = Headers(fakeAuth())
      )
    )(onError)

  override def createVm(request: CreateVmRequest)(implicit ev: Ask[F, AppContext]): F[CreateVmResult] =
    httpClient.expectOr[CreateVmResult](
      Request[F](
        method = Method.POST,
        uri = config.uri
          .withPath(
            Uri.Path
              .unsafeFromString(
                s"/api/workspaces/v1/${request.workspaceId.value.toString}/resources/controlled/azure/vm"
              )
          ),
        body = Stream.emits(request.asJson.noSpaces.getBytes),
        headers = Headers(fakeAuth())
      )
    )(onError)

  override def getWorkspace(workspaceId: WorkspaceId)(implicit ev: Ask[F, AppContext]): F[WorkspaceDescription] =
    httpClient.expectOr[WorkspaceDescription](
      Request[F](
        method = Method.GET,
        uri = config.uri
          .withPath(
            Uri.Path
              .unsafeFromString(s"/api/workspaces/v1/${workspaceId.value.toString}")
          ),
        headers = Headers(fakeAuth())
      )
    )(onError)

  override def deleteVm(request: DeleteVmRequest)(implicit ev: Ask[F, AppContext]): F[DeleteVmResult] =
    httpClient.expectOr[DeleteVmResult](
      Request[F](
        method = Method.POST,
        uri = config.uri
          .withPath(
            Uri.Path
              .unsafeFromString(
                s"/api/workspaces/v1/${request.workspaceId.value.toString}/resources/controlled/azure/vm/${request.resourceId.value.toString}"
              )
          ),
        body = Stream.emits(request.deleteRequest.asJson.noSpaces.getBytes),
        headers = Headers(fakeAuth())
      )
    )(onError)

  override def getCreateVmJobResult(request: GetJobResultRequest)(implicit ev: Ask[F, AppContext]): F[CreateVmResult] =
    httpClient.expectOr[CreateVmResult](
      Request[F](
        method = Method.GET,
        uri = config.uri
          .withPath(
            Uri.Path
              .unsafeFromString(
                s"/api/workspaces/v1/${request.workspaceId.value.toString}/resources/controlled/azure/vm/create-result/${request.jobId.value}"
              )
          ),
        headers = Headers(fakeAuth())
      )
    )(onError)

  private def onError(response: Response[F])(implicit ev: Ask[F, AppContext]): F[Throwable] =
    for {
      context <- ev.ask
      body <- response.bodyText.compile.foldMonoid
      _ <- logger.error(context.loggingCtx)(s"WSM call failed: $body")
      _ <- metrics.incrementCounter("wsm/errorResponse")
    } yield WsmException(context.traceId, body)

  private def fakeAuth(): Authorization =
    Authorization(Credentials.Token(AuthScheme.Bearer, ""))

}
