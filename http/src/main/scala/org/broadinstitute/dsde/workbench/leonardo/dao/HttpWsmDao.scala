package org.broadinstitute.dsde.workbench.leonardo
package dao

import _root_.io.circe.syntax._
import cats.effect.Async
import cats.implicits._
import cats.mtl.Ask
import fs2.Stream
import org.broadinstitute.dsde.workbench.leonardo.config.HttpWsmDaoConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.WsmDecoders._
import org.broadinstitute.dsde.workbench.leonardo.dao.WsmEncoders._
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.http4s._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.{`Content-Type`, Authorization}
import org.typelevel.log4cats.StructuredLogger

class HttpWsmDao[F[_]](httpClient: Client[F], config: HttpWsmDaoConfig)(
  implicit logger: StructuredLogger[F],
  F: Async[F],
  metrics: OpenTelemetryMetrics[F]
) extends WsmDao[F]
    with Http4sClientDsl[F] {

  implicit def http4sBody[A](body: A)(implicit encoder: EntityEncoder[F, A]): EntityBody[F] =
    encoder.toEntity(body).body

  val defaultMediaType = `Content-Type`(MediaType.application.json)

  override def createIp(request: CreateIpRequest,
                        authorization: Authorization)(implicit ev: Ask[F, AppContext]): F[CreateIpResponse] =
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
        body = request,
        headers = Headers(authorization, defaultMediaType)
      )
    )(onError)

  override def createDisk(request: CreateDiskRequest,
                          authorization: Authorization)(implicit ev: Ask[F, AppContext]): F[CreateDiskResponse] =
    httpClient.expectOr[CreateDiskResponse](
      Request[F](
        method = Method.POST,
        uri = config.uri
          .withPath(
            Uri.Path
              .unsafeFromString(
                s"/api/workspaces/v1/${request.workspaceId.value.toString}/resources/controlled/azure/disks"
              )
          ),
        body = request,
        headers = Headers(authorization, defaultMediaType)
      )
    )(onError)

  override def createNetwork(request: CreateNetworkRequest,
                             authorization: Authorization)(implicit ev: Ask[F, AppContext]): F[CreateNetworkResponse] =
    httpClient.expectOr[CreateNetworkResponse](
      Request[F](
        method = Method.POST,
        uri = config.uri
          .withPath(
            Uri.Path
              .unsafeFromString(
                s"/api/workspaces/v1/${request.workspaceId.value.toString}/resources/controlled/azure/network"
              )
          ),
        body = request,
        headers = Headers(authorization, defaultMediaType)
      )
    )(onError)

  override def createVm(request: CreateVmRequest,
                        authorization: Authorization)(implicit ev: Ask[F, AppContext]): F[CreateVmResult] =
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
        body = request,
        headers = Headers(authorization, defaultMediaType)
      )
    )(onError)

  override def getWorkspace(workspaceId: WorkspaceId,
                            authorization: Authorization)(implicit ev: Ask[F, AppContext]): F[WorkspaceDescription] =
    httpClient.expectOr[WorkspaceDescription](
      Request[F](
        method = Method.GET,
        uri = config.uri
          .withPath(
            Uri.Path
              .unsafeFromString(s"/api/workspaces/v1/${workspaceId.value.toString}")
          ),
        headers = Headers(authorization)
      )
    )(onError)

  override def deleteVm(request: DeleteVmRequest,
                        authorization: Authorization)(implicit ev: Ask[F, AppContext]): F[DeleteVmResult] =
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
        headers = Headers(authorization, defaultMediaType)
      )
    )(onError)

  override def getCreateVmJobResult(request: GetJobResultRequest, authorization: Authorization)(
    implicit ev: Ask[F, AppContext]
  ): F[GetCreateVmJobResult] =
    httpClient.expectOr[GetCreateVmJobResult](
      Request[F](
        method = Method.GET,
        uri = config.uri
          .withPath(
            Uri.Path
              .unsafeFromString(
                s"/api/workspaces/v1/${request.workspaceId.value.toString}/resources/controlled/azure/vm/create-result/${request.jobId.value}"
              )
          ),
        headers = Headers(authorization)
      )
    )(onError)

  private def onError(response: Response[F])(implicit ev: Ask[F, AppContext]): F[Throwable] =
    for {
      context <- ev.ask
      body <- response.bodyText.compile.foldMonoid
      _ <- logger.error(context.loggingCtx)(s"WSM call failed: $body")
      _ <- metrics.incrementCounter("wsm/errorResponse")
    } yield WsmException(context.traceId, body)
}
