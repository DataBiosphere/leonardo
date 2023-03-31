package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.http4s._
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.Authorization

class HttpHailBatchDAO[F[_]](httpClient: Client[F])(implicit
  F: Async[F],
  metrics: OpenTelemetryMetrics[F]
) extends HailBatchDAO[F]
    with Http4sClientDsl[F] {

  override def getStatus(baseUri: Uri, authHeader: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] = getStatusInternal(baseUri / "batch" / "batches", authHeader)

  override def getDriverStatus(baseUri: Uri, authHeader: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] =
    getStatusInternal(baseUri / "batch-driver", authHeader)

  private def getStatusInternal(uri: Uri, authHeader: Authorization): F[Boolean] = for {
    _ <- metrics.incrementCounter("hail/status")
    res <- httpClient.status(
      Request[F](
        method = Method.GET,
        uri = uri,
        headers = Headers(authHeader)
      )
    )
  } yield res.isSuccess
}
