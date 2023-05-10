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

/**
 * HTTP client for RStudio app.
 */
class HttpRStudioAppDAO[F[_]](httpClient: Client[F])(implicit
  F: Async[F],
  metrics: OpenTelemetryMetrics[F]
) extends RStudioAppDAO[F]
    with Http4sClientDsl[F] {

  override def getStatus(baseUri: Uri, authHeader: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] = getStatusInternal(baseUri / "rstudio", authHeader)

  private def getStatusInternal(uri: Uri, authHeader: Authorization): F[Boolean] = for {
    _ <- metrics.incrementCounter("rstudio/status")
    res <- httpClient.status(
      Request[F](
        method = Method.GET,
        uri = uri,
        headers = Headers(authHeader)
      )
    )
  } yield res.isSuccess
}
