package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.{Method, Request, Uri}

class HttpListenerDAO[F[_]](httpClient: Client[F])(implicit
  F: Async[F],
  metrics: OpenTelemetryMetrics[F]
) extends ListenerDAO[F]
    with Http4sClientDsl[F] {
  override def getStatus(baseUri: Uri)(implicit ev: Ask[F, AppContext]): F[Boolean] =
    for {
      _ <- metrics.incrementCounter("listener/status")
      listenerStatusUri = baseUri / "listenerstatus"
      res <- httpClient.status(
        Request[F](
          method = Method.GET,
          uri = listenerStatusUri
        )
      )
    } yield res.isSuccess
}
