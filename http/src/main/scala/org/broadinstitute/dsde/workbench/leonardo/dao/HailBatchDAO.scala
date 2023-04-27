package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.http4s.Uri
import org.http4s.headers.Authorization

/**
 * Client for Hail Batch app.
 * The Hail Batch app exposes 2 services: batch and batch-driver. They are both fronted
 * by a reverse proxy. The batch container serves the UI and user requests; the batch-driver
 * is responsible for provisioning and monitoring compute nodes.
 */
trait HailBatchDAO[F[_]] {

  /** Status of the batch container. */
  def getStatus(baseUri: Uri, authHeader: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean]

  /** Status of the batch-driver container. */
  def getDriverStatus(baseUri: Uri, authHeader: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean]
}
