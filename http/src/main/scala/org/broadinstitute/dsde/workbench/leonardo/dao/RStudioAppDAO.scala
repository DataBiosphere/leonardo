package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.http4s.Uri
import org.http4s.headers.Authorization

/**
 * Client for RStudio app.
 */
trait RStudioAppDAO[F[_]] {
  def getStatus(baseUri: Uri, authHeader: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean]
}
