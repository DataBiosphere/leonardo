package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.http4s.{Headers, Uri}

trait CbasDAO[F[_]] {
  def getStatus(baseUri: Uri, headers: Headers)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean]
}
