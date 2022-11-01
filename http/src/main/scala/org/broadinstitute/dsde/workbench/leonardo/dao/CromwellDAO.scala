package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.http4s.{Headers, Uri}

trait CromwellDAO[F[_]] {
  def getStatus(uri: Uri, headers: Headers)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean]
}
