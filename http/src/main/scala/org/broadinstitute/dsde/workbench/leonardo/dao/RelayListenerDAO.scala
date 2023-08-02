package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.http4s.Uri

trait RelayListenerDAO[F[_]] {
  def getStatus(baseUri: Uri)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean]
}
