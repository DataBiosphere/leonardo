package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure.RelayNamespace
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.http4s.Headers

trait CromwellDAO[F[_]] {
  def getStatus(relayNamespace: RelayNamespace, headers: Headers)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean]
}
