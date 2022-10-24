package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, CloudContext, RuntimeName}

trait CromwellDao[F[_]] {
  def getStatus(cloudContext: CloudContext, runtimeName: RuntimeName)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean]
}
