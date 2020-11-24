package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.model.TraceId

trait AppDescriptorDAO[F[_]] {
  def getDescriptor(path: String)(implicit ev: Ask[F, TraceId]): F[AppDescriptor]
}
