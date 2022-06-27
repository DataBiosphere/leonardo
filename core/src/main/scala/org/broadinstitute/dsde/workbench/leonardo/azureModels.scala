package org.broadinstitute.dsde.workbench.leonardo

import java.util.UUID

final case class WsmControlledResourceId(value: UUID) extends AnyVal

final case class AzureUnimplementedException(message: String) extends Exception {
  override def getMessage: String = message
}

final case class WsmJobId(value: String) extends AnyVal
