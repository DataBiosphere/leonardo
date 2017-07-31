package org.broadinstitute.dsde.workbench.leonardo

import net.ceedubs.ficus.readers.ValueReader
import org.broadinstitute.dsde.workbench.model._
import net.ceedubs.ficus.Ficus._

package object config {
  implicit val swaggerReader: ValueReader[SwaggerConfig] = ValueReader.relative { config =>
    SwaggerConfig()
  }
}
