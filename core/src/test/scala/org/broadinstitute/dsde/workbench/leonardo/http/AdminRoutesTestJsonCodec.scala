package org.broadinstitute.dsde.workbench.leonardo.http

import io.circe.Encoder

object AdminRoutesTestJsonCodec {

  implicit val getAppUpdateRequestEncoder: Encoder[UpdateAppsRequest] = Encoder.forProduct1(
    "dryRun"
  )(x =>
    (
      x.dryRun
    )
  )
}
