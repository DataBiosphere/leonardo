package org.broadinstitute.dsde.workbench.leonardo
package http

import io.circe.Encoder
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._

object DiskRoutesTestJsonCodec {
  implicit val createDiskRequestEncoder: Encoder[CreateDiskRequest] = Encoder.forProduct4(
    "labels",
    "size",
    "diskType",
    "blockSize"
  )(x =>
    (
      x.labels,
      x.size,
      x.diskType,
      x.blockSize
    )
  )
}
