package org.broadinstitute.dsde.workbench.leonardo.http

import io.circe.Encoder
import io.circe.Encoder.AsArray.importedAsArrayEncoder
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsp.ChartVersion

object AdminRoutesTestJsonCodec {

  implicit val chartVersionEncoder: Encoder[ChartVersion] = Encoder.encodeString.contramap(_.asString)

  implicit val getAppUpdateRequestEncoder: Encoder[UpdateAppsRequest] = Encoder.forProduct8(
    "appType",
    "cloudProvider",
    "appVersionsInclude",
    "appVersionsExclude",
    "googleProject",
    "workspaceId",
    "appNames",
    "dryRun"
  )(x =>
    (
      x.appType,
      x.cloudProvider,
      x.appVersionsInclude,
      x.appVersionsExclude,
      x.googleProject,
      x.workspaceId,
      x.appNames,
      x.dryRun
    )
  )
}
