package org.broadinstitute.dsde.workbench.leonardo.http

import io.circe.Encoder
import io.circe.Encoder.AsArray.importedAsArrayEncoder
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.UpdateAppJobId
import org.broadinstitute.dsp.ChartVersion

object AdminRoutesTestJsonCodec {

  implicit val chartVersionEncoder: Encoder[ChartVersion] = Encoder.encodeString.contramap(_.asString)

  implicit val chartVersionEncoder: Encoder[UpdateAppJobId] = Encoder.encodeUUID.contramap(_.value)

  implicit val getAppUpdateRequestEncoder: Encoder[UpdateAppsRequest] = Encoder.forProduct9(
    "jobId",
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
      x.jobId,
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
