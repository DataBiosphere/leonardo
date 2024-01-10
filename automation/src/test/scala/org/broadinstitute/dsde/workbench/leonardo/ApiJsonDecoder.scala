package org.broadinstitute.dsde.workbench.leonardo

import io.circe.Decoder
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.DiskConfig
import AutomationTestJsonCodec.clusterStatusDecoder

object ApiJsonDecoder {
  implicit val diskConfigDecoder: Decoder[DiskConfig] = Decoder.forProduct4(
    "name",
    "size",
    "diskType",
    "blockSize"
  )(DiskConfig.apply)

  implicit val getRuntimeResponseCopyDecoder: Decoder[GetRuntimeResponseCopy] = Decoder.forProduct16(
    "runtimeName",
    "googleProject",
    "cloudContext",
    "serviceAccount",
    "auditInfo",
    "asyncRuntimeFields",
    "runtimeConfig",
    "proxyUrl",
    "status",
    "labels",
    "jupyterUserScriptUri",
    "jupyterStartUserScriptUri",
    "errors",
    "userJupyterExtensionConfig",
    "autopauseThreshold",
    "diskConfig"
  )(GetRuntimeResponseCopy.apply)
}
