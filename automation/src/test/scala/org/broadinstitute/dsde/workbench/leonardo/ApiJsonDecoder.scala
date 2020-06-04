package org.broadinstitute.dsde.workbench.leonardo

import io.circe.Decoder
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.DiskConfig
import AutomationTestJsonCodec.clusterStatusDecoder

object ApiJsonDecoder {
  implicit val getDiskResponseDecoder: Decoder[GetPersistentDiskResponse] = Decoder.forProduct12(
    "id",
    "googleProject",
    "zone",
    "name",
    "googleId",
    "serviceAccount",
    "status",
    "auditInfo",
    "size",
    "diskType",
    "blockSize",
    "labels"
  )(GetPersistentDiskResponse.apply)

  implicit val diskConfigDecoder: Decoder[DiskConfig] = Decoder.forProduct4(
    "name",
    "size",
    "diskType",
    "blockSize"
  )(DiskConfig.apply)

  implicit val getRuntimeResponseCopyDecoder: Decoder[GetRuntimeResponseCopy] = Decoder.forProduct15(
    "runtimeName",
    "googleProject",
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
