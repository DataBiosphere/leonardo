package org.broadinstitute.dsde.workbench.leonardo.http

import java.net.URL
import io.circe.{Decoder, Encoder, KeyDecoder}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.NumNodepools
import org.http4s.Uri

object AppRoutesTestJsonCodec {
  implicit val descriptorEncoder: Encoder[Uri] = Encoder.encodeString.contramap(_.toString)

  implicit val createAppEncoder: Encoder[CreateAppRequest] = Encoder.forProduct9(
    "kubernetesRuntimeConfig",
    "appType",
    "allowedChartName",
    "accessScope",
    "diskConfig",
    "labels",
    "customEnvironmentVariables",
    "descriptorPath",
    "extraArgs"
  )(x =>
    (
      x.kubernetesRuntimeConfig,
      x.appType,
      x.allowedChartName,
      x.accessScope,
      x.diskConfig,
      x.labels,
      x.customEnvironmentVariables,
      x.descriptorPath,
      x.extraArgs
    )
  )
  implicit val numNodepoolsEncoder: Encoder[NumNodepools] = Encoder.encodeInt.contramap(_.value)

  implicit val proxyUrlDecoder: Decoder[Map[ServiceName, URL]] =
    Decoder.decodeMap[ServiceName, URL](KeyDecoder.decodeKeyString.map(ServiceName), urlDecoder)

  implicit val getAppResponseDecoder: Decoder[GetAppResponse] =
    Decoder.forProduct17(
      "workspaceId",
      "appName",
      "cloudContext",
      "region",
      "kubernetesRuntimeConfig",
      "errors",
      "status",
      "proxyUrls",
      "diskName",
      "customEnvironmentVariables",
      "auditInfo",
      "appType",
      "chartName",
      "accessScope",
      "labels",
      "autodeleteEnabled",
      "autodeleteThreshold"
    )(GetAppResponse.apply)

  implicit val listAppResponseDecoder: Decoder[ListAppResponse] =
    Decoder.forProduct16(
      "workspaceId",
      "cloudContext",
      "region",
      "kubernetesRuntimeConfig",
      "errors",
      "status",
      "proxyUrls",
      "appName",
      "appType",
      "chartName",
      "diskName",
      "auditInfo",
      "accessScope",
      "labels",
      "autodeleteEnabled",
      "autodeleteThreshold"
    )(ListAppResponse.apply)
}
