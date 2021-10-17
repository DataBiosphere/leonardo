package org.broadinstitute.dsde.workbench.leonardo.http

import java.net.URL

import io.circe.{Decoder, Encoder, KeyDecoder}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.NumNodepools
import org.http4s.Uri

object AppRoutesTestJsonCodec {
  implicit val descriptorEncoder: Encoder[Uri] = Encoder.encodeString.contramap(_.toString)

  implicit val createAppEncoder: Encoder[CreateAppRequest] = Encoder.forProduct7(
    "kubernetesRuntimeConfig",
    "appType",
    "diskConfig",
    "labels",
    "customEnvironmentVariables",
    "descriptorPath",
    "extraArgs"
  )(x =>
    (
      x.kubernetesRuntimeConfig,
      x.appType,
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
    Decoder.forProduct7("kubernetesRuntimeConfig",
                        "errors",
                        "status",
                        "proxyUrls",
                        "diskName",
                        "customEnvironmentVariables",
                        "auditInfo")(GetAppResponse.apply)

  implicit val listAppResponseDecoder: Decoder[ListAppResponse] =
    Decoder.forProduct9("googleProject",
                        "kubernetesRuntimeConfig",
                        "errors",
                        "status",
                        "proxyUrls",
                        "appName",
                        "diskName",
                        "auditInfo",
                        "includeLabels")(ListAppResponse.apply)
}
