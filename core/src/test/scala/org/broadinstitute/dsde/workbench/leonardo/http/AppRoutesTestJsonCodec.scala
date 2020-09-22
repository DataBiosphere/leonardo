package org.broadinstitute.dsde.workbench.leonardo.http

import java.net.URL

import io.circe.{Decoder, Encoder, KeyDecoder}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._

object AppRoutesTestJsonCodec {

  implicit val createAppEncoder: Encoder[CreateAppRequest] = Encoder.forProduct5(
    "kubernetesRuntimeConfig",
    "appType",
    "diskConfig",
    "labels",
    "customEnvironmentVariables"
  )(x =>
    (
      x.kubernetesRuntimeConfig,
      x.appType,
      x.diskConfig,
      x.labels,
      x.customEnvironmentVariables
    )
  )

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
    Decoder.forProduct8("googleProject",
                        "kubernetesRuntimeConfig",
                        "errors",
                        "status",
                        "proxyUrls",
                        "appName",
                        "diskName",
                        "auditInfo")(ListAppResponse.apply)
}
