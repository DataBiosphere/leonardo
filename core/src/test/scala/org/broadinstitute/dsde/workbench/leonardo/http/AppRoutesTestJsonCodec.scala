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
    Decoder.forProduct12(
      "appName",
      "cloudContext",
      "kubernetesRuntimeConfig",
      "errors",
      "status",
      "proxyUrls",
      "diskName",
      "customEnvironmentVariables",
      "auditInfo",
      "appType",
//      "chartName",
      "accessScope",
      "labels"
    )(
      (appName,
       cloudContext,
       kubernetesRuntimeConfig,
       errors,
       status,
       proxyUrls,
       diskName,
       customEnvironmentVariables,
       auditInfo,
       //       chartName, TODO: revert this once CBAS are upgraded
       appType,
       accessScope,
       labels
      ) =>
        GetAppResponse(
          appName,
          cloudContext,
          kubernetesRuntimeConfig,
          errors,
          status,
          proxyUrls,
          diskName,
          customEnvironmentVariables,
          auditInfo,
          //       chartName, TODO: revert this once CBAS are upgraded
          appType,
          org.broadinstitute.dsp.ChartName("dummy"),
          accessScope,
          labels
        )
    )

  implicit val listAppResponseDecoder: Decoder[ListAppResponse] =
    Decoder.forProduct12(
      "workspaceId",
      "cloudContext",
      "kubernetesRuntimeConfig",
      "errors",
      "status",
      "proxyUrls",
      "appName",
      "appType",
//      "chartName",
      "diskName",
      "auditInfo",
      "accessScope",
      "labels"
    )(
      (workspaceId,
       cloudContext,
       kubernetesRuntimeConfig,
       errors,
       status,
       proxyUrls,
       appName,
       appType,
       //       chartName, TODO: revert this once CBAS are upgraded
       diskName,
       auditInfo,
       accessScope,
       labels
      ) =>
        ListAppResponse(
          workspaceId,
          cloudContext,
          kubernetesRuntimeConfig,
          errors,
          status,
          proxyUrls,
          appName,
          appType,
          org.broadinstitute.dsp.ChartName("dummy"),
          diskName,
          auditInfo,
          accessScope,
          labels
        )
    )
}
