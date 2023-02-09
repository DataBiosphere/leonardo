package org.broadinstitute.dsde.workbench.leonardo.http

import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.{
  AsyncRuntimeFields,
  AuditInfo,
  CloudContext,
  CreateAzureDiskRequest,
  CreateAzureRuntimeRequest,
  LabelMap,
  RuntimeConfig,
  RuntimeError,
  RuntimeImage,
  RuntimeName,
  RuntimeStatus,
  UserJupyterExtensionConfig,
  UserScriptPath,
  WorkspaceId
}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import java.net.URL
import java.time.Instant

import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.RuntimeSamResourceId

object RuntimeRoutesTestJsonCodec {
  implicit val dataprocConfigRequestEncoder: Encoder[RuntimeConfigRequest.DataprocConfig] = Encoder.forProduct11(
    "cloudService",
    "numberOfWorkers",
    "masterMachineType",
    "masterDiskSize",
    // worker settings are None when numberOfWorkers is 0
    "workerMachineType",
    "workerDiskSize",
    "numberOfWorkerLocalSSDs",
    "numberOfPreemptibleWorkers",
    "region",
    "componentGatewayEnabled",
    "workerPrivateAccess"
  )(x =>
    (x.cloudService,
     x.numberOfWorkers,
     x.masterMachineType,
     x.masterDiskSize,
     x.workerMachineType,
     x.workerDiskSize,
     x.numberOfWorkerLocalSSDs,
     x.numberOfPreemptibleWorkers,
     x.region,
     x.componentGatewayEnabled,
     x.workerPrivateAccess
    )
  )
  implicit val gceRuntimeConfigRequestEncoder: Encoder[RuntimeConfigRequest.GceConfig] = Encoder.forProduct5(
    "cloudService",
    "machineType",
    "diskSize",
    "zone",
    "gpuConfig"
  )(x => (x.cloudService, x.machineType, x.diskSize, x.zone, x.gpuConfig))

  implicit val gceWithPdRuntimeConfigRequestEncoder: Encoder[RuntimeConfigRequest.GceWithPdConfig] =
    Encoder.forProduct5(
      "cloudService",
      "machineType",
      "persistentDisk",
      "zone",
      "gpuConfig"
    )(x => (x.cloudService, x.machineType, x.persistentDisk, x.zone, x.gpuConfig))

  implicit val runtimeConfigRequestEncoder: Encoder[RuntimeConfigRequest] = Encoder.instance { x =>
    x match {
      case x: RuntimeConfigRequest.DataprocConfig  => x.asJson
      case x: RuntimeConfigRequest.GceConfig       => x.asJson
      case x: RuntimeConfigRequest.GceWithPdConfig => x.asJson
    }
  }

  implicit val azureDiskConfigEncoder: Encoder[CreateAzureDiskRequest] =
    Encoder.forProduct4("labels", "name", "size", "diskType")(x => CreateAzureDiskRequest.unapply(x).get)
  implicit val createAzureRuntimeRequestEncoder: Encoder[CreateAzureRuntimeRequest] =
    Encoder.forProduct5("labels", "machineSize", "customEnvironmentVariables", "disk", "autopauseThreshold")(x =>
      CreateAzureRuntimeRequest.unapply(x).get
    )

  implicit val createRuntime2RequestEncoder: Encoder[CreateRuntimeRequest] = Encoder.forProduct13(
    "labels",
    "userScriptUri",
    "startUserScriptUri",
    "runtimeConfig",
    "userJupyterExtensionConfig",
    "autopause",
    "autopauseThreshold",
    "defaultClientId",
    "toolDockerImage",
    "welderRegistry",
    "scopes",
    "customEnvironmentVariables",
    "checkToolsInterruptAfter"
  )(x =>
    (
      x.labels,
      x.userScriptUri,
      x.startUserScriptUri,
      x.runtimeConfig,
      x.userJupyterExtensionConfig,
      x.autopause,
      x.autopauseThreshold.map(_.toMinutes),
      x.defaultClientId,
      x.toolDockerImage,
      x.welderRegistry,
      x.scopes,
      x.customEnvironmentVariables,
      x.checkToolsInterruptAfter.map(_.toMinutes)
    )
  )

  implicit val updateGceConfigRequestEncoder: Encoder[UpdateRuntimeConfigRequest.GceConfig] = Encoder.forProduct3(
    "cloudService",
    "machineType",
    "diskSize"
  )(x => (x.cloudService, x.updatedMachineType, x.updatedDiskSize))

  implicit val updateDataprocConfigRequestEncoder: Encoder[UpdateRuntimeConfigRequest.DataprocConfig] =
    Encoder.forProduct5(
      "cloudService",
      "masterMachineType",
      "masterDiskSize",
      "numberOfWorkers",
      "numberOfPreemptibleWorkers"
    )(x =>
      (x.cloudService,
       x.updatedMasterMachineType,
       x.updatedMasterDiskSize,
       x.updatedNumberOfWorkers,
       x.updatedNumberOfPreemptibleWorkers
      )
    )

  implicit val updateRuntimeConfigRequestEncoder: Encoder[UpdateRuntimeConfigRequest] = Encoder.instance { x =>
    x match {
      case x: UpdateRuntimeConfigRequest.DataprocConfig => x.asJson
      case x: UpdateRuntimeConfigRequest.GceConfig      => x.asJson
    }
  }

  implicit val updateRuntimeRequestEncoder: Encoder[UpdateRuntimeRequest] = Encoder.forProduct4(
    "runtimeConfig",
    "allowStop",
    "autopause",
    "autopauseThreshold"
  )(x =>
    (
      x.updatedRuntimeConfig,
      x.allowStop,
      x.updateAutopauseEnabled,
      x.updateAutopauseThreshold.map(_.toMinutes)
    )
  )

  implicit val getClusterResponseDecoder: Decoder[GetRuntimeResponse] = Decoder.instance { x =>
    for {
      id <- x.downField("id").as[Long]
      clusterName <- x.downField("runtimeName").as[RuntimeName]
      _ <- x
        .downField("googleProject")
        .as[GoogleProject] // this is only here for backwards-compatibility test. Once the API move away from googleProject, we can remove this as well
      cloudContext <- x.downField("cloudContext").as[CloudContext]
      serviceAccount <- x.downField("serviceAccount").as[WorkbenchEmail]
      asyncRuntimeFields <- x.downField("asyncRuntimeFields").as[Option[AsyncRuntimeFields]]
      auditInfo <- x.downField("auditInfo").as[AuditInfo]
      kernelFoundBusyDate <- x.downField("kernelFoundBusyDate").as[Option[Instant]]
      runtimeConfig <- x.downField("runtimeConfig").as[RuntimeConfig]
      clusterUrl <- x.downField("proxyUrl").as[URL]
      status <- x.downField("status").as[RuntimeStatus]
      labels <- x.downField("labels").as[LabelMap]
      jupyterUserScriptUri <- x.downField("jupyterUserScriptUri").as[Option[UserScriptPath]]
      jupyterStartUserScriptUri <- x.downField("jupyterStartUserScriptUri").as[Option[UserScriptPath]]
      errors <- x.downField("errors").as[List[RuntimeError]]
      userJupyterExtensionConfig <- x.downField("userJupyterExtensionConfig").as[Option[UserJupyterExtensionConfig]]
      autopauseThreshold <- x.downField("autopauseThreshold").as[Int]
      defaultClientId <- x.downField("defaultClientId").as[Option[String]]
      clusterImages <- x.downField("runtimeImages").as[Set[RuntimeImage]]
      scopes <- x.downField("scopes").as[Set[String]]
    } yield GetRuntimeResponse(
      id,
      RuntimeSamResourceId(""),
      clusterName,
      cloudContext,
      serviceAccount,
      asyncRuntimeFields,
      auditInfo,
      kernelFoundBusyDate,
      runtimeConfig,
      clusterUrl,
      status,
      labels,
      jupyterUserScriptUri,
      jupyterStartUserScriptUri,
      errors,
      userJupyterExtensionConfig,
      autopauseThreshold,
      defaultClientId,
      clusterImages,
      scopes,
      true,
      false,
      Map.empty,
      None
    )
  }

  implicit val listClusterResponseDecoder: Decoder[ListRuntimeResponse2] = Decoder.instance { x =>
    for {
      id <- x.downField("id").as[Long]
      workspaceId <- x.downField("workspaceId").as[Option[WorkspaceId]]
      clusterName <- x.downField("runtimeName").as[RuntimeName]
      cloudContext <- x.downField("cloudContext").as[CloudContext]
      auditInfo <- x.downField("auditInfo").as[AuditInfo]
      machineConfig <- x.downField("runtimeConfig").as[RuntimeConfig]
      clusterUrl <- x.downField("proxyUrl").as[URL]
      status <- x.downField("status").as[RuntimeStatus]
      labels <- x.downField("labels").as[LabelMap]
      patchInProgress <- x.downField("patchInProgress").as[Boolean]
    } yield ListRuntimeResponse2(
      id,
      workspaceId,
      RuntimeSamResourceId("fakeId"),
      clusterName,
      cloudContext,
      auditInfo,
      machineConfig,
      clusterUrl,
      status,
      labels,
      patchInProgress
    )
  }
}
