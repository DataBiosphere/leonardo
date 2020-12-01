package org.broadinstitute.dsde.workbench.leonardo.http

import io.circe.Encoder
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import io.circe.syntax._

object RuntimeRoutesTestJsonCodec {
  implicit val dataprocConfigEncoder: Encoder[RuntimeConfigRequest.DataprocConfig] = Encoder.forProduct8(
    "cloudService",
    "numberOfWorkers",
    "masterMachineType",
    "masterDiskSize",
    // worker settings are None when numberOfWorkers is 0
    "workerMachineType",
    "workerDiskSize",
    "numberOfWorkerLocalSSDs",
    "numberOfPreemptibleWorkers"
  )(x =>
    (x.cloudService,
     x.numberOfWorkers,
     x.masterMachineType,
     x.masterDiskSize,
     x.workerMachineType,
     x.workerDiskSize,
     x.numberOfWorkerLocalSSDs,
     x.numberOfPreemptibleWorkers
    )
  )
  implicit val gceRuntimConfigEncoder: Encoder[RuntimeConfigRequest.GceConfig] = Encoder.forProduct3(
    "cloudService",
    "machineType",
    "diskSize"
  )(x => (x.cloudService, x.machineType, x.diskSize))

  implicit val gceWithPdRuntimConfigEncoder: Encoder[RuntimeConfigRequest.GceWithPdConfig] = Encoder.forProduct3(
    "cloudService",
    "machineType",
    "persistentDisk"
  )(x => (x.cloudService, x.machineType, x.persistentDisk))

  implicit val runtimeConfigRequestEncoder: Encoder[RuntimeConfigRequest] = Encoder.instance { x =>
    x match {
      case x: RuntimeConfigRequest.DataprocConfig  => x.asJson
      case x: RuntimeConfigRequest.GceConfig       => x.asJson
      case x: RuntimeConfigRequest.GceWithPdConfig => x.asJson
    }
  }

  implicit val createRuntime2RequestEncoder: Encoder[CreateRuntime2Request] = Encoder.forProduct12(
    "labels",
    "jupyterUserScriptUri",
    "jupyterStartUserScriptUri",
    "runtimeConfig",
    "userJupyterExtensionConfig",
    "autopause",
    "autopauseThreshold",
    "defaultClientId",
    "toolDockerImage",
    "welderDockerImage",
    "scopes",
    "customEnvironmentVariables"
  )(x =>
    (
      x.labels,
      x.jupyterUserScriptUri,
      x.jupyterStartUserScriptUri,
      x.runtimeConfig,
      x.userJupyterExtensionConfig,
      x.autopause,
      x.autopauseThreshold.map(_.toMinutes),
      x.defaultClientId,
      x.toolDockerImage,
      x.welderDockerImage,
      x.scopes,
      x.customEnvironmentVariables
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
}
