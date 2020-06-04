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
     x.numberOfPreemptibleWorkers)
  )
  implicit val gceRuntimConfigEncoder: Encoder[RuntimeConfigRequest.GceConfig] = Encoder.forProduct3(
    "cloudService",
    "machineType",
    "diskSize"
  )(x => (x.cloudService, x.machineType, x.diskSize))

  implicit val persistentDiskRequestEncoder: Encoder[PersistentDiskRequest] = Encoder.forProduct5(
    "name",
    "size",
    "diskType",
    "blockSize",
    "labels"
  )(x => (x.name, x.size, x.diskType, x.blockSize, x.labels))

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
}
