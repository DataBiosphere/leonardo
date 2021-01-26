package org.broadinstitute.dsde.workbench.leonardo

import java.time.Instant

import monocle.{Lens, Optional, Prism}
import monocle.macros.GenLens
import org.broadinstitute.dsde.workbench.leonardo.db.GetClusterKey
import org.broadinstitute.dsde.workbench.leonardo.http.service.{CreateRuntimeResponse, ListRuntimeResponse}
import org.broadinstitute.dsde.workbench.leonardo.monitor.{DiskUpdate, RuntimeConfigInCreateRuntimeMessage}
import org.broadinstitute.dsde.workbench.leonardo.http.dataprocInCreateRuntimeMsgToDataprocRuntime
import org.broadinstitute.dsde.workbench.leonardo.http.dataprocRuntimeToDataprocInCreateRuntimeMsg
import org.broadinstitute.dsde.workbench.model.{IP, WorkbenchEmail}

object LeoLenses {
  val runtimeToRuntimeImages: Lens[Runtime, Set[RuntimeImage]] = GenLens[Runtime](_.runtimeImages)

  val runtimeToAuditInfo: Lens[Runtime, AuditInfo] = GenLens[Runtime](_.auditInfo)

  val runtimeToCreator: Lens[Runtime, WorkbenchEmail] = GenLens[Runtime](_.auditInfo.creator)

  val runtimeToRuntimeConfigId: Lens[Runtime, RuntimeConfigId] = GenLens[Runtime](_.runtimeConfigId)

  val runtimeAndRuntimeConfigToRuntime: Lens[RuntimeAndRuntimeConfig, Runtime] =
    GenLens[RuntimeAndRuntimeConfig](_.runtime)
  val runtimeToAsyncRuntimeFields: Optional[Runtime, AsyncRuntimeFields] =
    Optional[Runtime, AsyncRuntimeFields](x => x.asyncRuntimeFields)(asf => x => x.copy(asyncRuntimeFields = Some(asf)))
  val asyncRuntimeFieldsToIp: Optional[AsyncRuntimeFields, IP] =
    Optional[AsyncRuntimeFields, IP](x => x.hostIp)(ip => x => x.copy(hostIp = Some(ip)))

  val createRuntimeRespToGetClusterKey = Lens[CreateRuntimeResponse, GetClusterKey](x =>
    GetClusterKey(x.googleProject, x.clusterName, x.auditInfo.destroyedDate)
  )(x =>
    a =>
      a.copy(googleProject = x.googleProject,
             clusterName = x.clusterName,
             auditInfo = a.auditInfo.copy(destroyedDate = x.destroyedDate))
  )

  val ipRuntimeAndRuntimeConfig: Optional[RuntimeAndRuntimeConfig, IP] = runtimeAndRuntimeConfigToRuntime
    .composeOptional(runtimeToAsyncRuntimeFields)
    .composeOptional(asyncRuntimeFieldsToIp)

  val statusRuntimeAndRuntimeConfig: Lens[RuntimeAndRuntimeConfig, RuntimeStatus] =
    GenLens[RuntimeAndRuntimeConfig](x => x.runtime.status)

  val createRuntimeRespToListRuntimeResp = Lens[CreateRuntimeResponse, ListRuntimeResponse](x =>
    ListRuntimeResponse(
      x.id,
      x.samResource,
      x.clusterName,
      x.googleProject,
      x.serviceAccountInfo,
      x.asyncRuntimeFields,
      x.auditInfo,
      x.kernelFoundBusyDate,
      x.runtimeConfig,
      x.clusterUrl,
      x.status,
      x.labels,
      x.jupyterUserScriptUri,
      x.dataprocInstances,
      x.autopauseThreshold,
      x.defaultClientId,
      x.welderEnabled,
      x.patchInProgress
    )
  )(x =>
    c =>
      c.copy(
        id = x.id,
        samResource = x.samResource,
        clusterName = x.clusterName,
        googleProject = x.googleProject,
        serviceAccountInfo = x.serviceAccountInfo,
        asyncRuntimeFields = x.asyncRuntimeFields,
        auditInfo = x.auditInfo,
        kernelFoundBusyDate = x.kernelFoundBusyDate,
        clusterUrl = x.clusterUrl,
        status = x.status,
        labels = x.labels,
        jupyterExtensionUri = None,
        jupyterUserScriptUri = x.jupyterUserScriptUri,
        dataprocInstances = x.dataprocInstances,
        autopauseThreshold = x.autopauseThreshold,
        defaultClientId = x.defaultClientId,
        welderEnabled = x.welderEnabled,
        patchInProgress = x.patchInProgress
      )
  )

  val diskToDestroyedDate: Lens[PersistentDisk, Option[Instant]] = GenLens[PersistentDisk](_.auditInfo.destroyedDate)

  val diskToCreator: Lens[PersistentDisk, WorkbenchEmail] = GenLens[PersistentDisk](_.auditInfo.creator)

  val runtimeConfigPrism = Prism[RuntimeConfig, RuntimeConfigInCreateRuntimeMessage] {
    case x: RuntimeConfig.GceConfig =>
      Some(
        RuntimeConfigInCreateRuntimeMessage.GceConfig(
          x.machineType,
          x.diskSize,
          x.bootDiskSize
            .getOrElse(
              throw new Exception(
                "Can't use this RuntimeConfig as RuntimeConfigInCreateRuntimeMessage due to bootDiskSize not defined"
              )
            )
        )
      )
    case x: RuntimeConfig.GceWithPdConfig =>
      Some(
        RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig(
          x.machineType,
          x.persistentDiskId.getOrElse(
            throw new Exception(
              "Can't use this RuntimeConfig as RuntimeConfigInCreateRuntimeMessage due to persistentDiskId not defined"
            )
          ),
          x.bootDiskSize
        )
      )
    case x: RuntimeConfig.DataprocConfig =>
      Some(dataprocRuntimeToDataprocInCreateRuntimeMsg(x))
  } {
    case x: RuntimeConfigInCreateRuntimeMessage.GceConfig =>
      RuntimeConfig.GceConfig(
        x.machineType,
        x.diskSize,
        Some(x.bootDiskSize)
      )
    case x: RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig =>
      RuntimeConfig.GceWithPdConfig(
        x.machineType,
        Some(x.persistentDiskId),
        x.bootDiskSize
      )
    case x: RuntimeConfigInCreateRuntimeMessage.DataprocConfig =>
      dataprocInCreateRuntimeMsgToDataprocRuntime(x)
  }

  val dataprocPrism = Prism[RuntimeConfig, RuntimeConfig.DataprocConfig] {
    case x: RuntimeConfig.DataprocConfig => Some(x)
    case _                               => None
  }(identity)

  val pdSizeUpdatePrism = Prism[DiskUpdate, DiskUpdate.PdSizeUpdate] {
    case x: DiskUpdate.PdSizeUpdate => Some(x)
    case _                          => None
  }(identity)

  val appToServices: Lens[App, List[KubernetesService]] = GenLens[App](_.appResources.services)

  val appToCreator: Lens[App, WorkbenchEmail] = GenLens[App](_.auditInfo.creator)

  val appToDisk: Lens[App, Option[PersistentDisk]] = GenLens[App](_.appResources.disk)
}
