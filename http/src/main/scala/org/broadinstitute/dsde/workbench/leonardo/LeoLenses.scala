package org.broadinstitute.dsde.workbench.leonardo

import java.time.Instant

import monocle.{Lens, Optional}
import monocle.macros.GenLens
import org.broadinstitute.dsde.workbench.leonardo.db.GetClusterKey
import org.broadinstitute.dsde.workbench.leonardo.http.service.{CreateRuntimeResponse, ListRuntimeResponse}

object LeoLenses {
  val runtimeToRuntimeImages: Lens[Runtime, Set[RuntimeImage]] = GenLens[Runtime](_.runtimeImages)

  val runtimeToAuditInfo: Lens[Runtime, AuditInfo] = GenLens[Runtime](_.auditInfo)

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
      x.stopAfterCreation,
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
        stopAfterCreation = x.stopAfterCreation,
        welderEnabled = x.welderEnabled,
        patchInProgress = x.patchInProgress
      )
  )

  val diskToDestroyedDate: Lens[PersistentDisk, Option[Instant]] = GenLens[PersistentDisk](_.auditInfo.destroyedDate)

  val pdInRuntimeConfig: Lens[PersistentDisk, PersistentDiskInRuntimeConfig] =
    Lens[PersistentDisk, PersistentDiskInRuntimeConfig](x =>
      PersistentDiskInRuntimeConfig(x.id, x.zone, x.name, x.status, x.size, x.diskType, x.blockSize)
    )(prc =>
      pd =>
        pd.copy(id = prc.id,
                zone = prc.zone,
                status = prc.status,
                size = prc.size,
                diskType = prc.diskType,
                blockSize = prc.blockSize)
    )
}
