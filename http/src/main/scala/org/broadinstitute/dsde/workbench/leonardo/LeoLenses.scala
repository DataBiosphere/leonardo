package org.broadinstitute.dsde.workbench.leonardo

import cats.implicits._

import java.time.Instant
import monocle.macros.GenLens
import monocle.{Lens, Optional, Prism}
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.google2.{RegionName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.http.{
  dataprocInCreateRuntimeMsgToDataprocRuntime,
  dataprocRuntimeToDataprocInCreateRuntimeMsg
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.{DiskUpdate, RuntimeConfigInCreateRuntimeMessage}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
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

  val ipRuntimeAndRuntimeConfig: Optional[RuntimeAndRuntimeConfig, IP] = runtimeAndRuntimeConfigToRuntime
    .andThen(runtimeToAsyncRuntimeFields)
    .andThen(asyncRuntimeFieldsToIp)

  val statusRuntimeAndRuntimeConfig: Lens[RuntimeAndRuntimeConfig, RuntimeStatus] =
    GenLens[RuntimeAndRuntimeConfig](x => x.runtime.status)

  val diskToDestroyedDate: Lens[PersistentDisk, Option[Instant]] = GenLens[PersistentDisk](_.auditInfo.destroyedDate)
  val cloudContextToGoogleProject: Lens[CloudContext, Option[GoogleProject]] =
    Lens[CloudContext, Option[GoogleProject]] { x =>
      x match {
        case p: CloudContext.Gcp   => p.value.some
        case _: CloudContext.Azure => none[GoogleProject]
      }
    }(googleProjectOpt => cloudContext => googleProjectOpt.fold(cloudContext)(p => CloudContext.Gcp(p)))
  val cloudContextToManagedResourceGroup: Lens[CloudContext, Option[AzureCloudContext]] =
    Lens[CloudContext, Option[AzureCloudContext]] { x =>
      x match {
        case _: CloudContext.Gcp   => none[AzureCloudContext]
        case p: CloudContext.Azure => p.value.some
      }
    }(mrg => cloudContext => mrg.fold(cloudContext)(p => CloudContext.Azure(p)))

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
            ),
          x.zone,
          x.gpuConfig
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
          x.bootDiskSize,
          x.zone,
          x.gpuConfig
        )
      )
    case x: RuntimeConfig.DataprocConfig =>
      Some(dataprocRuntimeToDataprocInCreateRuntimeMsg(x))
    case _: RuntimeConfig.AzureConfig =>
      throw AzureUnimplementedException("Azure vms should not be handled with existing create runtime message")
  } {
    case x: RuntimeConfigInCreateRuntimeMessage.GceConfig =>
      RuntimeConfig.GceConfig(
        x.machineType,
        x.diskSize,
        Some(x.bootDiskSize),
        x.zone,
        x.gpuConfig
      )
    case x: RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig =>
      RuntimeConfig.GceWithPdConfig(
        x.machineType,
        Some(x.persistentDiskId),
        x.bootDiskSize,
        x.zone,
        x.gpuConfig
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

  val kubernetesClusterToDestroyedDate: Lens[KubernetesCluster, Option[Instant]] =
    GenLens[KubernetesCluster](_.auditInfo.destroyedDate)

  val nodepoolToDestroyedDate: Lens[Nodepool, Option[Instant]] = GenLens[Nodepool](_.auditInfo.destroyedDate)

  val dataprocRegion: Optional[RuntimeConfig, RegionName] = Optional[RuntimeConfig, RegionName] {
    case x: RuntimeConfig.DataprocConfig => Some(x.region)
    case _                               => None
  }(r =>
    x =>
      x match {
        case x: RuntimeConfig.DataprocConfig => x.copy(region = r)
        case x                               => x
      }
  )

  val gceZone: Optional[RuntimeConfig, ZoneName] = Optional[RuntimeConfig, ZoneName] {
    case x: RuntimeConfig.GceConfig       => Some(x.zone)
    case x: RuntimeConfig.GceWithPdConfig => Some(x.zone)
    case _                                => None
  }(z =>
    x =>
      x match {
        case x: RuntimeConfig.GceConfig       => x.copy(zone = z)
        case x: RuntimeConfig.GceWithPdConfig => x.copy(zone = z)
        case x                                => x
      }
  )
}
