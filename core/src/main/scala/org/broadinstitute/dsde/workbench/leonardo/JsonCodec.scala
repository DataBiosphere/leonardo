package org.broadinstitute.dsde.workbench.leonardo

import cats.syntax.all._
import com.azure.core.management.Region
import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes
import io.circe.syntax._
import io.circe.{Decoder, DecodingFailure, Encoder, Json}
import org.broadinstitute.dsde.workbench.azure.{
  AKSClusterName,
  ApplicationInsightsName,
  AzureCloudContext,
  BatchAccountName,
  RelayNamespace
}
import org.broadinstitute.dsde.workbench.google2.GKEModels.{KubernetesClusterName, NodepoolName}
import org.broadinstitute.dsde.workbench.google2.JsonCodec.{traceIdDecoder, traceIdEncoder}
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.KubernetesApiServerIp
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceName}
import org.broadinstitute.dsde.workbench.google2.{
  DataprocRole,
  DiskName,
  KubernetesName,
  Location,
  MachineTypeName,
  NetworkName,
  OperationName,
  RegionName,
  SubnetworkName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId._
import org.broadinstitute.dsde.workbench.leonardo.http.{
  CreateRuntimeResponse,
  DiskConfig,
  GetRuntimeResponse,
  PersistentDiskRequest
}
import org.broadinstitute.dsde.workbench.model.google.{
  parseGcsPath,
  GcsBucketName,
  GcsObjectName,
  GcsPath,
  GoogleProject
}
import org.broadinstitute.dsde.workbench.model.{IP, WorkbenchEmail, WorkbenchUserId}
import org.broadinstitute.dsde.workbench.util2.InstanceName
import org.broadinstitute.dsp.ChartName
import org.http4s.Uri

import java.net.URL
import java.nio.file.{Path, Paths}
import java.time.Instant
import java.util.UUID
import java.util.stream.Collectors

object JsonCodec {
  // Errors
  val negativeNumberDecodingFailure = DecodingFailure("Negative number is not allowed", List.empty)
  val oneWorkerSpecifiedDecodingFailure = DecodingFailure(
    "Google Dataproc does not support clusters with 1 non-preemptible worker. Must be 0, 2 or more.",
    List.empty
  )
  val numNodesDecodingFailure = DecodingFailure(
    "num nodes must be >= 1",
    List.empty
  )
  val deleteDefaultLabelsDecodingFailure = DecodingFailure("Default labels are not allowed to be deleted", List.empty)
  val updateDefaultLabelDecodingFailure = DecodingFailure("Default labels are not allowed to be altered", List.empty)
  val upsertEmptyLabelDecodingFailure = DecodingFailure("Label values are not allowed to be empty", List.empty)

  implicit val operationNameEncoder: Encoder[OperationName] = Encoder.encodeString.contramap(_.value)
  implicit val googleIdEncoder: Encoder[ProxyHostName] = Encoder.encodeString.contramap(_.value)
  implicit val ipEncoder: Encoder[IP] = Encoder.encodeString.contramap(_.asString)
  implicit val gcsBucketNameEncoder: Encoder[GcsBucketName] = Encoder.encodeString.contramap(_.value)
  implicit val workbenchEmailEncoder: Encoder[WorkbenchEmail] = Encoder.encodeString.contramap(_.value)
  implicit val gcsObjectNameEncoder: Encoder[GcsObjectName] = Encoder.encodeString.contramap(_.value)
  implicit val googleProjectEncoder: Encoder[GoogleProject] = Encoder.encodeString.contramap(_.value)
  implicit val gcsPathEncoder: Encoder[GcsPath] = Encoder.encodeString.contramap(_.toUri)
  implicit val userScriptPathEncoder: Encoder[UserScriptPath] = Encoder.encodeString.contramap(_.asString)
  implicit val machineTypeEncoder: Encoder[MachineTypeName] = Encoder.encodeString.contramap(_.value)
  implicit val cloudServiceEncoder: Encoder[CloudService] = Encoder.encodeString.contramap(_.asString)
  implicit val runtimeNameEncoder: Encoder[RuntimeName] = Encoder.encodeString.contramap(_.asString)
  implicit val runtimeSamResourceIdEncoder: Encoder[RuntimeSamResourceId] = Encoder.encodeString.contramap(_.resourceId)
  implicit val storageContainerNameEncoder: Encoder[org.broadinstitute.dsde.workbench.azure.ContainerName] =
    Encoder.encodeString.contramap(_.value)
  implicit val storageAccountNameEncoder: Encoder[StorageAccountName] =
    Encoder.encodeString.contramap(_.value)
  implicit val urlEncoder: Encoder[URL] = Encoder.encodeString.contramap(_.toString)
  implicit val zoneNameEncoder: Encoder[ZoneName] = Encoder.encodeString.contramap(_.value)
  implicit val regionNameEncoder: Encoder[RegionName] = Encoder.encodeString.contramap(_.value)
  implicit val diskNameEncoder: Encoder[DiskName] = Encoder.encodeString.contramap(_.value)
  implicit val diskSamResourceIdEncoder: Encoder[PersistentDiskSamResourceId] =
    Encoder.encodeString.contramap(_.resourceId)
  implicit val diskSizeEncoder: Encoder[DiskSize] = Encoder.encodeInt.contramap(_.gb)
  implicit val blockSizeEncoder: Encoder[BlockSize] = Encoder.encodeInt.contramap(_.bytes)
  implicit val diskIdEncoder: Encoder[DiskId] = Encoder.encodeLong.contramap(_.value)
  implicit val diskStatusEncoder: Encoder[DiskStatus] = Encoder.encodeString.contramap(_.entryName)
  implicit val diskTypeEncoder: Encoder[DiskType] = Encoder.encodeString.contramap(_.asString)
  implicit val gpuTypeEncoder: Encoder[GpuType] = Encoder.encodeString.contramap(_.asString)
  implicit val cloudProviderEncoder: Encoder[CloudProvider] = Encoder.encodeString.contramap(_.asString)
  implicit val diskLinkEncoder: Encoder[DiskLink] = Encoder.encodeString.contramap(_.asString)
  implicit val formattedByEncoder: Encoder[FormattedBy] = Encoder.encodeString.contramap(_.asString)
  implicit val toolEncoder: Encoder[Tool] = Encoder.encodeString.contramap(_.asString)
  implicit val runtimeConfigTypeEncoder: Encoder[RuntimeConfigType] = Encoder.encodeString.contramap(_.asString)
  implicit val appAccessScopeEncoder: Encoder[AppAccessScope] = Encoder.encodeString.contramap(_.toString)

  implicit val cloudContextEncoder: Encoder[CloudContext] = Encoder.forProduct2(
    "cloudProvider",
    "cloudResource"
  )(x => (x.cloudProvider, x.asString))

  implicit val gpuConfigEncoder: Encoder[GpuConfig] = Encoder.forProduct2(
    "gpuType",
    "numOfGpus"
  )(x => GpuConfig.unapply(x).get)

  implicit val appMachineTypeEncoder: Encoder[AppMachineType] = Encoder.forProduct2(
    "memorySizeInGb",
    "numOfCpus"
  )(x => AppMachineType.unapply(x).get)

  implicit val dataprocConfigEncoder: Encoder[RuntimeConfig.DataprocConfig] = Encoder.forProduct12(
    "numberOfWorkers",
    "masterMachineType",
    "masterDiskSize",
    // worker settings are None when numberOfWorkers is 0
    "workerMachineType",
    "workerDiskSize",
    "numberOfWorkerLocalSSDs",
    "numberOfPreemptibleWorkers",
    "cloudService",
    "region",
    "componentGatewayEnabled",
    "workerPrivateAccess",
    "configType"
  )(x =>
    (x.numberOfWorkers,
     x.machineType,
     x.diskSize,
     x.workerMachineType,
     x.workerDiskSize,
     x.numberOfWorkerLocalSSDs,
     x.numberOfPreemptibleWorkers,
     x.cloudService,
     x.region,
     x.componentGatewayEnabled,
     x.workerPrivateAccess,
     x.configType
    )
  )
  implicit val gceRuntimeConfigEncoder: Encoder[RuntimeConfig.GceConfig] = Encoder.forProduct7(
    "machineType",
    "diskSize",
    "cloudService",
    "bootDiskSize",
    "zone",
    "gpuConfig",
    "configType"
  )(x => (x.machineType, x.diskSize, x.cloudService, x.bootDiskSize, x.zone, x.gpuConfig, x.configType))

  implicit val azureRegionEncoder: Encoder[Region] = Encoder.encodeString.contramap(_.toString)

  implicit val applicationInsightsNameEncoder: Encoder[ApplicationInsightsName] =
    Encoder.encodeString.contramap(_.value)
  implicit val azureRuntimeConfigEncoder: Encoder[RuntimeConfig.AzureConfig] = Encoder.forProduct5(
    "cloudService",
    "machineType",
    "persistentDiskId",
    "region",
    "configType"
  )(x => (x.cloudService, x.machineType, x.persistentDiskId, x.region, x.configType))

  implicit val azureMachineTypeDecoder: Decoder[VirtualMachineSizeTypes] = Decoder.decodeString.emap { s =>
    val machineSizeOpt: Option[VirtualMachineSizeTypes] =
      if (
        VirtualMachineSizeTypes.values.stream
          .map((x: VirtualMachineSizeTypes) => x.toString)
          .collect(Collectors.toList[String])
          .contains(s)
      )
        Some(VirtualMachineSizeTypes.fromString(s))
      else none[VirtualMachineSizeTypes]
    machineSizeOpt.toRight(s"Invalid azure virtualMachineSizeType ${s}")
  }
  implicit val azureImageUriDecoder: Decoder[AzureImage] = Decoder.forProduct4(
    "publisher",
    "offer",
    "sku",
    "version"
  )((x, y, z, v) => AzureImage(x, y, z, v))
  implicit val azureDiskNameDecoder: Decoder[AzureDiskName] = Decoder.decodeString.map(AzureDiskName)

  implicit val userJupyterExtensionConfigEncoder: Encoder[UserJupyterExtensionConfig] = Encoder.forProduct4(
    "nbExtensions",
    "serverExtensions",
    "combinedExtensions",
    "labExtensions"
  )(x => UserJupyterExtensionConfig.unapply(x).get)
  implicit val auditInfoEncoder: Encoder[AuditInfo] = Encoder.forProduct4(
    "creator",
    "createdDate",
    "destroyedDate",
    "dateAccessed"
  )(x => AuditInfo.unapply(x).get)
  implicit val runtimeImageTypeEncoder: Encoder[RuntimeImageType] = Encoder.encodeString.contramap(_.toString)
  implicit val containerImageEncoder: Encoder[ContainerImage] = Encoder.encodeString.contramap(_.imageUrl)
  implicit val containerRegistryEncoder: Encoder[ContainerRegistry] = Encoder.encodeString.contramap(_.entryName)
  implicit val pathEncoder: Encoder[Path] = Encoder.encodeString.contramap(_.toString)
  implicit val runtimeImageEncoder: Encoder[RuntimeImage] = Encoder.forProduct4(
    "imageType",
    "imageUrl",
    "homeDirectory",
    "timestamp"
  )(x => RuntimeImage.unapply(x).get)
  implicit val gceWithPdConfigEncoder: Encoder[RuntimeConfig.GceWithPdConfig] = Encoder.forProduct7(
    "machineType",
    "persistentDiskId",
    "cloudService",
    "bootDiskSize",
    "zone",
    "gpuConfig",
    "configType"
  )(x => (x.machineType, x.persistentDiskId, x.cloudService, x.bootDiskSize, x.zone, x.gpuConfig, x.configType))

  implicit val runtimeConfigEncoder: Encoder[RuntimeConfig] = Encoder.instance(x =>
    x match {
      case x: RuntimeConfig.DataprocConfig  => x.asJson
      case x: RuntimeConfig.GceConfig       => x.asJson
      case x: RuntimeConfig.GceWithPdConfig => x.asJson
      case x: RuntimeConfig.AzureConfig     => x.asJson
    }
  )
  implicit val defaultRuntimeLabelsEncoder: Encoder[DefaultRuntimeLabels] = Encoder.forProduct8(
    "clusterName",
    "googleProject",
    "cloudContext",
    "creator",
    "clusterServiceAccount",
    "notebookUserScript",
    "notebookStartUserScript",
    "tool"
  )(x => DefaultRuntimeLabels.unapply(x).get)
  implicit val asyncRuntimeFieldsEncoder: Encoder[AsyncRuntimeFields] =
    Encoder.forProduct4("googleId", "operationName", "stagingBucket", "hostIp")(x => AsyncRuntimeFields.unapply(x).get)
  implicit val clusterProjectAndNameEncoder: Encoder[RuntimeProjectAndName] = Encoder.forProduct3(
    "cloudProvider",
    "cloudContext",
    "runtimeName"
  )(x => (x.cloudContext.cloudProvider, x.cloudContext.asString, x.runtimeName))
  implicit val runtimeErrorEncoder: Encoder[RuntimeError] = Encoder.forProduct4(
    "errorMessage",
    "errorCode",
    "timestamp",
    "traceId"
  )(x => RuntimeError.unapply(x).get)
  implicit val errorSourceEncoder: Encoder[ErrorSource] = Encoder.encodeString.contramap(_.toString)
  implicit val allowedChartNameEncoder: Encoder[AllowedChartName] = Encoder.encodeString.contramap(_.asString)
  implicit val chartNameEncoder: Encoder[ChartName] = Encoder.encodeString.contramap(_.asString)
  implicit val errorActionEncoder: Encoder[ErrorAction] = Encoder.encodeString.contramap(_.toString)
  implicit val appErrorEncoder: Encoder[AppError] =
    Encoder.forProduct6("errorMessage", "timestamp", "action", "source", "googleErrorCode", "traceId")(x =>
      AppError.unapply(x).get
    )
  implicit val nodepoolIdEncoder: Encoder[NodepoolLeoId] = Encoder.encodeLong.contramap(_.id)
  implicit val nodepoolNameEncoder: Encoder[NodepoolName] = Encoder.encodeString.contramap(_.value)
  implicit val nodepoolStatusEncoder: Encoder[NodepoolStatus] = Encoder.encodeString.contramap(status =>
    status match {
      case NodepoolStatus.Precreating => NodepoolStatus.Provisioning.toString
      case _                          => status.toString
    }
  )
  implicit val numNodesEncoder: Encoder[NumNodes] = Encoder.encodeInt.contramap(_.amount)
  implicit val autoscalingMinEncoder: Encoder[AutoscalingMin] = Encoder.encodeInt.contramap(_.amount)
  implicit val autoscalingMaxEncoder: Encoder[AutoscalingMax] = Encoder.encodeInt.contramap(_.amount)
  implicit val autoscalingConfigEncoder: Encoder[AutoscalingConfig] =
    Encoder.forProduct2("autoscalingMin", "autoscalingMax")(x => AutoscalingConfig.unapply(x).get)
  implicit val kubernetesRuntimeConfigEncoder: Encoder[KubernetesRuntimeConfig] =
    Encoder.forProduct3("numNodes", "machineType", "autoscalingEnabled")(x => KubernetesRuntimeConfig.unapply(x).get)

  implicit val locationEncoder: Encoder[Location] = Encoder.encodeString.contramap(_.value)
  implicit val kubeClusterIdEncoder: Encoder[KubernetesClusterLeoId] = Encoder.encodeLong.contramap(_.id)
  implicit val kubeClusterNameEncoder: Encoder[KubernetesClusterName] = Encoder.encodeString.contramap(_.value)
  implicit val kubeClusterStatusEncoder: Encoder[KubernetesClusterStatus] = Encoder.encodeString.contramap(status =>
    status match {
      case KubernetesClusterStatus.Precreating => KubernetesClusterStatus.Provisioning.toString
      case _                                   => status.toString
    }
  )
  implicit val appSamIdEncoder: Encoder[AppSamResourceId] = Encoder.encodeString.contramap(_.resourceId)
  implicit val namespaceEncoder: Encoder[NamespaceName] = Encoder.encodeString.contramap(_.value)
  implicit val appNameEncoder: Encoder[AppName] = Encoder.encodeString.contramap(_.value)
  implicit val appStatusEncoder: Encoder[AppStatus] = Encoder.encodeString.contramap { x =>
    x match {
      case AppStatus.Precreating => AppStatus.Provisioning.toString
      case AppStatus.PreStarting => AppStatus.Starting.toString
      case AppStatus.PreStopping => AppStatus.Stopping.toString
      case AppStatus.Predeleting => AppStatus.Deleting.toString
      case _                     => x.toString
    }
  }
  implicit val appTypeEncoder: Encoder[AppType] = Encoder.encodeString.contramap(_.toString)
  implicit val serviceNameEncoder: Encoder[ServiceName] = Encoder.encodeString.contramap(_.value)
  implicit val serviceNameDecoder: Decoder[ServiceName] = Decoder.decodeString.map(s => ServiceName(s))

  implicit val apiServerIpEncoder: Encoder[KubernetesApiServerIp] = Encoder.encodeString.contramap(_.value)
  implicit val networkNameEncoder: Encoder[NetworkName] = Encoder.encodeString.contramap(_.value)
  implicit val subNetworkNameEncoder: Encoder[SubnetworkName] = Encoder.encodeString.contramap(_.value)
  implicit val ipRangeEncoder: Encoder[IpRange] = Encoder.encodeString.contramap(_.value)
  implicit val wsmJobIdEncoder: Encoder[WsmJobId] = Encoder.encodeString.contramap(_.value.toString)

  implicit val batchAccountNameDecoder: Decoder[BatchAccountName] = Decoder.decodeString.map(BatchAccountName)
  implicit val batchAccountNameEncoder: Encoder[BatchAccountName] = Encoder.encodeString.contramap(_.value)

  implicit val networkFieldsEncoder: Encoder[NetworkFields] =
    Encoder.forProduct3("networkName", "subNetworkName", "subNetworkIpRange")(x => NetworkFields.unapply(x).get)
  implicit val kubeAsyncFieldEncoder: Encoder[KubernetesClusterAsyncFields] =
    Encoder.forProduct3("loadBalancerIp", "apiServerIp", "networkInfo")(x =>
      KubernetesClusterAsyncFields.unapply(x).get
    )

  implicit val instanceNameEncoder: Encoder[InstanceName] = Encoder.encodeString.contramap(_.value)
  implicit val dataprocRoleEncoder: Encoder[DataprocRole] =
    Encoder.encodeString.contramap(_.toString) // We've been using `.toString` in db
  implicit val gceInstanceStatusEncoder: Encoder[GceInstanceStatus] =
    Encoder.encodeString.contramap(_.toString) // We've been using `.toString` in db

  implicit val instanceNameDecoder: Decoder[InstanceName] = Decoder.decodeString.map(InstanceName)
  implicit val dataprocRoleDecoder: Decoder[DataprocRole] =
    Decoder.decodeString.emap(s => DataprocRole.stringToDataprocRole.get(s).toRight(s"invalid dataproc role ${s}"))
  implicit val gceInstanceStatusDecoder: Decoder[GceInstanceStatus] =
    Decoder.decodeString.emap(s => GceInstanceStatus.withNameInsensitiveOption(s).toRight(s"invalid gce status ${s}"))
  implicit val operationNameDecoder: Decoder[OperationName] = Decoder.decodeString.map(OperationName)
  implicit val managedResourceGroupDecoder: Decoder[AzureCloudContext] =
    Decoder.decodeString.emap(s => AzureCloudContext.fromString(s))
  implicit val cloudProviderDecoder: Decoder[CloudProvider] =
    Decoder.decodeString.emap(s => CloudProvider.stringToCloudProvider.get(s).toRight(s"invalid cloud provider ${s}"))
  implicit val googleIdDecoder: Decoder[ProxyHostName] = Decoder.decodeString.map(ProxyHostName)
  implicit val ipDecoder: Decoder[IP] = Decoder.decodeString.map(IP)
  implicit val containerImageDecoder: Decoder[ContainerImage] =
    Decoder.decodeString.emap(s => ContainerImage.fromImageUrl(s).toRight(s"invalid container image ${s}"))
  implicit val containerRegistryDecoder: Decoder[ContainerRegistry] =
    Decoder.decodeString.emap(s =>
      ContainerRegistry.withNameInsensitiveOption(s).toRight(s"Unsupported container registry ${s}")
    )
  implicit val cloudServiceDecoder: Decoder[CloudService] =
    Decoder.decodeString.emap(s => CloudService.withNameInsensitiveOption(s).toRight(s"Unsupported cloud service ${s}"))
  implicit val runtimeNameDecoder: Decoder[RuntimeName] = Decoder.decodeString.map(RuntimeName)
  implicit val runtimeStatusDecoder: Decoder[RuntimeStatus] = Decoder.decodeString.map(s => RuntimeStatus.withName(s))
  implicit val machineTypeDecoder: Decoder[MachineTypeName] = Decoder.decodeString.emap(s =>
    if (s.isEmpty) Left("machine type cannot be an empty string") else Right(MachineTypeName(s))
  )
  implicit val instantDecoder: Decoder[Instant] =
    Decoder.decodeString.emap(s => Either.catchNonFatal(Instant.parse(s)).leftMap(_.getMessage))
  implicit val gcsBucketNameDecoder: Decoder[GcsBucketName] = Decoder.decodeString.map(GcsBucketName)
  implicit val googleProjectDecoder: Decoder[GoogleProject] = Decoder.decodeString.map(GoogleProject)
  implicit val urlDecoder: Decoder[URL] =
    Decoder.decodeString.emap(s => Either.catchNonFatal(new URL(s)).leftMap(_.getMessage))
  implicit val diskSizeDecoder: Decoder[DiskSize] =
    Decoder.decodeInt.emap(d => if (d < 5) Left("Minimum required disk size is 5GB") else Right(DiskSize(d)))
  implicit val blockSizeDecoder: Decoder[BlockSize] =
    Decoder.decodeInt.emap(d => if (d < 0) Left("Negative number is not allowed") else Right(BlockSize(d)))
  implicit val workbenchEmailDecoder: Decoder[WorkbenchEmail] = Decoder.decodeString.map(WorkbenchEmail)
  implicit val workbenchUserIdDecoder: Decoder[WorkbenchUserId] = Decoder.decodeString.map(WorkbenchUserId)
  implicit val storageContainerNameDecoder: Decoder[org.broadinstitute.dsde.workbench.azure.ContainerName] =
    Decoder.decodeString.map(org.broadinstitute.dsde.workbench.azure.ContainerName)
  implicit val storageAccountNameDecoder: Decoder[StorageAccountName] =
    Decoder.decodeString.map(StorageAccountName)
  implicit val pathDecoder: Decoder[Path] = Decoder.decodeString.map(s => Paths.get(s))
  implicit val runtimeImageTypeDecoder: Decoder[RuntimeImageType] = Decoder.decodeString.emap(s =>
    RuntimeImageType.stringToRuntimeImageType.get(s).toRight(s"invalid RuntimeImageType ${s}")
  )
  implicit val toolDecoder: Decoder[Tool] =
    Decoder.decodeString.emap(s => Tool.withNameOption(s).toRight(s"Unsupported Tool ${s}"))

  implicit val cloudContextDecoder: Decoder[CloudContext] = Decoder.instance { x =>
    for {
      cloudProvider <- x.downField("cloudProvider").as[CloudProvider]
      context <- cloudProvider match {
        case CloudProvider.Gcp =>
          x.downField("cloudResource").as[GoogleProject].map(p => CloudContext.Gcp(p))
        case CloudProvider.Azure =>
          x.downField("cloudResource").as[AzureCloudContext].map(p => CloudContext.Azure(p))
      }
    } yield context
  }

  implicit val runtimeImageDecoder: Decoder[RuntimeImage] = Decoder.forProduct4(
    "imageType",
    "imageUrl",
    "homeDirectory",
    "timestamp"
  )(RuntimeImage.apply)
  implicit val auditInfoDecoder: Decoder[AuditInfo] = Decoder.forProduct4(
    "creator",
    "createdDate",
    "destroyedDate",
    "dateAccessed"
  )(AuditInfo.apply)
  implicit val rtDataprocConfigDecoder: Decoder[RuntimeConfig.DataprocConfig] = Decoder.instance { c =>
    for {
      numberOfWorkers <- c.downField("numberOfWorkers").as[Int]
      masterMachineType <- c.downField("masterMachineType").as[MachineTypeName]
      masterDiskSize <- c.downField("masterDiskSize").as[DiskSize]
      workerMachineType <- c.downField("workerMachineType").as[Option[MachineTypeName]]
      workerDiskSize <- c.downField("workerDiskSize").as[Option[DiskSize]]
      numberOfWorkerLocalSSDs <- c.downField("numberOfWorkerLocalSSDs").as[Option[Int]]
      numberOfPreemptibleWorkers <- c.downField("numberOfPreemptibleWorkers").as[Option[Int]]
      propertiesOpt <- c.downField("properties").as[Option[LabelMap]]
      properties = propertiesOpt.getOrElse(Map.empty)
      region <- c.downField("region").as[RegionName]
      componentGatewayEnabled <- c.downField("componentGatewayEnabled").as[Boolean]
      workerPrivateAccess <- c.downField("workerPrivateAccess").as[Boolean]
    } yield RuntimeConfig.DataprocConfig(
      numberOfWorkers,
      masterMachineType,
      masterDiskSize,
      workerMachineType,
      workerDiskSize,
      numberOfWorkerLocalSSDs,
      numberOfPreemptibleWorkers,
      properties,
      region,
      componentGatewayEnabled,
      workerPrivateAccess
    )
  }

  implicit val runtimeConfigDecoder: Decoder[RuntimeConfig] = Decoder.instance { x =>
    for {
      cloudService <- x.downField("cloudService").as[CloudService]
      r <- cloudService match {
        case CloudService.Dataproc =>
          x.as[RuntimeConfig.DataprocConfig]
        case CloudService.GCE =>
          x.as[RuntimeConfig.GceConfig] orElse x.as[RuntimeConfig.GceWithPdConfig]
        case CloudService.AzureVm =>
          x.as[RuntimeConfig.AzureConfig]
      }
    } yield r
  }

  implicit val runtimeErrorDecoder: Decoder[RuntimeError] = Decoder.forProduct4(
    "errorMessage",
    "errorCode",
    "timestamp",
    "traceId"
  )(RuntimeError.apply)
  implicit val gcsPathDecoder: Decoder[GcsPath] = Decoder.decodeString.emap(s => parseGcsPath(s).leftMap(_.value))
  implicit val userScriptPathDecoder: Decoder[UserScriptPath] = Decoder.decodeString.emap { s =>
    UserScriptPath.stringToUserScriptPath(s).leftMap(_.getMessage)
  }
  implicit val userJupyterExtensionConfigDecoder: Decoder[UserJupyterExtensionConfig] = Decoder.instance { c =>
    for {
      ne <- c.downField("nbExtensions").as[Option[Map[String, String]]]
      se <- c.downField("serverExtensions").as[Option[Map[String, String]]]
      ce <- c.downField("combinedExtensions").as[Option[Map[String, String]]]
      le <- c.downField("labExtensions").as[Option[Map[String, String]]]
    } yield UserJupyterExtensionConfig(ne.getOrElse(Map.empty),
                                       se.getOrElse(Map.empty),
                                       ce.getOrElse(Map.empty),
                                       le.getOrElse(Map.empty)
    )
  }

  // Used only in pubsub messages
  implicit val clusterProjectAndNameDecoder: Decoder[RuntimeProjectAndName] =
    Decoder.instance(x =>
      // support old messages where `googleProject` and `clusterName` are still being used. Remove once this code is released
      for {
        cloudProviderOpt <- x.downField("cloudProvider").as[Option[CloudProvider]]
        cloudContext <- cloudProviderOpt match {
          case Some(value) =>
            value match {
              case CloudProvider.Gcp =>
                x.downField("cloudContext").as[GoogleProject].map(p => CloudContext.Gcp(p))
              case CloudProvider.Azure =>
                x.downField("cloudContext").as[AzureCloudContext].map(mrg => CloudContext.Azure(mrg))
            }
          case None =>
            x.downField("googleProject")
              .as[GoogleProject]
              .map(p =>
                CloudContext.Gcp(p)
              ) // TODO: remove this case since this is just for backwards compatibility once this change is released to prod
        }
        runtimeName <- x.downField("clusterName").as[RuntimeName].orElse(x.downField("runtimeName").as[RuntimeName])
      } yield RuntimeProjectAndName(cloudContext, runtimeName)
    )

  implicit val asyncRuntimeFieldsDecoder: Decoder[AsyncRuntimeFields] =
    Decoder.forProduct4("googleId", "operationName", "stagingBucket", "hostIp")(AsyncRuntimeFields.apply)

  implicit val zoneDecoder: Decoder[ZoneName] = Decoder.decodeString.map(ZoneName(_))
  implicit val regionDecoder: Decoder[RegionName] = Decoder.decodeString.map(RegionName(_))
  implicit val diskNameDecoder: Decoder[DiskName] = Decoder.decodeString.emap(s => validateName(s).map(DiskName))
  implicit val diskIdDecoder: Decoder[DiskId] = Decoder.decodeLong.map(DiskId)
  implicit val diskLinkDecoder: Decoder[DiskLink] = Decoder.decodeString.map(DiskLink)
  implicit val formattedByDecoder: Decoder[FormattedBy] =
    Decoder.decodeString.emap(fb => FormattedBy.values.find(_.asString == fb).toRight(s"Invalid formatted by: $fb"))
  implicit val diskStatusDecoder: Decoder[DiskStatus] =
    Decoder.decodeString.emap(x => DiskStatus.withNameOption(x).toRight(s"Invalid disk status: $x"))
  implicit val diskTypeDecoder: Decoder[DiskType] =
    Decoder.decodeString.emap(x => DiskType.stringToObject.get(x).toRight(s"Invalid disk type: $x"))
  implicit val gpuTypeDecoder: Decoder[GpuType] =
    Decoder.decodeString.emap(s => GpuType.stringToObject.get(s).toRight(s"unsupported gpuType ${s}"))
  implicit val azureRegionDecoder: Decoder[Region] =
    Decoder.decodeString.emap { s =>
      val regionOpt: Option[Region] =
        if (Region.values.stream.map((x: Region) => x.toString).collect(Collectors.toList[String]).contains(s))
          Some(Region.fromName(s))
        else none[Region]
      regionOpt.toRight(s"Invalid azure region ${s}")
    }
  implicit val applicationInsightsNameDecoder: Decoder[ApplicationInsightsName] =
    Decoder.decodeString.map(ApplicationInsightsName)

  implicit val gpuConfigDecoder: Decoder[GpuConfig] = Decoder.forProduct2(
    "gpuType",
    "numOfGpus"
  )(GpuConfig.apply)

  implicit val appMachineTypeDecoder: Decoder[AppMachineType] = Decoder.forProduct2(
    "memorySizeInGb",
    "numOfCpus"
  )(AppMachineType.apply)

  implicit val gceWithPdConfigDecoder: Decoder[RuntimeConfig.GceWithPdConfig] = Decoder.forProduct5(
    "machineType",
    "persistentDiskId",
    "bootDiskSize",
    "zone",
    "gpuConfig"
  )(RuntimeConfig.GceWithPdConfig.apply)
  implicit val gceConfigDecoder: Decoder[RuntimeConfig.GceConfig] = Decoder.forProduct5(
    "machineType",
    "diskSize",
    "bootDiskSize",
    "zone",
    "gpuConfig"
  )((mt, ds, bds, z, gpu) => RuntimeConfig.GceConfig(mt, ds, bds, z, gpu))

  implicit val azureVmConfigDecoder: Decoder[RuntimeConfig.AzureConfig] = Decoder.forProduct3(
    "machineType",
    "persistentDiskId",
    "region"
  )(RuntimeConfig.AzureConfig.apply)

  implicit val persistentDiskRequestDecoder: Decoder[PersistentDiskRequest] = Decoder.instance { x =>
    for {
      n <- x.downField("name").as[DiskName]
      s <- x.downField("size").as[Option[DiskSize]]
      t <- x.downField("diskType").as[Option[DiskType]]
      l <- x.downField("labels").as[Option[LabelMap]]
    } yield PersistentDiskRequest(n, s, t, l.getOrElse(Map.empty))
  }

  implicit val runtimeStatusEncoder: Encoder[RuntimeStatus] = Encoder.encodeString.contramap { x =>
    x match {
      case RuntimeStatus.PreCreating => RuntimeStatus.Creating.toString
      case RuntimeStatus.PreStarting => RuntimeStatus.Starting.toString
      case RuntimeStatus.PreStopping => RuntimeStatus.Stopping.toString
      case RuntimeStatus.PreDeleting => RuntimeStatus.Deleting.toString
      case _                         => x.toString
    }
  }

  implicit val diskConfigEncoder: Encoder[DiskConfig] = Encoder.forProduct4(
    "name",
    "size",
    "diskType",
    "blockSize"
  )(x => (x.name, x.size, x.diskType, x.blockSize))

  // can't use Encoder.forProductX because there are 23 fields
  implicit val getRuntimeResponseEncoder: Encoder[GetRuntimeResponse] = Encoder.instance { x =>
    Json.obj(
      ("id", x.id.asJson),
      ("runtimeName", x.clusterName.asJson),
      ("googleProject", x.cloudContext.asString.asJson),
      ("cloudContext", x.cloudContext.asJson),
      ("serviceAccount", x.serviceAccountInfo.asJson),
      ("asyncRuntimeFields", x.asyncRuntimeFields.asJson),
      ("auditInfo", x.auditInfo.asJson),
      ("runtimeConfig", x.runtimeConfig.asJson),
      ("proxyUrl", x.clusterUrl.asJson),
      ("status", x.status.asJson),
      ("labels", x.labels.asJson),
      ("userScriptUri", x.userScriptUri.asJson),
      ("startUserScriptUri", x.startUserScriptUri.asJson),
      ("jupyterUserScriptUri", x.userScriptUri.asJson),
      ("jupyterStartUserScriptUri", x.startUserScriptUri.asJson),
      ("errors", x.errors.asJson),
      ("userJupyterExtensionConfig", x.userJupyterExtensionConfig.asJson),
      ("autopauseThreshold", x.autopauseThreshold.asJson),
      ("defaultClientId", x.defaultClientId.asJson),
      ("runtimeImages", x.clusterImages.asJson),
      ("scopes", x.scopes.asJson),
      ("customEnvironmentVariables", x.customClusterEnvironmentVariables.asJson),
      ("diskConfig", x.diskConfig.asJson),
      ("patchInProgress", x.patchInProgress.asJson)
    )
  }

  implicit val createRuntimeResponseEncoder: Encoder[CreateRuntimeResponse] =
    Encoder.forProduct1("traceId")(x => CreateRuntimeResponse.unapply(x).get)

  implicit val persistentDiskRequestEncoder: Encoder[PersistentDiskRequest] = Encoder.forProduct4(
    "name",
    "size",
    "diskType",
    "labels"
  )(x => (x.name, x.size, x.diskType, x.labels))

  implicit val runtimeSamResourceDecoder: Decoder[RuntimeSamResourceId] =
    Decoder.decodeString.map(RuntimeSamResourceId)
  implicit val persistentDiskSamResourceDecoder: Decoder[PersistentDiskSamResourceId] =
    Decoder.decodeString.map(PersistentDiskSamResourceId)
  implicit val projectSamResourceDecoder: Decoder[ProjectSamResourceId] =
    Decoder.decodeString.map(x => ProjectSamResourceId(GoogleProject(x)))
  implicit val wsmResourceSamResourceIdDecoder: Decoder[WsmResourceSamResourceId] =
    Decoder.decodeString.emap(x =>
      Either
        .catchNonFatal(WsmResourceSamResourceId(WsmControlledResourceId(UUID.fromString(x))))
        .leftMap(_.getMessage)
    )

  implicit val workspaceIdDecoder: Decoder[WorkspaceId] =
    Decoder.decodeString.emap { x =>
      Either
        .catchNonFatal(WorkspaceId(UUID.fromString(x)))
        .leftMap(_.getMessage)
    }

  implicit val billingProfileIdDecoder: Decoder[BillingProfileId] =
    Decoder.decodeString.map(BillingProfileId)

  implicit val workspaceSamResourceIdDecoder: Decoder[WorkspaceResourceSamResourceId] =
    workspaceIdDecoder.map(WorkspaceResourceSamResourceId.apply)

  implicit val errorSourceDecoder: Decoder[ErrorSource] =
    Decoder.decodeString.emap(s => ErrorSource.stringToObject.get(s).toRight(s"Invalid error source ${s}"))
  implicit val errorActionDecoder: Decoder[ErrorAction] =
    Decoder.decodeString.emap(s => ErrorAction.stringToObject.get(s).toRight(s"Invalid error action ${s}"))
  implicit val appErrorDecoder: Decoder[AppError] =
    Decoder.forProduct6("errorMessage", "timestamp", "action", "source", "googleErrorCode", "traceId")(AppError.apply)

  implicit val nodepoolIdDecoder: Decoder[NodepoolLeoId] = Decoder.decodeLong.map(NodepoolLeoId)
  implicit val nodepoolNameDecoder: Decoder[NodepoolName] =
    Decoder.decodeString.emap(s => KubernetesName.withValidation(s, NodepoolName).leftMap(_.getMessage))
  implicit val nodepoolStatusDecoder: Decoder[NodepoolStatus] =
    Decoder.decodeString.emap(s => NodepoolStatus.stringToObject.get(s).toRight(s"Invalid nodepool status ${s}"))
  implicit val numNodesDecoder: Decoder[NumNodes] =
    Decoder.decodeInt.emap(n => if (n < 1) Left("Minimum number of nodes is 1") else Right(NumNodes.apply(n)))
  implicit val autoscalingMinDecoder: Decoder[AutoscalingMin] = Decoder.decodeInt.map(AutoscalingMin)
  implicit val autoscalingMaxDecoder: Decoder[AutoscalingMax] = Decoder.decodeInt.map(AutoscalingMax)
  implicit val autoscalingConfigDecoder: Decoder[AutoscalingConfig] =
    Decoder.forProduct2("autoscalingMin", "autoscalingMax")(AutoscalingConfig.apply)
  implicit val kubernetesRuntimeConfigDecoder: Decoder[KubernetesRuntimeConfig] =
    Decoder.forProduct3("numNodes", "machineType", "autoscalingEnabled")(KubernetesRuntimeConfig.apply)
  implicit val computeClassDecoder: Decoder[ComputeClass] = Decoder.decodeString.emap(s =>
    ComputeClass.stringToObject.get(s.toLowerCase).toRight(s"Invalid compute class ${s}")
  )
  implicit val autopilotDecoder: Decoder[Autopilot] =
    Decoder.forProduct2("computeClass", "ephemeralStorageInGb")(Autopilot.apply)

  implicit val locationDecoder: Decoder[Location] = Decoder.decodeString.map(Location)
  implicit val kubeClusterIdDecoder: Decoder[KubernetesClusterLeoId] = Decoder.decodeLong.map(KubernetesClusterLeoId)
  implicit val kubeClusterNameDecoder: Decoder[KubernetesClusterName] =
    Decoder.decodeString.emap(s => KubernetesName.withValidation(s, KubernetesClusterName).leftMap(_.getMessage))
  implicit val kubeClusterStatusDecoder: Decoder[KubernetesClusterStatus] = Decoder.decodeString.emap(s =>
    KubernetesClusterStatus.stringToObject.get(s).toRight(s"Invalid cluster status ${s}")
  )
  implicit val appAccessScopeDecoder: Decoder[AppAccessScope] =
    Decoder.decodeString.emap(s => AppAccessScope.stringToObject.get(s).toRight(s"Invalid app accessScope ${s}"))

  // Note these are not implicit because they would be ambiguous types. We switch between them
  // at runtime depending on app access scope.
  val appSamIdDecoder: Decoder[AppSamResourceId] =
    Decoder.decodeString.map(s => AppSamResourceId(s, Some(AppAccessScope.UserPrivate)))
  val sharedAppSamIdDecoder: Decoder[AppSamResourceId] =
    Decoder.decodeString.map(s => AppSamResourceId(s, Some(AppAccessScope.WorkspaceShared)))

  implicit val namespaceNameDecoder: Decoder[NamespaceName] =
    Decoder.decodeString.emap(s => KubernetesName.withValidation(s, NamespaceName).leftMap(_.getMessage))
  implicit val appNameDecoder: Decoder[AppName] = Decoder.decodeString.map(AppName)
  implicit val appStatusDecoder: Decoder[AppStatus] =
    Decoder.decodeString.emap(s => AppStatus.stringToObject.get(s).toRight(s"Invalid app status ${s}"))
  implicit val appTypeDecoder: Decoder[AppType] =
    Decoder.decodeString.emap(s => AppType.stringToObject.get(s).toRight(s"Invalid app type ${s}"))
  implicit val relayNamespaceDecoder: Decoder[RelayNamespace] = Decoder.decodeString.map(RelayNamespace)
  implicit val chartNameDecoder: Decoder[ChartName] = Decoder.decodeString.map(ChartName)
  implicit val allowedChartNameDecoder: Decoder[AllowedChartName] =
    Decoder.decodeString.emap(x => AllowedChartName.stringToObject.get(x).toRight("chart name not allowed"))
  implicit val aksClusterNameDecoder: Decoder[AKSClusterName] = Decoder.decodeString.map(AKSClusterName)
  implicit val aksClusterDecoder: Decoder[AKSCluster] = Decoder.forProduct2("name", "tags")(AKSCluster)
  implicit val postgresServerDecoder: Decoder[PostgresServer] =
    Decoder.forProduct2("name", "pgBouncerEnabled")(PostgresServer)

  implicit val apiServerIpDecoder: Decoder[KubernetesApiServerIp] = Decoder.decodeString.map(KubernetesApiServerIp)
  implicit val networkNameDecoder: Decoder[NetworkName] = Decoder.decodeString.map(NetworkName)
  implicit val subNetworkNameDecoder: Decoder[SubnetworkName] = Decoder.decodeString.map(SubnetworkName)
  implicit val ipRangeDecoder: Decoder[IpRange] = Decoder.decodeString.map(IpRange)
  implicit val networkFieldsDecoder: Decoder[NetworkFields] =
    Decoder.forProduct3("networkName", "subNetworkName", "subNetworkIpRange")(NetworkFields)
  implicit val kubeAsyncFieldDecoder: Decoder[KubernetesClusterAsyncFields] =
    Decoder.forProduct3("loadBalancerIp", "apiServerIp", "networkInfo")(KubernetesClusterAsyncFields)

  implicit val dataprocInstanceKeyDecoder: Decoder[DataprocInstanceKey] = Decoder.forProduct3(
    "project",
    "zone",
    "name"
  )(DataprocInstanceKey.apply)

  implicit val dataprocInstanceKeyEncoder: Encoder[DataprocInstanceKey] = Encoder.forProduct3(
    "project",
    "zone",
    "name"
  )(x => DataprocInstanceKey.unapply(x).get)

  implicit val dataprocInstanceDecoder: Decoder[DataprocInstance] =
    Decoder.forProduct6(
      "key",
      "googleId",
      "status",
      "ip",
      "dataprocRole",
      "createdDate"
    )(DataprocInstance.apply)

  implicit val dataprocInstanceEncoder: Encoder[DataprocInstance] =
    Encoder.forProduct6(
      "key",
      "googleId",
      "status",
      "ip",
      "dataprocRole",
      "createdDate"
    )(x => DataprocInstance.unapply(x).get)

  implicit val uriDecoder: Decoder[Uri] =
    Decoder.decodeString.emap(s => Uri.fromString(s).leftMap(_.getMessage()))

  implicit val uuidDecoder: Decoder[UUID] = Decoder.decodeString.map(s => UUID.fromString(s))

  implicit val wsmJobIdDecoder: Decoder[WsmJobId] =
    Decoder.decodeString.map(s => WsmJobId(s))

  implicit val workspaceIdEncoder: Encoder[WorkspaceId] =
    Encoder.encodeString.contramap(_.value.toString)

  implicit val billingProfileIdEncoder: Encoder[BillingProfileId] =
    Encoder.encodeString.contramap(_.value)

  implicit val wsmControlledResourceIdEncoder: Encoder[WsmControlledResourceId] =
    Encoder.encodeString.contramap(_.value.toString)

  implicit val wsmControlledResourceIdDecoder: Decoder[WsmControlledResourceId] =
    Decoder.decodeString.emap(x =>
      Either
        .catchNonFatal(WsmControlledResourceId(UUID.fromString(x)))
        .leftMap(_.getMessage)
    )

  implicit val azureMachineTypeEncoder: Encoder[VirtualMachineSizeTypes] = Encoder.encodeString.contramap(_.toString)
  implicit val azureDiskNameEncoder: Encoder[AzureDiskName] = Encoder.encodeString.contramap(_.value)
  implicit val relayNamespaceEncoder: Encoder[RelayNamespace] = Encoder.encodeString.contramap(_.value)
  implicit val aksClusterNameEncoder: Encoder[AKSClusterName] = Encoder.encodeString.contramap(_.value)
  implicit val aksClusterEncoder: Encoder[AKSCluster] =
    Encoder.forProduct2("name", "tags")(x => (x.name, x.tags))
  implicit val postgresServerEncoder: Encoder[PostgresServer] =
    Encoder.forProduct2("name", "pgBouncerEnabled")(x => (x.name, x.pgBouncerEnabled))

  implicit val azureImageEncoder: Encoder[AzureImage] = Encoder.forProduct4(
    "publisher",
    "offer",
    "sku",
    "version"
  )(x => (x.publisher, x.offer, x.sku, x.version))

  implicit val landingZoneResourcesDecoder: Decoder[LandingZoneResources] =
    Decoder.forProduct11(
      "landingZoneId",
      "clusterName",
      "batchAccountName",
      "relayNamespace",
      "storageAccountName",
      "vnetName",
      "batchNodesSubnetName",
      "aksSubnetName",
      "region",
      "applicationInsightsName",
      "postgresName"
    )(
      LandingZoneResources.apply
    )

  implicit val landingZoneResourcesEncoder: Encoder[LandingZoneResources] = Encoder.forProduct11(
    "landingZoneId",
    "clusterName",
    "batchAccountName",
    "relayNamespace",
    "storageAccountName",
    "vnetName",
    "batchNodesSubnetName",
    "aksSubnetName",
    "region",
    "applicationInsightsName",
    "postgresName"
  )(x =>
    (x.landingZoneId,
     x.aksCluster,
     x.batchAccountName,
     x.relayNamespace,
     x.storageAccountName,
     x.vnetName,
     x.batchNodesSubnetName,
     x.aksSubnetName,
     x.region,
     x.applicationInsightsName,
     x.postgresServer
    )
  )

  implicit val autodeleteThresholdEncoder: Encoder[AutodeleteThreshold] = Encoder.encodeInt.contramap(_.value)

  implicit val autodeleteThresholdDecoder: Decoder[AutodeleteThreshold] = Decoder.decodeInt.emap {
    case n if n <= 0 => Left("autodeleteThreshold must be a positive number of minutes")
    case n           => Right(AutodeleteThreshold.apply(n))
  }

  implicit val updateAppJobIdDecoder: Decoder[UpdateAppJobId] = Decoder.decodeString.emap(x =>
    Either
      .catchNonFatal(UpdateAppJobId(UUID.fromString(x)))
      .leftMap(_.getMessage)
  )
  implicit val updateAppJobIdEncoder: Encoder[UpdateAppJobId] = Encoder.encodeString.contramap(_.value.toString)
}
