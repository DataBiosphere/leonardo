package org.broadinstitute.dsde.workbench.leonardo

import java.net.URL
import java.time.Instant

import cats.implicits._
import io.circe.syntax._
import io.circe.{Decoder, DecodingFailure, Encoder}
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{
  parseGcsPath,
  GcsBucketName,
  GcsObjectName,
  GcsPath,
  GoogleProject
}

object JsonCodec {
  // Errors
  val negativeNumberDecodingFailure = DecodingFailure("Negative number is not allowed", List.empty)
  val minimumDiskSizeDecodingFailure = DecodingFailure("Minimum required disk size is 50GB", List.empty)
  val oneWorkerSpecifiedDecodingFailure = DecodingFailure(
    "Google Dataproc does not support clusters with 1 non-preemptible worker. Must be 0, 2 or more.",
    List.empty
  )

  // Encoders
  implicit val operationNameEncoder: Encoder[OperationName] = Encoder.encodeString.contramap(_.value)
  implicit val googleIdEncoder: Encoder[GoogleId] = Encoder.encodeString.contramap(_.value)
  implicit val ipEncoder: Encoder[IP] = Encoder.encodeString.contramap(_.value)
  implicit val gcsBucketNameEncoder: Encoder[GcsBucketName] = Encoder.encodeString.contramap(_.value)
  implicit val workbenchEmailEncoder: Encoder[WorkbenchEmail] = Encoder.encodeString.contramap(_.value)
  implicit val gcsObjectNameEncoder: Encoder[GcsObjectName] = Encoder.encodeString.contramap(_.value)
  implicit val googleProjectEncoder: Encoder[GoogleProject] = Encoder.encodeString.contramap(_.value)
  implicit val gcsPathEncoder: Encoder[GcsPath] = Encoder.encodeString.contramap(_.toUri)
  implicit val userScriptPathEncoder: Encoder[UserScriptPath] = Encoder.encodeString.contramap(_.asString)
  implicit val machineTypeEncoder: Encoder[MachineTypeName] = Encoder.encodeString.contramap(_.value)
  implicit val cloudServiceEncoder: Encoder[CloudService] = Encoder.encodeString.contramap(_.asString)
  implicit val runtimeNameEncoder: Encoder[RuntimeName] = Encoder.encodeString.contramap(_.asString)
  implicit val urlEncoder: Encoder[URL] = Encoder.encodeString.contramap(_.toString)
  implicit val diskSizeEncoder: Encoder[DiskSize] = Encoder.encodeInt.contramap(_.gb)
  implicit val blockSizeEncoder: Encoder[BlockSize] = Encoder.encodeInt.contramap(_.bytes)
  implicit val dataprocConfigEncoder: Encoder[RuntimeConfig.DataprocConfig] = Encoder.forProduct8(
    "numberOfWorkers",
    "masterMachineType",
    "masterDiskSize",
    // worker settings are None when numberOfWorkers is 0
    "workerMachineType",
    "workerDiskSize",
    "numberOfWorkerLocalSSDs",
    "numberOfPreemptibleWorkers",
    "cloudService"
  )(x =>
    (x.numberOfWorkers,
     x.machineType,
     x.diskSize,
     x.workerMachineType,
     x.workerDiskSize,
     x.numberOfWorkerLocalSSDs,
     x.numberOfPreemptibleWorkers,
     x.cloudService)
  )
  implicit val gceRuntimeConfigEncoder: Encoder[RuntimeConfig.GceConfig] = Encoder.forProduct3(
    "machineType",
    "diskSize",
    "cloudService"
  )(x => (x.machineType, x.diskSize, x.cloudService))
  implicit val userJupyterExtensionConfigEncoder: Encoder[UserJupyterExtensionConfig] = Encoder.forProduct4(
    "nbExtensions",
    "serverExtensions",
    "combinedExtensions",
    "labExtensions"
  )(x => UserJupyterExtensionConfig.unapply(x).get)
  implicit val auditInfoEncoder: Encoder[AuditInfo] = Encoder.forProduct5(
    "creator",
    "createdDate",
    "destroyedDate",
    "dateAccessed",
    "kernelFoundBusyDate"
  )(x => AuditInfo.unapply(x).get)
  implicit val runtimeImageTypeEncoder: Encoder[RuntimeImageType] = Encoder.encodeString.contramap(_.toString)
  implicit val containerImageEncoder: Encoder[ContainerImage] = Encoder.encodeString.contramap(_.imageUrl)
  implicit val runtimeImageEncoder: Encoder[RuntimeImage] = Encoder.forProduct3(
    "imageType",
    "imageUrl",
    "timestamp"
  )(x => RuntimeImage.unapply(x).get)
  implicit val runtimeConfigEncoder: Encoder[RuntimeConfig] = Encoder.instance(x =>
    x match {
      case x: RuntimeConfig.DataprocConfig => x.asJson
      case x: RuntimeConfig.GceConfig      => x.asJson
    }
  )
  implicit val runtimeStatusEncoder: Encoder[RuntimeStatus] = Encoder.encodeString.contramap(_.toString)
  implicit val defaultLabelsEncoder: Encoder[DefaultLabels] = Encoder.forProduct7(
    "clusterName",
    "googleProject",
    "creator",
    "clusterServiceAccount",
    "notebookUserScript",
    "notebookStartUserScript",
    "tool"
  )(x => DefaultLabels.unapply(x).get)
  implicit val asyncRuntimeFieldsEncoder: Encoder[AsyncRuntimeFields] =
    Encoder.forProduct4("googleId", "operationName", "stagingBucket", "hostIp")(x => AsyncRuntimeFields.unapply(x).get)
  implicit val clusterProjectAndNameEncoder: Encoder[RuntimeProjectAndName] = Encoder.forProduct2(
    "googleProject",
    "clusterName"
  )(x => RuntimeProjectAndName.unapply(x).get)
  implicit val runtimeErrorEncoder: Encoder[RuntimeError] = Encoder.forProduct3(
    "errorMessage",
    "errorCode",
    "timestamp"
  )(x => RuntimeError.unapply(x).get)
  implicit val diskStatusEncoder: Encoder[DiskStatus] = Encoder.encodeString.contramap(_.toString)
  implicit val diskAuditInfoEncoder: Encoder[DiskAuditInfo] = Encoder.forProduct4(
    "creator",
    "createdDate",
    "destroyedDate",
    "dateAccessed"
  )(x => DiskAuditInfo.unapply(x).get)
  implicit val diskTypeEncoder: Encoder[DiskType] = Encoder.encodeString.contramap(_.entryName)

  // Decoders
  implicit val operationNameDecoder: Decoder[OperationName] = Decoder.decodeString.map(OperationName)
  implicit val googleIdDecoder: Decoder[GoogleId] = Decoder.decodeString.map(GoogleId)
  implicit val ipDecoder: Decoder[IP] = Decoder.decodeString.map(IP)
  implicit val containerImageDecoder: Decoder[ContainerImage] =
    Decoder.decodeString.emap(s => ContainerImage.fromString(s).toRight(s"invalid container image ${s}"))
  implicit val cloudServiceDecoder: Decoder[CloudService] =
    Decoder.decodeString.emap(s => CloudService.withNameInsensitiveOption(s).toRight(s"Unsupported cloud service ${s}"))
  implicit val runtimeNameDecoder: Decoder[RuntimeName] = Decoder.decodeString.map(RuntimeName)
  implicit val runtimeStatusDecoder: Decoder[RuntimeStatus] = Decoder.decodeString.map(s => RuntimeStatus.withName(s))
  implicit val runtimeInternalIdDecoder: Decoder[RuntimeInternalId] = Decoder.decodeString.map(RuntimeInternalId)
  implicit val persistentDiskInternalIdDecoder: Decoder[PersistentDiskInternalId] =
    Decoder.decodeString.map(PersistentDiskInternalId)
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
    Decoder.decodeInt.emap(d => if (d < 50) Left("Minimum required disk size is 50GB") else Right(DiskSize(d)))
  implicit val blockSizeDecoder: Decoder[BlockSize] =
    Decoder.decodeInt.emap(d => if (d < 0) Left("Negative number is not allowed") else Right(BlockSize(d)))
  implicit val workbenchEmailDecoder: Decoder[WorkbenchEmail] = Decoder.decodeString.map(WorkbenchEmail)
  implicit val runtimeImageTypeDecoder: Decoder[RuntimeImageType] = Decoder.decodeString.emap(s =>
    RuntimeImageType.stringToRuntimeImageType.get(s).toRight(s"invalid RuntimeImageType ${s}")
  )
  implicit val runtimeImageDecoder: Decoder[RuntimeImage] = Decoder.forProduct3(
    "imageType",
    "imageUrl",
    "timestamp"
  )(RuntimeImage.apply)
  implicit val auditInfoDecoder: Decoder[AuditInfo] = Decoder.forProduct5(
    "creator",
    "createdDate",
    "destroyedDate",
    "dateAccessed",
    "kernelFoundBusyDate"
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
    } yield RuntimeConfig.DataprocConfig(numberOfWorkers,
                                         masterMachineType,
                                         masterDiskSize,
                                         workerMachineType,
                                         workerDiskSize,
                                         numberOfWorkerLocalSSDs,
                                         numberOfPreemptibleWorkers,
                                         properties)
  }

  implicit val gceConfigDecoder: Decoder[RuntimeConfig.GceConfig] = Decoder.forProduct2(
    "machineType",
    "diskSize"
  )((mt, ds) => RuntimeConfig.GceConfig(mt, ds))
  implicit val runtimeConfigDecoder: Decoder[RuntimeConfig] = Decoder.instance { x =>
    for {
      cloudService <- x.downField("cloudService").as[CloudService]
      r <- cloudService match {
        case CloudService.Dataproc =>
          x.as[RuntimeConfig.DataprocConfig]
        case CloudService.GCE =>
          x.as[RuntimeConfig.GceConfig]
      }
    } yield r
  }

  implicit val runtimeErrorDecoder: Decoder[RuntimeError] = Decoder.forProduct3(
    "errorMessage",
    "errorCode",
    "timestamp"
  )(RuntimeError.apply)
  implicit val gcsPathDecoder: Decoder[GcsPath] = Decoder.decodeString.emap(s => parseGcsPath(s).leftMap(_.value))
  implicit val userScriptPathDecoder: Decoder[UserScriptPath] = Decoder.decodeString.emap { s =>
    UserScriptPath.stringToUserScriptPath(s).leftMap(_.getMessage)
  }
  implicit val userJupyterExtensionConfigDecoder: Decoder[UserJupyterExtensionConfig] = Decoder.forProduct4(
    "nbExtensions",
    "serverExtensions",
    "combinedExtensions",
    "labExtensions"
  )(UserJupyterExtensionConfig.apply)
  implicit val clusterProjectAndNameDecoder: Decoder[RuntimeProjectAndName] =
    Decoder.forProduct2("googleProject", "clusterName")(RuntimeProjectAndName.apply)

  implicit val asyncRuntimeFieldsDecoder: Decoder[AsyncRuntimeFields] =
    Decoder.forProduct4("googleId", "operationName", "stagingBucket", "hostIp")(AsyncRuntimeFields.apply)
  implicit val diskStatusDecoder: Decoder[DiskStatus] =
    Decoder.decodeString.emap(x => DiskStatus.withNameOption(x).toRight(s"Invalid disk status: $x"))
  implicit val diskAuditInfoDecoder: Decoder[DiskAuditInfo] =
    Decoder.forProduct4("creator", "createdDate", "destroyedDate", "dateAccesed")(DiskAuditInfo.apply)
  implicit val diskTypeDecoder: Decoder[DiskType] =
    Decoder.decodeString.emap(x => DiskType.withNameOption(x).toRight(s"Invalid disk type: $x"))
}
